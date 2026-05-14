module Config = struct
  type t = {
    core : Nats.Config.t;
    credentials : Nats.Client.Connect.t;
    command_capacity : int;
    subscription_capacity : int;
    event_capacity : int;
    read_capacity : int;
    read_chunk_size : int;
    flush_timeout : Mtime.Span.t;
    drain_timeout : Mtime.Span.t;
  }

  let default_span = Mtime.Span.(5 * s)

  let validate_capacity name value =
    if value > 0 then Ok value
    else Error (Error.Invalid_capacity { name; value })

  let validate_timeout name value =
    if Mtime.Span.compare value Mtime.Span.zero > 0 then Ok value
    else Error (Error.Invalid_timeout name)

  let v ?(core = Nats.Config.default) ?(credentials = Nats.Client.Connect.v ())
      ?(command_capacity = 128) ?(subscription_capacity = 256)
      ?(event_capacity = 64) ?(read_capacity = 4) ?(read_chunk_size = 65536)
      ?(flush_timeout = default_span) ?(drain_timeout = default_span) () =
    match validate_capacity "command" command_capacity with
    | Error error -> Error error
    | Ok command_capacity -> (
        match validate_capacity "subscription" subscription_capacity with
        | Error error -> Error error
        | Ok subscription_capacity -> (
            match validate_capacity "event" event_capacity with
            | Error error -> Error error
            | Ok event_capacity -> (
                match validate_capacity "read" read_capacity with
                | Error error -> Error error
                | Ok read_capacity -> (
                    if read_chunk_size <= 0 then
                      Error (Error.Invalid_chunk_size read_chunk_size)
                    else
                      match validate_timeout "flush" flush_timeout with
                      | Error error -> Error error
                      | Ok flush_timeout -> (
                          match validate_timeout "drain" drain_timeout with
                          | Error error -> Error error
                          | Ok drain_timeout ->
                              Ok
                                {
                                  core;
                                  credentials;
                                  command_capacity;
                                  subscription_capacity;
                                  event_capacity;
                                  read_capacity;
                                  read_chunk_size;
                                  flush_timeout;
                                  drain_timeout;
                                })))))

  let default =
    match v () with
    | Ok value -> value
    | Error error ->
        invalid_arg
          (Format.asprintf "invalid default Eio config: %a" Error.pp error)
end

module Event_stream = struct
  type item = Event of Event.t | Done of Error.t

  type t = {
    queue : item Eio.Stream.t;
    capacity : int;
    mutable terminal : Error.t option;
    mutable done_seen : bool;
  }

  let create capacity =
    {
      queue =
        Eio.Stream.create
          (if capacity >= max_int - 1 then max_int else capacity + 2);
      capacity;
      terminal = None;
      done_seen = false;
    }

  let push t event =
    if Option.is_some t.terminal then false
    else if Eio.Stream.length t.queue >= t.capacity then false
    else (
      Eio.Stream.add t.queue (Event event);
      true)

  let next t =
    if t.done_seen then
      match t.terminal with Some error -> Error error | None -> assert false
    else
      match Eio.Stream.take t.queue with
      | Event event -> Ok event
      | Done error ->
          t.done_seen <- true;
          Error error

  let terminate t error =
    if Option.is_none t.terminal then (
      t.terminal <- Some error;
      Eio.Stream.add t.queue (Done error))

  let push_terminal t event =
    if Option.is_none t.terminal then Eio.Stream.add t.queue (Event event)
end

module Subscription = struct
  type item = Message of delivery | Done of Error.t
  and delivery = { message : Nats.Message.t; status : Nats.Op.status option }

  type t = {
    sid : int;
    queue : item Eio.Stream.t;
    capacity : int;
    mutable terminal : Error.t option;
    mutable done_seen : bool;
    mutable active : bool;
    unsubscribe_request : unit -> (unit, Error.t) result;
  }

  let create ~sid ~capacity ~unsubscribe_request =
    {
      sid;
      queue =
        Eio.Stream.create (if capacity = max_int then max_int else capacity + 1);
      capacity;
      terminal = None;
      done_seen = false;
      active = true;
      unsubscribe_request;
    }

  let sid t = t.sid

  let push t (delivery : delivery) =
    if (not t.active) || Option.is_some t.terminal then false
    else if Eio.Stream.length t.queue >= t.capacity then false
    else (
      Eio.Stream.add t.queue (Message delivery);
      true)

  let terminate t error =
    t.active <- false;
    if Option.is_none t.terminal then (
      t.terminal <- Some error;
      Eio.Stream.add t.queue (Done error))

  let next t =
    if t.done_seen then
      match t.terminal with Some error -> Error error | None -> assert false
    else
      match Eio.Stream.take t.queue with
      | Message delivery -> Ok delivery
      | Done error ->
          t.done_seen <- true;
          Error error

  let unsubscribe t = if not t.active then Ok () else t.unsubscribe_request ()
end

type input = Data of bytes | Eof | Failure of exn

type work = Command of command | Input_ready | Timer of int

and command =
  | Publish of {
      message : Nats.Message.t;
      resolver : (unit, Error.t) result Eio.Promise.u;
    }
  | Subscribe of {
      subject : Nats.Subject.Filter.t;
      queue_group : Nats.Queue_group.t option;
      resolver : (Subscription.t, Error.t) result Eio.Promise.u;
    }
  | Unsubscribe of {
      sid : int;
      resolver : (unit, Error.t) result Eio.Promise.u;
    }
  | Flush of {
      timeout : Mtime.Span.t;
      resolver : (unit, Error.t) result Eio.Promise.u;
    }
  | Drain of {
      timeout : Mtime.Span.t;
      resolver : (unit, Error.t) result Eio.Promise.u;
    }
  | Close of { resolver : (unit, Error.t) result Eio.Promise.u }

type flush_waiter = {
  resolver : (unit, Error.t) result Eio.Promise.u;
  deadline : Mtime.t;
}

type barrier =
  | Flush_waiter of flush_waiter
  | Drain_waiter of {
      resolver : (unit, Error.t) result Eio.Promise.u;
      deadline : Mtime.t;
    }

type transport = {
  read : Cstruct.t -> int;
  write : string -> unit;
  close : unit -> unit;
}

type monotonic_clock = { now : unit -> Mtime.t; sleep_until : Mtime.t -> unit }

type t = {
  sw : Eio.Switch.t;
  flow : transport;
  clock : monotonic_clock;
  config : Config.t;
  work : work Eio.Stream.t;
  input : input Eio.Stream.t;
  pending : Buffer.t;
  events : Event_stream.t;
  mutable state : Nats.Client.t;
  subscriptions : (int, Subscription.t) Hashtbl.t;
  mutable eof_seen : bool;
  mutable closed : bool;
  mutable connect_sent : bool;
  mutable pending_commands : int;
  mutable timer_generation : int;
  mutable scheduled_deadline : Mtime.t option;
  ready_promise : (unit, Error.t) result Eio.Promise.t;
  ready : (unit, Error.t) result Eio.Promise.u;
  barriers : barrier Queue.t;
}

let protocol error = Error.Protocol error
let io_error error = Error.Io error

let command_error = function
  | Nats.Error.Closed -> Error.Closed
  | Nats.Error.Draining -> Error.Draining
  | error -> protocol error

let add_input input work value =
  Eio.Stream.add input value;
  Eio.Stream.add work Input_ready

let rec read_loop transport input work chunk_size =
  let buffer = Cstruct.create chunk_size in
  try
    let length = transport.read buffer in
    if length <= 0 then add_input input work Eof
    else (
      add_input input work
        (Data (Cstruct.to_bytes (Cstruct.sub buffer 0 length)));
      read_loop transport input work chunk_size)
  with
  | Eio.Cancel.Cancelled _ -> ()
  | End_of_file -> add_input input work Eof
  | Eio.Io (_, _) as error -> add_input input work (Failure error)

let now t = t.clock.now ()

let write_outputs t output =
  let rec loop = function
    | [] -> Ok ()
    | value :: rest -> (
        try
          t.flow.write value;
          loop rest
        with
        | End_of_file -> Error Error.Disconnected
        | Eio.Io (_, _) as error -> Error (io_error error))
  in
  loop output

let resolve_unit resolver value = Eio.Promise.resolve resolver value
let resolve_ready t value = Eio.Promise.resolve t.ready value

let event t event =
  if Event_stream.push t.events event then Ok ()
  else Error (Error.Slow_consumer Error.Events)

let close_subscription t sid error =
  match Hashtbl.find_opt t.subscriptions sid with
  | None -> ()
  | Some subscription ->
      Hashtbl.remove t.subscriptions sid;
      Subscription.terminate subscription error

let close_subscriptions t error =
  let sids = Hashtbl.fold (fun sid _ acc -> sid :: acc) t.subscriptions [] in
  List.iter (fun sid -> close_subscription t sid error) sids

let fail_waiter resolver error = resolve_unit resolver (Error error)

let fail_command = function
  | Publish { resolver; _ } -> fail_waiter resolver Error.Closed
  | Subscribe { resolver; _ } -> fail_waiter resolver Error.Closed
  | Unsubscribe { resolver; _ } -> fail_waiter resolver Error.Closed
  | Flush { resolver; _ } -> fail_waiter resolver Error.Closed
  | Drain { resolver; _ } -> fail_waiter resolver Error.Closed
  | Close { resolver } -> resolve_unit resolver (Ok ())

let fail_pending_commands t =
  let rec loop () =
    match Eio.Stream.take_nonblocking t.work with
    | None -> ()
    | Some (Command command) ->
        fail_command command;
        loop ()
    | Some (Input_ready | Timer _) -> loop ()
  in
  loop ();
  t.pending_commands <- 0

let finish t error =
  if not t.closed then (
    t.closed <- true;
    (match error with
    | Error.Disconnected | Error.Io _ | Error.Timeout ->
        Event_stream.push_terminal t.events Event.Disconnected
    | Error.Slow_consumer kind ->
        Event_stream.push_terminal t.events (Event.Slow_consumer kind)
    | Error.Invalid_capacity _ | Error.Command_queue_full _
    | Error.Invalid_chunk_size _ | Error.Invalid_timeout _ ->
        ()
    | Error.Protocol _ | Error.Draining | Error.Closed -> ());
    close_subscriptions t error;
    Queue.iter
      (function
        | Flush_waiter waiter -> fail_waiter waiter.resolver error
        | Drain_waiter waiter -> fail_waiter waiter.resolver error)
      t.barriers;
    Queue.clear t.barriers;
    fail_pending_commands t;
    (match Eio.Promise.peek t.ready_promise with
    | Some _ -> ()
    | None -> resolve_ready t (Error error));
    Event_stream.terminate t.events error;
    t.flow.close ())

let remove_finished_subscriptions t =
  let active = Nats.Client.subscriptions t.state in
  let is_active sid =
    List.exists
      (fun (subscription : Nats.Client.subscription) ->
        Int.equal sid subscription.sid)
      active
  in
  let finished = ref [] in
  Hashtbl.iter
    (fun sid _ -> if not (is_active sid) then finished := sid :: !finished)
    t.subscriptions;
  List.iter (fun sid -> close_subscription t sid Error.Closed) !finished

let handle_event t event =
  match event with
  | Nats.Event.Info _ -> Ok ()
  | Nats.Event.Connected ->
      if not t.connect_sent then Ok ()
      else if Eio.Promise.is_resolved t.ready_promise then Ok ()
      else (
        resolve_ready t (Ok ());
        Ok ())
  | Nats.Event.Flush_completed -> (
      if Queue.is_empty t.barriers then Ok ()
      else
        match Queue.take t.barriers with
        | Flush_waiter waiter ->
            resolve_unit waiter.resolver (Ok ());
            Ok ()
        | Drain_waiter waiter ->
            resolve_unit waiter.resolver (Ok ());
            finish t Error.Closed;
            Ok ())
  | Nats.Event.Closed -> Ok ()
  | Nats.Event.Draining -> Ok ()
  | Nats.Event.Lame_duck_mode | Nats.Event.Server_error _
  | Nats.Event.Protocol_notice _ ->
      Ok ()

let handle_events t events =
  let rec loop = function
    | [] -> Ok ()
    | event_value :: rest -> (
        match event t (Event.Core event_value) with
        | Error error -> Error error
        | Ok () -> (
            match handle_event t event_value with
            | Error error -> Error error
            | Ok () -> loop rest))
  in
  loop events

let handle_deliveries t deliveries =
  let rec loop = function
    | [] -> Ok ()
    | (delivery : Nats.Client.delivery) :: rest -> (
        match Hashtbl.find_opt t.subscriptions delivery.sid with
        | None -> loop rest
        | Some subscription -> (
            let value : Subscription.delivery =
              { message = delivery.message; status = delivery.status }
            in
            if Subscription.push subscription value then (
              remove_finished_subscriptions t;
              loop rest)
            else
              let slow_consumer =
                Error.Slow_consumer (Error.Subscription { sid = delivery.sid })
              in
              close_subscription t delivery.sid slow_consumer;
              match
                event t
                  (Event.Slow_consumer
                     (Error.Subscription { sid = delivery.sid }))
              with
              | Error error -> Error error
              | Ok () -> (
                  let unsubscribe =
                    Nats.Client.outgoing t.state
                      (Nats.Client.Unsubscribe { sid = delivery.sid })
                  in
                  match unsubscribe with
                  | Ok transition ->
                      t.state <- transition.state;
                      write_outputs t transition.output
                  | Error (Nats.Error.Unknown_subscription _) -> Ok ()
                  | Error error -> Error (protocol error))))
  in
  loop deliveries

let apply_transition t (transition : Nats.Client.transition) =
  t.state <- transition.state;
  match write_outputs t transition.output with
  | Error error -> Error error
  | Ok () -> (
      match handle_events t transition.events with
      | Error error -> Error error
      | Ok () -> handle_deliveries t transition.deliveries)

let connect_after_info t =
  if
    Nats.Client.phase t.state = Nats.Client.Awaiting_connect
    && not t.connect_sent
  then (
    match
      Nats.Client.outgoing t.state (Nats.Client.Connect t.config.credentials)
    with
    | Error error -> Error (protocol error)
    | Ok transition ->
        t.connect_sent <- true;
        apply_transition t transition)
  else Ok ()

let rec drain_input t =
  match Eio.Stream.take_nonblocking t.input with
  | None -> Ok ()
  | Some (Data bytes) ->
      Buffer.add_bytes t.pending bytes;
      drain_input t
  | Some Eof ->
      t.eof_seen <- true;
      drain_input t
  | Some (Failure error) -> Error (io_error error)

let consume_pending t length =
  if length > 0 then
    let value = Buffer.contents t.pending in
    let remaining = String.length value - length in
    Buffer.clear t.pending;
    if remaining > 0 then Buffer.add_substring t.pending value length remaining

let apply_incoming t =
  match drain_input t with
  | Error error -> Error error
  | Ok () ->
      let rec loop () =
        let end_of_data = t.eof_seen in
        let value = Buffer.contents t.pending in
        if String.length value = 0 && not end_of_data then Ok ()
        else
          let reader = Bytesrw.Bytes.Reader.of_string value in
          match Nats.Client.incoming ~eod:end_of_data t.state ~now:(now t) reader with
          | Error error -> Error (protocol error)
          | Ok transition ->
              let consumed = Bytesrw.Bytes.Reader.pos reader in
              if Int.equal consumed 0 && not end_of_data then Ok ()
              else (
                match apply_transition t transition with
                | Error error -> Error error
                | Ok () ->
                    consume_pending t consumed;
                    if Nats.Client.phase t.state = Nats.Client.Closed then (
                      finish t Error.Disconnected;
                      Ok ())
                    else
                      match connect_after_info t with
                      | Error error -> Error error
                      | Ok () ->
                          if end_of_data || Buffer.length t.pending > 0 then
                            loop ()
                          else Ok ())
      in
      loop ()

let apply_outgoing t command =
  match command with
  | Publish { message; resolver } -> (
      match Nats.Client.outgoing t.state (Nats.Client.Publish message) with
      | Error error ->
          fail_waiter resolver (command_error error);
          Ok ()
      | Ok transition -> (
          match apply_transition t transition with
          | Error error ->
              fail_waiter resolver error;
              Error error
          | Ok () ->
              resolve_unit resolver (Ok ());
              Ok ()))
  | Subscribe { subject; queue_group; resolver } -> (
      match
        Nats.Client.outgoing t.state
          (Nats.Client.Subscribe { subject; queue_group })
      with
      | Error error ->
          fail_waiter resolver (command_error error);
          Ok ()
      | Ok transition -> (
          match transition.subscription_id with
          | None ->
              let error =
                protocol
                  (Nats.Error.Codec
                     (Nats.Codec.Invalid_operation
                        { keyword = "SUB did not allocate an id" }))
              in
              fail_waiter resolver error;
              Error error
          | Some sid -> (
              let subscription =
                Subscription.create ~sid
                  ~capacity:t.config.subscription_capacity
                  ~unsubscribe_request:(fun () ->
                    if t.closed then Error Error.Closed
                    else if
                      t.pending_commands >= t.config.Config.command_capacity
                    then
                      Error
                        (Error.Command_queue_full
                           { capacity = t.config.Config.command_capacity })
                    else
                      let promise, resolver = Eio.Promise.create () in
                      t.pending_commands <- t.pending_commands + 1;
                      Eio.Stream.add t.work
                        (Command (Unsubscribe { sid; resolver }));
                      Eio.Promise.await promise)
              in
              Hashtbl.replace t.subscriptions sid subscription;
              match apply_transition t transition with
              | Error error ->
                  close_subscription t sid error;
                  fail_waiter resolver error;
                  Error error
              | Ok () ->
                  resolve_unit resolver (Ok subscription);
                  Ok ())))
  | Unsubscribe { sid; resolver } -> (
      match Nats.Client.outgoing t.state (Nats.Client.Unsubscribe { sid }) with
      | Error error ->
          fail_waiter resolver (command_error error);
          Ok ()
      | Ok transition -> (
          match apply_transition t transition with
          | Error error ->
              fail_waiter resolver error;
              Error error
          | Ok () ->
              close_subscription t sid Error.Closed;
              resolve_unit resolver (Ok ());
              Ok ()))
  | Flush { timeout; resolver } -> (
      match Nats.Client.outgoing t.state Nats.Client.Flush with
      | Error error ->
          fail_waiter resolver (command_error error);
          Ok ()
      | Ok transition -> (
          match apply_transition t transition with
          | Error error ->
              fail_waiter resolver error;
              Error error
          | Ok () -> (
              match Mtime.add_span (now t) timeout with
              | None ->
                  fail_waiter resolver Error.Timeout;
                  Error Error.Timeout
              | Some deadline ->
                  Queue.add (Flush_waiter { resolver; deadline }) t.barriers;
                  Ok ())))
  | Drain { timeout; resolver } -> (
      match Nats.Client.outgoing t.state Nats.Client.Drain with
      | Error error ->
          fail_waiter resolver (command_error error);
          Ok ()
      | Ok transition -> (
          match apply_transition t transition with
          | Error error ->
              fail_waiter resolver error;
              Error error
          | Ok () -> (
              match Mtime.add_span (now t) timeout with
              | None ->
                  fail_waiter resolver Error.Timeout;
                  Error Error.Timeout
              | Some deadline ->
                  Queue.add (Drain_waiter { resolver; deadline }) t.barriers;
                  close_subscriptions t Error.Draining;
                  Ok ())))
  | Close { resolver } -> (
      match Nats.Client.outgoing t.state Nats.Client.Close with
      | Error Nats.Error.Closed ->
          resolve_unit resolver (Ok ());
          finish t Error.Closed;
          Ok ()
      | Error error ->
          fail_waiter resolver (command_error error);
          Ok ()
      | Ok transition -> (
          match apply_transition t transition with
          | Error error ->
              fail_waiter resolver error;
              Error error
          | Ok () ->
              resolve_unit resolver (Ok ());
              finish t Error.Closed;
              Ok ()))

let timer_deadline t =
  let earliest current candidate =
    match current with
    | None -> Some candidate
    | Some current ->
        if Mtime.is_earlier candidate ~than:current then Some candidate
        else Some current
  in
  let deadline = Nats.Client.next_timeout t.state in
  match Queue.peek_opt t.barriers with
  | None -> deadline
  | Some (Flush_waiter waiter) -> earliest deadline waiter.deadline
  | Some (Drain_waiter waiter) -> earliest deadline waiter.deadline

let same_deadline left right =
  match (left, right) with
  | None, None -> true
  | Some left, Some right -> Int.equal (Mtime.compare left right) 0
  | None, Some _ | Some _, None -> false

let schedule_timer t =
  let deadline = timer_deadline t in
  if (not t.closed) && not (same_deadline t.scheduled_deadline deadline) then (
    t.timer_generation <- t.timer_generation + 1;
    t.scheduled_deadline <- deadline;
    match deadline with
    | None -> ()
    | Some deadline ->
        let generation = t.timer_generation in
        Eio.Fiber.fork ~sw:t.sw (fun () ->
            t.clock.sleep_until deadline;
            if not t.closed then Eio.Stream.add t.work (Timer generation)))

let apply_timer t generation =
  if Int.equal generation t.timer_generation then
    let current = now t in
    let barrier_due =
      match Queue.peek_opt t.barriers with
      | None -> false
      | Some (Flush_waiter waiter) -> Mtime.compare current waiter.deadline >= 0
      | Some (Drain_waiter waiter) -> Mtime.compare current waiter.deadline >= 0
    in
    if barrier_due then finish t Error.Timeout
    else
      let transition = Nats.Client.timer t.state ~now:current in
      match apply_transition t transition with
      | Error error -> finish t error
      | Ok () ->
          if Nats.Client.phase t.state = Nats.Client.Closed then
            finish t Error.Timeout

let rec owner_loop t =
  if not t.closed then
    match Eio.Stream.take t.work with
    | Command command -> (
        t.pending_commands <- t.pending_commands - 1;
        match apply_outgoing t command with
        | Ok () ->
            schedule_timer t;
            owner_loop t
        | Error error -> finish t error)
    | Input_ready -> (
        match apply_incoming t with
        | Ok () ->
            schedule_timer t;
            owner_loop t
        | Error error -> finish t error)
    | Timer generation ->
        apply_timer t generation;
        schedule_timer t;
        owner_loop t

let create ~sw ~clock ~config flow =
  let clock =
    {
      now = (fun () -> Eio.Time.Mono.now clock);
      sleep_until = (fun deadline -> Eio.Time.Mono.sleep_until clock deadline);
    }
  in
  let transport =
    {
      read = (fun buffer -> Eio.Flow.single_read flow buffer);
      write = (fun value -> Eio.Flow.copy_string value flow);
      close = (fun () -> Eio.Flow.close flow);
    }
  in
  let input = Eio.Stream.create config.Config.read_capacity in
  let ready, ready_resolver = Eio.Promise.create () in
  let connection =
    {
      sw;
      flow = transport;
      clock;
      config;
      work = Eio.Stream.create max_int;
      input;
      pending = Buffer.create 4096;
      events = Event_stream.create config.Config.event_capacity;
      state = Nats.Client.v config.Config.core;
      subscriptions = Hashtbl.create 16;
      eof_seen = false;
      closed = false;
      connect_sent = false;
      pending_commands = 0;
      timer_generation = 0;
      scheduled_deadline = None;
      ready_promise = ready;
      ready = ready_resolver;
      barriers = Queue.create ();
    }
  in
  Eio.Fiber.fork ~sw (fun () ->
      read_loop transport input connection.work config.Config.read_chunk_size);
  Eio.Fiber.fork ~sw (fun () -> owner_loop connection);
  (connection, ready)

let connect ~sw ~net ~clock ?(config = Config.default) address =
  try
    let flow = Eio.Net.connect ~sw net address in
    let connection, ready = create ~sw ~clock ~config flow in
    match Eio.Promise.await ready with
    | Ok () -> Ok connection
    | Error error -> Error error
  with
  | End_of_file -> Error Error.Disconnected
  | Eio.Io (_, _) as error -> Error (io_error error)

let send t command promise =
  if t.closed then Error Error.Closed
  else if t.pending_commands >= t.config.Config.command_capacity then
    Error
      (Error.Command_queue_full { capacity = t.config.Config.command_capacity })
  else (
    t.pending_commands <- t.pending_commands + 1;
    Eio.Stream.add t.work (Command command);
    Eio.Promise.await promise)

let publish_msg t message =
  let promise, resolver = Eio.Promise.create () in
  send t (Publish { message; resolver }) promise

let publish t ?reply_to ?(headers = Nats.Header.empty) subject payload =
  publish_msg t (Nats.Message.v ~subject ?reply_to ~headers payload)

let subscribe t ?queue_group subject =
  let promise, resolver = Eio.Promise.create () in
  send t (Subscribe { subject; queue_group; resolver }) promise

let validate_timeout name timeout =
  if Mtime.Span.compare timeout Mtime.Span.zero > 0 then Ok timeout
  else Error (Error.Invalid_timeout name)

let flush ?timeout t =
  let timeout = Option.value timeout ~default:t.config.Config.flush_timeout in
  match validate_timeout "flush" timeout with
  | Error error -> Error error
  | Ok timeout ->
      let promise, resolver = Eio.Promise.create () in
      send t (Flush { timeout; resolver }) promise

let drain ?timeout t =
  let timeout = Option.value timeout ~default:t.config.Config.drain_timeout in
  match validate_timeout "drain" timeout with
  | Error error -> Error error
  | Ok timeout ->
      let promise, resolver = Eio.Promise.create () in
      send t (Drain { timeout; resolver }) promise

let close t =
  if t.closed then Ok ()
  else
    let promise, resolver = Eio.Promise.create () in
    send t (Close { resolver }) promise

let events t = t.events
