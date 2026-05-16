module Config = struct
  type t = {
    core : Nats.Config.t;
    credentials : Nats.Client.Connect.t;
    command_capacity : int;
    subscription_capacity : int;
    event_capacity : int;
    read_capacity : int;
    read_chunk_size : int;
    inbox_prefix : Nats.Subject.t;
    tls : Tls.Config.client option;
    tls_required : bool;
    handshake_timeout : Mtime.Span.t;
    request_timeout : Mtime.Span.t;
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
      ?tls ?(tls_required = false) ?(inbox_prefix = "_INBOX.ocaml")
      ?(handshake_timeout = default_span) ?(request_timeout = default_span)
      ?(flush_timeout = default_span) ?(drain_timeout = default_span) () =
    if tls_required && Option.is_none tls then Error Error.Tls_required
    else
      match Nats.Subject.of_string inbox_prefix with
      | Error error -> Error (Error.Invalid_inbox_prefix error)
      | Ok inbox_prefix -> (
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
                            match
                              validate_timeout "handshake" handshake_timeout
                            with
                            | Error error -> Error error
                            | Ok handshake_timeout -> (
                                match
                                  validate_timeout "request" request_timeout
                                with
                                | Error error -> Error error
                                | Ok request_timeout -> (
                                    match
                                      validate_timeout "flush" flush_timeout
                                    with
                                    | Error error -> Error error
                                    | Ok flush_timeout -> (
                                        match
                                          validate_timeout "drain" drain_timeout
                                        with
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
                                                inbox_prefix;
                                                tls;
                                                tls_required;
                                                handshake_timeout;
                                                request_timeout;
                                                flush_timeout;
                                                drain_timeout;
                                              }))))))))

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

type subscription_drain_waiter = {
  sid : int;
  promise : (unit, Error.t) result Eio.Promise.t;
  resolver : (unit, Error.t) result Eio.Promise.u;
  deadline : Mtime.t;
  mutable server_flushed : bool;
  mutable done_seen : bool;
  mutable completed : bool;
  on_complete : (unit, Error.t) result -> unit;
}

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
    mutable drain_waiter : subscription_drain_waiter option;
    mutable drain_promise : (unit, Error.t) result Eio.Promise.t option;
    mutable drain_resolver : (unit, Error.t) result Eio.Promise.u option;
    mutable drain_result : (unit, Error.t) result option;
    unsubscribe_request : unit -> (unit, Error.t) result;
    auto_unsubscribe_request : max_messages:int -> (unit, Error.t) result;
    drain_request :
      timeout:Mtime.Span.t option ->
      promise:(unit, Error.t) result Eio.Promise.t ->
      resolver:(unit, Error.t) result Eio.Promise.u ->
      (unit, Error.t) result;
    cancel_drain_request : unit -> unit;
  }

  let create ~sid ~capacity ~unsubscribe_request ~auto_unsubscribe_request
      ~drain_request ~cancel_drain_request =
    {
      sid;
      queue =
        Eio.Stream.create (if capacity = max_int then max_int else capacity + 1);
      capacity;
      terminal = None;
      done_seen = false;
      active = true;
      drain_waiter = None;
      drain_promise = None;
      drain_resolver = None;
      drain_result = None;
      unsubscribe_request;
      auto_unsubscribe_request;
      drain_request;
      cancel_drain_request;
    }

  let sid t = t.sid

  let push t (delivery : delivery) =
    if (not t.active) || Option.is_some t.terminal then false
    else if Eio.Stream.length t.queue >= t.capacity then false
    else (
      Eio.Stream.add t.queue (Message delivery);
      true)

  let complete_drain_waiter waiter result =
    if not waiter.completed then (
      waiter.completed <- true;
      waiter.on_complete result;
      Eio.Promise.resolve waiter.resolver result)

  let clear_drain_state t =
    t.drain_promise <- None;
    t.drain_resolver <- None

  let record_drain_result t result = t.drain_result <- Some result

  let complete_drain t waiter result =
    t.drain_waiter <- None;
    record_drain_result t result;
    clear_drain_state t;
    complete_drain_waiter waiter result

  let arm_drain t waiter = t.drain_waiter <- Some waiter

  let fail_pending_drain t error =
    match t.drain_waiter with
    | Some waiter -> complete_drain t waiter (Error error)
    | None -> (
        match t.drain_resolver with
        | None -> ()
        | Some resolver ->
            t.drain_result <- Some (Error error);
            clear_drain_state t;
            Eio.Promise.resolve resolver (Error error))

  let cancel_drain t = fail_pending_drain t Error.Closed
  let fail_drain t error = fail_pending_drain t error

  let terminate t error =
    t.active <- false;
    if Option.is_none t.terminal then (
      t.terminal <- Some error;
      Eio.Stream.add t.queue (Done error));
    fail_pending_drain t error

  let terminate_for_drain t =
    t.active <- false;
    if Option.is_none t.terminal then (
      t.terminal <- Some Error.Closed;
      Eio.Stream.add t.queue (Done Error.Closed));
    match t.drain_waiter with
    | None -> ()
    | Some waiter ->
        waiter.done_seen <- true;
        if waiter.server_flushed then complete_drain t waiter (Ok ())

  let next t =
    if t.done_seen then
      match t.terminal with Some error -> Error error | None -> assert false
    else
      match Eio.Stream.take t.queue with
      | Message delivery -> Ok delivery
      | Done error ->
          t.done_seen <- true;
          (match t.drain_waiter with
          | None -> ()
          | Some waiter ->
              waiter.done_seen <- true;
              if waiter.server_flushed then complete_drain t waiter (Ok ()));
          Error error

  let unsubscribe t = if not t.active then Ok () else t.unsubscribe_request ()

  let auto_unsubscribe t ~max_messages =
    if not t.active then Ok () else t.auto_unsubscribe_request ~max_messages

  let drain ?timeout t =
    match t.drain_promise with
    | Some promise -> Eio.Promise.await promise
    | None -> (
        match t.drain_result with
        | Some result -> result
        | None -> (
            if not t.active then Ok ()
            else
              let promise, resolver = Eio.Promise.create () in
              t.drain_promise <- Some promise;
              t.drain_resolver <- Some resolver;
              match t.drain_request ~timeout ~promise ~resolver with
              | Error error ->
                  clear_drain_state t;
                  Error error
              | Ok () -> (
                  try Eio.Promise.await promise
                  with Eio.Cancel.Cancelled _ as cancellation ->
                    Eio.Cancel.protect (fun () -> t.cancel_drain_request ());
                    raise cancellation)))

  let iter t ~f =
    let rec loop () =
      match next t with
      | Ok delivery ->
          f delivery;
          loop ()
      | Error (Error.Closed | Error.Draining) -> Ok ()
      | Error error -> Error error
    in
    loop ()
end

type input = Data of bytes | Eof | Io_failure of exn | Failure of exn

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
  | Auto_unsubscribe of {
      sid : int;
      max_messages : int;
      resolver : (unit, Error.t) result Eio.Promise.u;
    }
  | Drain_subscription of {
      sid : int;
      timeout : Mtime.Span.t;
      subscription : Subscription.t;
      promise : (unit, Error.t) result Eio.Promise.t;
      resolver : (unit, Error.t) result Eio.Promise.u;
    }
  | Cancel_subscription_drain of { sid : int }
  | Request of {
      message : Nats.Message.t;
      timeout : Mtime.Span.t;
      setup : (int, Error.t) result Eio.Promise.u;
      resolver : (Nats.Message.t, Error.t) result Eio.Promise.u;
    }
  | Cancel_request of { sid : int }
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
  mutable completed : bool;
}

type request_waiter = {
  resolver : (Nats.Message.t, Error.t) result Eio.Promise.u;
  deadline : Mtime.t;
}

type barrier =
  | Flush_waiter of flush_waiter
  | Drain_waiter of {
      resolver : (unit, Error.t) result Eio.Promise.u;
      deadline : Mtime.t;
    }
  | Subscription_drain_waiter of subscription_drain_waiter

type flow =
  | Flow :
      ([> Eio.Flow.two_way_ty | Eio.Resource.close_ty ] as 'a) Eio.Resource.t
      -> flow

type transport = { mutable flow : flow }

type reader = {
  cancel : Eio.Cancel.t;
  done_ : unit Eio.Promise.t;
  finished : bool ref;
  stopped : bool ref;
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
  requests : (int, request_waiter) Hashtbl.t;
  subscription_drains : (int, subscription_drain_waiter) Hashtbl.t;
  mutable reader : reader option;
  mutable eof_seen : bool;
  mutable closed : bool;
  mutable connect_sent : bool;
  mutable tls_active : bool;
  mutable tls_info_received : bool;
  mutable pending_commands : int;
  mutable active_request_setup : (int, Error.t) result Eio.Promise.u option;
  mutable handshake_deadline : Mtime.t option;
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

let rec read_loop (transport : transport) input work chunk_size stopped =
  let buffer = Cstruct.create chunk_size in
  try
    let length =
      match transport.flow with Flow flow -> Eio.Flow.single_read flow buffer
    in
    if length <= 0 then (if not !stopped then add_input input work Eof)
    else if not !stopped then (
      add_input input work
        (Data (Cstruct.to_bytes (Cstruct.sub buffer 0 length)));
      read_loop transport input work chunk_size stopped)
  with
  | Eio.Cancel.Cancelled _ -> ()
  | End_of_file -> if not !stopped then add_input input work Eof
  | Eio.Io (_, _) as error ->
      if not !stopped then add_input input work (Io_failure error)
  | error -> if not !stopped then add_input input work (Failure error)

let start_reader t =
  let cancel_promise, cancel_resolver = Eio.Promise.create () in
  let done_promise, done_resolver = Eio.Promise.create () in
  let finished = ref false in
  let stopped = ref false in
  Eio.Fiber.fork ~sw:t.sw (fun () ->
      Fun.protect
        ~finally:(fun () ->
          finished := true;
          Eio.Promise.resolve done_resolver ())
        (fun () ->
          Eio.Cancel.sub (fun cancel ->
              Eio.Promise.resolve cancel_resolver cancel;
              read_loop t.flow t.input t.work t.config.Config.read_chunk_size
                stopped)));
  let cancel = Eio.Promise.await cancel_promise in
  t.reader <- Some { cancel; done_ = done_promise; finished; stopped }

let stop_reader t =
  match t.reader with
  | None -> ()
  | Some reader ->
      t.reader <- None;
      reader.stopped := true;
      if not !(reader.finished) then Eio.Cancel.cancel reader.cancel End_of_file;
      Eio.Promise.await reader.done_

let write_flow flow value =
  match flow with Flow flow -> Eio.Flow.copy_string value flow

let close_flow flow = match flow with Flow flow -> Eio.Flow.close flow

let transport_error t error =
  if t.tls_active then Error (Error.Tls error) else Error (Error.Io error)

let now t = t.clock.now ()
let inbox_counter = Atomic.make 0

let fresh_inbox t =
  let sequence = Atomic.fetch_and_add inbox_counter 1 in
  let timestamp = Mtime.to_uint64_ns (now t) in
  Nats.Subject.literal
    (Format.asprintf "%s.%Ld.%d"
       (Nats.Subject.to_string t.config.Config.inbox_prefix)
       timestamp sequence)

let write_outputs t output =
  let rec loop = function
    | [] -> Ok ()
    | value :: rest -> (
        try
          write_flow t.flow.flow value;
          loop rest
        with
        | Eio.Cancel.Cancelled _ as error -> raise error
        | End_of_file -> Error Error.Disconnected
        | Eio.Io (_, _) as error -> Error (io_error error)
        | error -> transport_error t error)
  in
  loop output

let resolve_unit resolver value = Eio.Promise.resolve resolver value
let resolve_ready t value = Eio.Promise.resolve t.ready value

let resolve_setup t resolver value =
  t.active_request_setup <- None;
  resolve_unit resolver value

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
  | Auto_unsubscribe { resolver; _ } -> fail_waiter resolver Error.Closed
  | Drain_subscription { subscription; _ } ->
      Subscription.fail_pending_drain subscription Error.Closed
  | Cancel_subscription_drain _ -> ()
  | Request { setup; _ } -> resolve_unit setup (Error Error.Closed)
  | Cancel_request _ -> ()
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

let fail_requests t error =
  let requests =
    Hashtbl.fold (fun sid waiter acc -> (sid, waiter) :: acc) t.requests []
  in
  Hashtbl.clear t.requests;
  List.iter (fun (_sid, waiter) -> fail_waiter waiter.resolver error) requests

let fail_subscription_drains t error =
  let waiters =
    Hashtbl.fold (fun _sid waiter acc -> waiter :: acc) t.subscription_drains []
  in
  List.iter
    (fun waiter -> Subscription.complete_drain_waiter waiter (Error error))
    waiters

let finish t error =
  if not t.closed then (
    t.closed <- true;
    (match t.active_request_setup with
    | None -> ()
    | Some resolver ->
        t.active_request_setup <- None;
        resolve_unit resolver (Error error));
    (match error with
    | Error.Disconnected | Error.Io _ | Error.Tls _ | Error.Tls_required
    | Error.Tls_unexpected_input | Error.Timeout ->
        Event_stream.push_terminal t.events Event.Disconnected
    | Error.Slow_consumer kind ->
        Event_stream.push_terminal t.events (Event.Slow_consumer kind)
    | Error.Invalid_capacity _ | Error.Command_queue_full _
    | Error.Invalid_chunk_size _ | Error.Invalid_inbox_prefix _
    | Error.Invalid_timeout _ | Error.No_responders ->
        ()
    | Error.Protocol _ | Error.Draining | Error.Closed -> ());
    close_subscriptions t error;
    fail_requests t error;
    fail_subscription_drains t error;
    Queue.iter
      (function
        | Flush_waiter waiter ->
            if not waiter.completed then fail_waiter waiter.resolver error
        | Drain_waiter waiter -> fail_waiter waiter.resolver error
        | Subscription_drain_waiter waiter ->
            Subscription.complete_drain_waiter waiter (Error error))
      t.barriers;
    Queue.clear t.barriers;
    fail_pending_commands t;
    (match Eio.Promise.peek t.ready_promise with
    | Some _ -> ()
    | None -> resolve_ready t (Error error));
    Event_stream.terminate t.events error;
    close_flow t.flow.flow)

let remove_finished_subscriptions t =
  let active = Nats.Client.subscriptions t.state in
  let is_active sid =
    List.exists
      (fun (subscription : Nats.Client.subscription) ->
        Int.equal sid subscription.sid)
      active
  in
  let is_draining sid = Hashtbl.mem t.subscription_drains sid in
  let finished = ref [] in
  Hashtbl.iter
    (fun sid _ ->
      if (not (is_active sid)) && not (is_draining sid) then
        finished := sid :: !finished)
    t.subscriptions;
  List.iter (fun sid -> close_subscription t sid Error.Closed) !finished

let unsubscribe_sid t sid =
  match Nats.Client.outgoing t.state (Nats.Client.Unsubscribe { sid }) with
  | Error (Nats.Error.Unknown_subscription _) -> Ok ()
  | Error error -> Error (command_error error)
  | Ok transition ->
      t.state <- transition.state;
      write_outputs t transition.output

let handle_event t event =
  match event with
  | Nats.Event.Info _ -> Ok ()
  | Nats.Event.Connected ->
      t.handshake_deadline <- None;
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
            if not waiter.completed then (
              waiter.completed <- true;
              resolve_unit waiter.resolver (Ok ()))
            else ();
            Ok ()
        | Drain_waiter waiter ->
            resolve_unit waiter.resolver (Ok ());
            finish t Error.Closed;
            Ok ()
        | Subscription_drain_waiter waiter ->
            if waiter.completed then Ok ()
            else (
              waiter.server_flushed <- true;
              match Hashtbl.find_opt t.subscriptions waiter.sid with
              | None ->
                  Subscription.complete_drain_waiter waiter (Error Error.Closed);
                  Ok ()
              | Some subscription ->
                  if waiter.done_seen then
                    Subscription.complete_drain subscription waiter (Ok ())
                  else Hashtbl.replace t.subscription_drains waiter.sid waiter;
                  Ok ()))
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
        match Hashtbl.find_opt t.requests delivery.sid with
        | Some waiter -> (
            Hashtbl.remove t.requests delivery.sid;
            let result =
              match delivery.status with
              | Some { code = 503; _ } -> Error Error.No_responders
              | _ -> Ok delivery.message
            in
            resolve_unit waiter.resolver result;
            match unsubscribe_sid t delivery.sid with
            | Ok () -> loop rest
            | Error error -> Error error)
        | None -> (
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
                    Error.Slow_consumer
                      (Error.Subscription { sid = delivery.sid })
                  in
                  close_subscription t delivery.sid slow_consumer;
                  match
                    event t
                      (Event.Slow_consumer
                         (Error.Subscription { sid = delivery.sid }))
                  with
                  | Error error -> Error error
                  | Ok () -> (
                      match unsubscribe_sid t delivery.sid with
                      | Ok () -> loop rest
                      | Error error -> Error error))))
  in
  loop deliveries

let apply_transition t (transition : Nats.Client.transition) =
  t.state <- transition.state;
  if
    t.tls_active
    && List.exists
         (function Nats.Event.Info _ -> true | _ -> false)
         transition.events
  then t.tls_info_received <- true;
  match write_outputs t transition.output with
  | Error error -> Error error
  | Ok () -> (
      match handle_events t transition.events with
      | Error error -> Error error
      | Ok () -> handle_deliveries t transition.deliveries)

let rec drain_input t =
  match Eio.Stream.take_nonblocking t.input with
  | None -> Ok ()
  | Some (Data bytes) ->
      Buffer.add_bytes t.pending bytes;
      drain_input t
  | Some Eof ->
      t.eof_seen <- true;
      drain_input t
  | Some (Io_failure error) -> Error (io_error error)
  | Some (Failure error) -> transport_error t error

let tls_required_by_server t =
  match Nats.Client.info t.state with
  | None -> false
  | Some info -> Nats.Info.tls_required info

let tls_handshake t config =
  let (Flow flow) = t.flow.flow in
  try
    Eio.Fiber.first
      (fun () -> Ok (Tls_eio.client_of_flow config flow))
      (fun () ->
        match t.handshake_deadline with
        | None -> Error Error.Timeout
        | Some deadline ->
            t.clock.sleep_until deadline;
            Error Error.Timeout)
  with
  | Eio.Cancel.Cancelled _ as error -> raise error
  | End_of_file -> (
      match t.handshake_deadline with
      | Some deadline when Mtime.compare (now t) deadline >= 0 ->
          Error Error.Timeout
      | _ -> Error Error.Disconnected)
  | error ->
      (* TLS also reports RNG and provider failures as exceptions. Preserve the
         cause while keeping them on the connection's result boundary. *)
      Error (Error.Tls error)

let upgrade_tls t =
  match t.config.Config.tls with
  | None -> Error Error.Tls_required
  | Some config -> (
      stop_reader t;
      match drain_input t with
      | Error error -> Error error
      | Ok () -> (
          if t.eof_seen then Error Error.Disconnected
          else if Buffer.length t.pending > 0 then
            Error Error.Tls_unexpected_input
          else
            match tls_handshake t config with
            | Error error -> Error error
            | Ok flow ->
                t.flow.flow <- Flow flow;
                t.tls_active <- true;
                t.tls_info_received <- false;
                start_reader t;
                Ok ()))

let connect_after_info t =
  if
    Nats.Client.phase t.state = Nats.Client.Awaiting_connect
    && not t.connect_sent
  then (
    if
      (tls_required_by_server t || t.config.Config.tls_required)
      && not t.tls_active
    then upgrade_tls t
    else if t.tls_active && not t.tls_info_received then Ok ()
    else
      match
        Nats.Client.outgoing t.state
          (Nats.Client.Connect
             { credentials = t.config.credentials; tls_required = t.tls_active })
      with
      | Error error -> Error (protocol error)
      | Ok transition ->
          t.connect_sent <- true;
          apply_transition t transition)
  else Ok ()

let consume_pending t length =
  if length > 0 then (
    let value = Buffer.contents t.pending in
    let remaining = String.length value - length in
    Buffer.clear t.pending;
    if remaining > 0 then Buffer.add_substring t.pending value length remaining)

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
          match
            Nats.Client.incoming ~eod:end_of_data t.state ~now:(now t) reader
          with
          | Error error -> Error (protocol error)
          | Ok transition -> (
              let consumed = Bytesrw.Bytes.Reader.pos reader in
              if Int.equal consumed 0 && not end_of_data then Ok ()
              else
                match apply_transition t transition with
                | Error error -> Error error
                | Ok () -> (
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
                          else Ok ()))
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
                  ~auto_unsubscribe_request:(fun ~max_messages ->
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
                        (Command
                           (Auto_unsubscribe { sid; max_messages; resolver }));
                      Eio.Promise.await promise)
                  ~drain_request:(fun ~timeout ~promise ~resolver ->
                    let timeout =
                      Option.value timeout
                        ~default:t.config.Config.drain_timeout
                    in
                    if Mtime.Span.compare timeout Mtime.Span.zero <= 0 then
                      Error (Error.Invalid_timeout "drain")
                    else if t.closed then Error Error.Closed
                    else if
                      t.pending_commands >= t.config.Config.command_capacity
                    then
                      Error
                        (Error.Command_queue_full
                           { capacity = t.config.Config.command_capacity })
                    else
                      match Mtime.add_span (now t) timeout with
                      | None -> Error Error.Timeout
                      | Some _ -> (
                          match Hashtbl.find_opt t.subscriptions sid with
                          | None -> Error Error.Closed
                          | Some subscription ->
                              Eio.Cancel.protect (fun () ->
                                  t.pending_commands <- t.pending_commands + 1;
                                  Eio.Stream.add t.work
                                    (Command
                                       (Drain_subscription
                                          {
                                            sid;
                                            timeout;
                                            subscription;
                                            promise;
                                            resolver;
                                          })));
                              Ok ()))
                  ~cancel_drain_request:(fun () ->
                    Eio.Cancel.protect (fun () ->
                        if not t.closed then
                          Eio.Stream.add t.work
                            (Command (Cancel_subscription_drain { sid }))))
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
  | Auto_unsubscribe { sid; max_messages; resolver } -> (
      match
        Nats.Client.outgoing t.state
          (Nats.Client.Auto_unsubscribe { sid; max_messages })
      with
      | Error (Nats.Error.Unknown_subscription _) ->
          resolve_unit resolver (Ok ());
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
              Ok ()))
  | Drain_subscription { sid; timeout; subscription; promise; resolver } -> (
      match Hashtbl.find_opt t.subscriptions sid with
      | None ->
          Subscription.fail_pending_drain subscription Error.Closed;
          Ok ()
      | Some subscription -> (
          match Mtime.add_span (now t) timeout with
          | None ->
              Subscription.fail_pending_drain subscription Error.Timeout;
              Ok ()
          | Some deadline -> (
              let waiter =
                {
                  sid;
                  promise;
                  resolver;
                  deadline;
                  server_flushed = false;
                  done_seen = false;
                  completed = false;
                  on_complete =
                    (fun result ->
                      Subscription.record_drain_result subscription result;
                      Subscription.clear_drain_state subscription;
                      Hashtbl.remove t.subscription_drains sid;
                      Hashtbl.remove t.subscriptions sid);
                }
              in
              Subscription.arm_drain subscription waiter;
              Hashtbl.replace t.subscription_drains sid waiter;
              match
                Nats.Client.outgoing t.state (Nats.Client.Unsubscribe { sid })
              with
              | Error (Nats.Error.Unknown_subscription _) ->
                  Subscription.terminate subscription Error.Closed;
                  Ok ()
              | Error error ->
                  Subscription.terminate subscription (command_error error);
                  Ok ()
              | Ok unsubscribe_transition -> (
                  match apply_transition t unsubscribe_transition with
                  | Error error ->
                      Subscription.complete_drain subscription waiter
                        (Error error);
                      Error error
                  | Ok () -> (
                      Subscription.terminate_for_drain subscription;
                      match Nats.Client.outgoing t.state Nats.Client.Flush with
                      | Error error ->
                          Subscription.complete_drain subscription waiter
                            (Error (command_error error));
                          Ok ()
                      | Ok flush_transition -> (
                          match apply_transition t flush_transition with
                          | Error error ->
                              Subscription.complete_drain subscription waiter
                                (Error error);
                              Error error
                          | Ok () ->
                              Queue.add (Subscription_drain_waiter waiter)
                                t.barriers;
                              Ok ()))))))
  | Cancel_subscription_drain { sid } -> (
      match Hashtbl.find_opt t.subscriptions sid with
      | None -> Ok ()
      | Some subscription ->
          Subscription.cancel_drain subscription;
          Ok ())
  | Request { message; timeout; setup; resolver } -> (
      match Mtime.add_span (now t) timeout with
      | None ->
          resolve_setup t setup (Error Error.Timeout);
          Ok ()
      | Some deadline -> (
          let inbox = fresh_inbox t in
          let filter =
            Nats.Subject.Filter.literal (Nats.Subject.to_string inbox)
          in
          match
            Nats.Client.outgoing t.state
              (Nats.Client.Subscribe { subject = filter; queue_group = None })
          with
          | Error error ->
              resolve_setup t setup (Error (command_error error));
              Ok ()
          | Ok subscribe_transition -> (
              match subscribe_transition.subscription_id with
              | None ->
                  let error =
                    protocol
                      (Nats.Error.Codec
                         (Nats.Codec.Invalid_operation
                            { keyword = "request SUB did not allocate an id" }))
                  in
                  resolve_setup t setup (Error error);
                  Error error
              | Some sid -> (
                  match apply_transition t subscribe_transition with
                  | Error error ->
                      resolve_setup t setup (Error error);
                      Error error
                  | Ok () -> (
                      match
                        Nats.Client.outgoing t.state
                          (Nats.Client.Auto_unsubscribe
                             { sid; max_messages = 1 })
                      with
                      | Error error -> (
                          let error = command_error error in
                          resolve_setup t setup (Error error);
                          match unsubscribe_sid t sid with
                          | Ok () -> Ok ()
                          | Error cleanup_error -> Error cleanup_error)
                      | Ok auto_unsubscribe -> (
                          match apply_transition t auto_unsubscribe with
                          | Error error -> (
                              resolve_setup t setup (Error error);
                              match unsubscribe_sid t sid with
                              | Ok () -> Ok ()
                              | Error cleanup_error -> Error cleanup_error)
                          | Ok () -> (
                              Hashtbl.replace t.requests sid
                                { resolver; deadline };
                              let message =
                                Nats.Message.v
                                  ~subject:(Nats.Message.subject message)
                                  ~reply_to:inbox
                                  ~headers:(Nats.Message.headers message)
                                  (Nats.Message.payload message)
                              in
                              match
                                Nats.Client.outgoing t.state
                                  (Nats.Client.Publish message)
                              with
                              | Error error -> (
                                  Hashtbl.remove t.requests sid;
                                  resolve_setup t setup
                                    (Error (command_error error));
                                  match unsubscribe_sid t sid with
                                  | Ok () -> Ok ()
                                  | Error error -> Error error)
                              | Ok transition -> (
                                  match apply_transition t transition with
                                  | Error error ->
                                      Hashtbl.remove t.requests sid;
                                      resolve_setup t setup (Error error);
                                      Error error
                                  | Ok () ->
                                      resolve_setup t setup (Ok sid);
                                      Ok ()))))))))
  | Cancel_request { sid } -> (
      match Hashtbl.find_opt t.requests sid with
      | None -> Ok ()
      | Some waiter -> (
          Hashtbl.remove t.requests sid;
          fail_waiter waiter.resolver Error.Closed;
          match unsubscribe_sid t sid with
          | Error error -> Error error
          | Ok () -> Ok ()))
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
                  Queue.add
                    (Flush_waiter { resolver; deadline; completed = false })
                    t.barriers;
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
                  fail_requests t Error.Draining;
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

let expire_requests t current =
  let expired =
    Hashtbl.fold
      (fun sid (waiter : request_waiter) acc ->
        if Mtime.compare current waiter.deadline >= 0 then sid :: acc else acc)
      t.requests []
  in
  let rec loop = function
    | [] -> Ok ()
    | sid :: rest -> (
        match Hashtbl.find_opt t.requests sid with
        | None -> loop rest
        | Some waiter -> (
            Hashtbl.remove t.requests sid;
            fail_waiter waiter.resolver Error.Timeout;
            match unsubscribe_sid t sid with
            | Error error -> Error error
            | Ok () -> loop rest))
  in
  loop expired

let expire_subscription_drains t current =
  let expired =
    Hashtbl.fold
      (fun sid (waiter : subscription_drain_waiter) acc ->
        if Mtime.compare current waiter.deadline >= 0 then sid :: acc else acc)
      t.subscription_drains []
  in
  List.iter
    (fun sid ->
      match Hashtbl.find_opt t.subscription_drains sid with
      | None -> ()
      | Some waiter -> (
          match Hashtbl.find_opt t.subscriptions sid with
          | None ->
              Subscription.complete_drain_waiter waiter (Error Error.Timeout)
          | Some subscription ->
              Subscription.fail_drain subscription Error.Timeout))
    expired

let expire_barriers t current =
  let retained = ref [] in
  let connection_timeout = ref false in
  Queue.iter
    (function
      | Flush_waiter waiter
        when (not waiter.completed)
             && Mtime.compare current waiter.deadline >= 0 ->
          waiter.completed <- true;
          fail_waiter waiter.resolver Error.Timeout;
          retained := Flush_waiter waiter :: !retained
      | Drain_waiter waiter when Mtime.compare current waiter.deadline >= 0 ->
          fail_waiter waiter.resolver Error.Timeout;
          connection_timeout := true
      | barrier -> retained := barrier :: !retained)
    t.barriers;
  Queue.clear t.barriers;
  List.iter (fun barrier -> Queue.add barrier t.barriers) (List.rev !retained);
  !connection_timeout

let timer_deadline t =
  let earliest current candidate =
    match current with
    | None -> Some candidate
    | Some current ->
        if Mtime.is_earlier candidate ~than:current then Some candidate
        else Some current
  in
  let request_deadline =
    Hashtbl.fold
      (fun _sid (waiter : request_waiter) deadline ->
        earliest deadline waiter.deadline)
      t.requests None
  in
  let subscription_drain_deadline =
    Hashtbl.fold
      (fun _sid (waiter : subscription_drain_waiter) deadline ->
        earliest deadline waiter.deadline)
      t.subscription_drains None
  in
  let deadline = t.handshake_deadline in
  let deadline =
    match Nats.Client.next_timeout t.state with
    | None -> deadline
    | Some candidate -> earliest deadline candidate
  in
  let deadline =
    match request_deadline with
    | None -> deadline
    | Some candidate -> earliest deadline candidate
  in
  let deadline =
    match subscription_drain_deadline with
    | None -> deadline
    | Some candidate -> earliest deadline candidate
  in
  let barrier_deadline = ref None in
  Queue.iter
    (function
      | Flush_waiter waiter when not waiter.completed ->
          barrier_deadline := earliest !barrier_deadline waiter.deadline
      | Flush_waiter _ -> ()
      | Drain_waiter waiter ->
          barrier_deadline := earliest !barrier_deadline waiter.deadline
      | Subscription_drain_waiter waiter when not waiter.completed ->
          barrier_deadline := earliest !barrier_deadline waiter.deadline
      | Subscription_drain_waiter _ -> ())
    t.barriers;
  match !barrier_deadline with
  | None -> deadline
  | Some candidate -> earliest deadline candidate

let schedule_timer t =
  let start deadline =
    t.timer_generation <- t.timer_generation + 1;
    t.scheduled_deadline <- Some deadline;
    let generation = t.timer_generation in
    Eio.Fiber.fork ~sw:t.sw (fun () ->
        t.clock.sleep_until deadline;
        if not t.closed then Eio.Stream.add t.work (Timer generation))
  in
  if not t.closed then
    match (t.scheduled_deadline, timer_deadline t) with
    | None, None -> ()
    | Some _, None ->
        t.timer_generation <- t.timer_generation + 1;
        t.scheduled_deadline <- None
    | None, Some deadline -> start deadline
    | Some scheduled, Some deadline ->
        if Mtime.is_earlier deadline ~than:scheduled then start deadline

let apply_timer t generation =
  if Int.equal generation t.timer_generation then (
    t.scheduled_deadline <- None;
    let current = now t in
    let handshake_due =
      match (t.handshake_deadline, Nats.Client.phase t.state) with
      | Some deadline, (Nats.Client.Awaiting_info | Nats.Client.Awaiting_connect)
        ->
          Mtime.compare current deadline >= 0
      | _ -> false
    in
    let barrier_due =
      match Queue.peek_opt t.barriers with
      | None -> false
      | Some (Flush_waiter waiter) ->
          (not waiter.completed) && Mtime.compare current waiter.deadline >= 0
      | Some (Drain_waiter waiter) -> Mtime.compare current waiter.deadline >= 0
      | Some (Subscription_drain_waiter _) -> false
    in
    if handshake_due || barrier_due then finish t Error.Timeout
    else (
      expire_subscription_drains t current;
      if expire_barriers t current then finish t Error.Timeout
      else
        match expire_requests t current with
        | Error error -> finish t error
        | Ok () -> (
            let transition = Nats.Client.timer t.state ~now:current in
            match apply_transition t transition with
            | Error error -> finish t error
            | Ok () ->
                if Nats.Client.phase t.state = Nats.Client.Closed then
                  finish t Error.Timeout)))

let rec owner_loop t =
  try
    if not t.closed then
      match Eio.Stream.take t.work with
      | Command command -> (
          (match command with
          | Cancel_request _ | Cancel_subscription_drain _ -> ()
          | _ -> t.pending_commands <- t.pending_commands - 1);
          (match command with
          | Request { setup; _ } -> t.active_request_setup <- Some setup
          | _ -> ());
          match apply_outgoing t command with
          | Ok () ->
              t.active_request_setup <- None;
              schedule_timer t;
              owner_loop t
          | Error error ->
              t.active_request_setup <- None;
              finish t error)
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
  with Eio.Cancel.Cancelled _ ->
    Eio.Cancel.protect (fun () -> finish t Error.Closed)

let create ~sw ~clock ~config flow =
  let handshake_deadline =
    Mtime.add_span (Eio.Time.Mono.now clock) config.Config.handshake_timeout
  in
  let clock =
    {
      now = (fun () -> Eio.Time.Mono.now clock);
      sleep_until = (fun deadline -> Eio.Time.Mono.sleep_until clock deadline);
    }
  in
  let transport = { flow = Flow flow } in
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
      requests = Hashtbl.create 16;
      subscription_drains = Hashtbl.create 16;
      reader = None;
      eof_seen = false;
      closed = false;
      connect_sent = false;
      tls_active = false;
      tls_info_received = false;
      pending_commands = 0;
      active_request_setup = None;
      handshake_deadline;
      timer_generation = 0;
      scheduled_deadline = None;
      ready_promise = ready;
      ready = ready_resolver;
      barriers = Queue.create ();
    }
  in
  Eio.Switch.on_release sw (fun () -> finish connection Error.Closed);
  schedule_timer connection;
  start_reader connection;
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

let cancel_request t sid =
  if t.closed then Ok ()
  else (
    Eio.Stream.add t.work (Command (Cancel_request { sid }));
    Ok ())

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

let request_msg ?timeout t message =
  let timeout = Option.value timeout ~default:t.config.Config.request_timeout in
  match validate_timeout "request" timeout with
  | Error error -> Error error
  | Ok timeout -> (
      let setup, setup_resolver = Eio.Promise.create () in
      let response, response_resolver = Eio.Promise.create () in
      let setup_result =
        Eio.Cancel.protect (fun () ->
            send t
              (Request
                 {
                   message;
                   timeout;
                   setup = setup_resolver;
                   resolver = response_resolver;
                 })
              setup)
      in
      match setup_result with
      | Error error -> Error error
      | Ok sid -> (
          try Eio.Promise.await response
          with Eio.Cancel.Cancelled _ as cancellation ->
            ignore (Eio.Cancel.protect (fun () -> cancel_request t sid));
            raise cancellation))

let request ?timeout ?(headers = Nats.Header.empty) t subject payload =
  request_msg ?timeout t (Nats.Message.v ~subject ~headers payload)

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
