module Config = struct
  type t = {
    core : Nats.Config.t;
    auth : Nats.Auth.t;
    command_capacity : int;
    subscription_capacity : int;
    event_capacity : int;
    read_capacity : int;
    read_chunk_size : int;
    inbox_prefix : Nats.Subject.t;
    max_reconnect_attempts : int option;
    reconnect_buffer_size : int option;
    reconnect_delay : Mtime.Span.t;
    reconnect_max_delay : Mtime.Span.t;
    reconnect_jitter : Mtime.Span.t;
    random : Random.State.t;
    random_lock : Mutex.t;
    tls : Tls.Config.client option;
    tls_required : bool;
    handshake_timeout : Mtime.Span.t;
    request_timeout : Mtime.Span.t;
    flush_timeout : Mtime.Span.t;
    drain_timeout : Mtime.Span.t;
  }

  let default_span = Mtime.Span.(5 * s)
  let default_reconnect_delay = Mtime.Span.(1 * s)
  let default_reconnect_max_delay = Mtime.Span.(30 * s)
  let default_reconnect_buffer_size = 8 * 1024 * 1024

  let validate_capacity name value =
    if value > 0 then Ok value
    else Error (Error.Invalid_capacity { name; value })

  let validate_timeout name value =
    if Mtime.Span.compare value Mtime.Span.zero > 0 then Ok value
    else Error (Error.Invalid_timeout name)

  let validate_reconnect_attempts = function
    | None -> Ok None
    | Some value when value >= 0 -> Ok (Some value)
    | Some value -> Error (Error.Invalid_reconnect_attempts value)

  let validate_reconnect_buffer_size value =
    if Int.equal value 0 then Ok (Some default_reconnect_buffer_size)
    else if Int.equal value (-1) then Ok None
    else if value > 0 then Ok (Some value)
    else Error (Error.Invalid_reconnect_buffer_size value)

  let validate_reconnect_delays initial maximum =
    match validate_timeout "reconnect delay" initial with
    | Error error -> Error error
    | Ok initial -> (
        match validate_timeout "reconnect maximum delay" maximum with
        | Error error -> Error error
        | Ok maximum ->
            if Mtime.Span.compare maximum initial < 0 then
              Error (Error.Invalid_reconnect_delay { initial; maximum })
            else Ok (initial, maximum))

  let v ?(core = Nats.Config.default) ?(auth = Nats.Auth.none)
      ?(command_capacity = 128) ?(subscription_capacity = 256)
      ?(event_capacity = 64) ?(read_capacity = 4) ?(read_chunk_size = 65536)
      ?(max_reconnect_attempts = Some 3)
      ?(reconnect_buffer_size = default_reconnect_buffer_size)
      ?(reconnect_delay = default_reconnect_delay)
      ?(reconnect_max_delay = default_reconnect_max_delay)
      ?(reconnect_jitter = Mtime.Span.zero) ?random ?tls ?(tls_required = false)
      ?(inbox_prefix = "_INBOX.ocaml") ?(handshake_timeout = default_span)
      ?(request_timeout = default_span) ?(flush_timeout = default_span)
      ?(drain_timeout = default_span) () =
    if tls_required && Option.is_none tls then Error Error.Tls_required
    else
      match Nats.Subject.of_string inbox_prefix with
      | Error error -> Error (Error.Invalid_inbox_prefix error)
      | Ok inbox_prefix ->
          let ( let* ) value f =
            match value with Error error -> Error error | Ok value -> f value
          in
          let* command_capacity =
            validate_capacity "command" command_capacity
          in
          let* subscription_capacity =
            validate_capacity "subscription" subscription_capacity
          in
          let* event_capacity = validate_capacity "event" event_capacity in
          let* read_capacity = validate_capacity "read" read_capacity in
          let* () =
            if read_chunk_size <= 0 then
              Error (Error.Invalid_chunk_size read_chunk_size)
            else Ok ()
          in
          let* handshake_timeout =
            validate_timeout "handshake" handshake_timeout
          in
          let* request_timeout = validate_timeout "request" request_timeout in
          let* flush_timeout = validate_timeout "flush" flush_timeout in
          let* drain_timeout = validate_timeout "drain" drain_timeout in
          let* max_reconnect_attempts =
            validate_reconnect_attempts max_reconnect_attempts
          in
          let* reconnect_buffer_size =
            validate_reconnect_buffer_size reconnect_buffer_size
          in
          let* reconnect_delay, reconnect_max_delay =
            validate_reconnect_delays reconnect_delay reconnect_max_delay
          in
          let* () =
            if Mtime.Span.compare reconnect_jitter Mtime.Span.zero < 0 then
              Error (Error.Invalid_reconnect_jitter reconnect_jitter)
            else Ok ()
          in
          let random =
            match random with
            | Some random -> Random.State.copy random
            | None -> Random.State.make_self_init ()
          in
          Ok
            {
              core;
              auth;
              command_capacity;
              subscription_capacity;
              event_capacity;
              read_capacity;
              read_chunk_size;
              inbox_prefix;
              max_reconnect_attempts;
              reconnect_buffer_size;
              reconnect_delay;
              reconnect_max_delay;
              reconnect_jitter;
              random;
              random_lock = Mutex.create ();
              tls;
              tls_required;
              handshake_timeout;
              request_timeout;
              flush_timeout;
              drain_timeout;
            }

  let default =
    match v () with
    | Ok value -> value
    | Error error ->
        invalid_arg
          (Format.asprintf "invalid default Eio config: %a" Error.pp error)

  let random_for_connection config =
    Mutex.lock config.random_lock;
    Fun.protect
      ~finally:(fun () -> Mutex.unlock config.random_lock)
      (fun () -> Random.State.split config.random)
end

module Event_stream = struct
  type item = Event of Event.t | Done of Error.t

  type t = {
    queue : item Eio.Stream.t;
    capacity : int;
    control_capacity : int;
    storage_capacity : int;
    mutable terminal : Error.t option;
    mutable done_seen : bool;
    mutable control_sequence : bool;
    mutable control_hold : bool;
  }

  let create capacity =
    let add_capacity extra value =
      if value >= max_int - extra then max_int else value + extra
    in
    let control_capacity = add_capacity 8 capacity in
    let storage_capacity = add_capacity 10 capacity in
    {
      queue = Eio.Stream.create storage_capacity;
      capacity;
      control_capacity;
      storage_capacity;
      terminal = None;
      done_seen = false;
      control_sequence = false;
      control_hold = false;
    }

  let push t event =
    if Option.is_some t.terminal then false
    else
      let capacity =
        if t.control_sequence then t.control_capacity else t.capacity
      in
      if Eio.Stream.length t.queue >= capacity then false
      else (
        Eio.Stream.add t.queue (Event event);
        true)

  let begin_control_sequence t =
    t.control_sequence <- true;
    t.control_hold <- true

  let end_control_sequence t =
    t.control_hold <- false;
    if Eio.Stream.length t.queue <= t.capacity then t.control_sequence <- false

  let push_control t event =
    if Option.is_some t.terminal then false
    else if Eio.Stream.length t.queue >= t.control_capacity then false
    else (
      Eio.Stream.add t.queue (Event event);
      true)

  let next t =
    if t.done_seen then
      match t.terminal with Some error -> Error error | None -> assert false
    else
      let value = Eio.Stream.take t.queue in
      if
        t.control_sequence && (not t.control_hold)
        && Eio.Stream.length t.queue <= t.capacity
      then t.control_sequence <- false;
      match value with
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
  type item = Message of delivery | Recovery | Done of Error.t
  and delivery = { message : Nats.Message.t; status : Nats.Op.status option }

  type recovery = Detached of int | Attached of int
  type next = Delivery of delivery | Recovery

  type t = {
    sid : int;
    queue : item Eio.Stream.t;
    capacity : int;
    pending_messages_limit : int option;
    pending_bytes_limit : int64 option;
    mutable pending_messages : int;
    mutable pending_bytes : int64;
    mutable terminal : Error.t option;
    mutable done_seen : bool;
    mutable active : bool;
    mutable drain_requested : bool;
    mutable drain_waiter : subscription_drain_waiter option;
    mutable drain_promise : (unit, Error.t) result Eio.Promise.t option;
    mutable drain_resolver : (unit, Error.t) result Eio.Promise.u option;
    mutable drain_result : (unit, Error.t) result option;
    mutable recovery : recovery;
    mutable recovery_queued : bool;
    mutable recovery_signal : unit Eio.Promise.t;
    mutable recovery_signal_u : unit Eio.Promise.u;
    unsubscribe_request : unit -> (unit, Error.t) result;
    auto_unsubscribe_request : max_messages:int -> (unit, Error.t) result;
    replay_on_reconnect : bool;
    drain_request :
      timeout:Mtime.Span.t option ->
      promise:(unit, Error.t) result Eio.Promise.t ->
      resolver:(unit, Error.t) result Eio.Promise.u ->
      (unit, Error.t) result;
    cancel_drain_request : unit -> unit;
    wait : Mtime.Span.t -> (unit, Error.t) result;
  }

  let create ~sid ~capacity ~pending_messages ~pending_bytes
      ~unsubscribe_request ~auto_unsubscribe_request ~replay_on_reconnect
      ~drain_request ~cancel_drain_request ~wait =
    let recovery_signal, recovery_signal_u = Eio.Promise.create () in
    let capacity =
      match pending_messages with
      | None -> capacity
      | Some limit -> Int.min capacity limit
    in
    {
      sid;
      queue =
        Eio.Stream.create
          (if capacity >= max_int - 2 then max_int else capacity + 2);
      capacity;
      pending_messages_limit = pending_messages;
      pending_bytes_limit = Option.map Int64.of_int pending_bytes;
      pending_messages = 0;
      pending_bytes = 0L;
      terminal = None;
      done_seen = false;
      active = true;
      drain_requested = false;
      drain_waiter = None;
      drain_promise = None;
      drain_resolver = None;
      drain_result = None;
      recovery = Attached 0;
      recovery_queued = false;
      recovery_signal;
      recovery_signal_u;
      unsubscribe_request;
      auto_unsubscribe_request;
      replay_on_reconnect;
      drain_request;
      cancel_drain_request;
      wait;
    }

  let sid t = t.sid
  let replay_on_reconnect t = t.replay_on_reconnect
  let recovery t = t.recovery

  let equal_recovery first second =
    match (first, second) with
    | Detached first, Detached second | Attached first, Attached second ->
        Int.equal first second
    | Detached _, Attached _ | Attached _, Detached _ -> false

  let signal_recovery t =
    let signal = t.recovery_signal_u in
    let next_signal, next_signal_u = Eio.Promise.create () in
    t.recovery_signal <- next_signal;
    t.recovery_signal_u <- next_signal_u;
    Eio.Promise.resolve signal ()

  let queue_recovery t =
    if (not t.recovery_queued) && Option.is_none t.terminal then (
      t.recovery_queued <- true;
      Eio.Stream.add t.queue Recovery)

  let set_recovery t recovery =
    if not (equal_recovery t.recovery recovery) then (
      t.recovery <- recovery;
      signal_recovery t)

  let detach t =
    match t.recovery with
    | Attached generation ->
        set_recovery t (Detached generation);
        queue_recovery t
    | Detached _ -> ()

  let next_generation generation =
    if Int.equal generation max_int then max_int else generation + 1

  let attach t =
    match t.recovery with
    | Detached generation ->
        set_recovery t (Attached (next_generation generation))
    | Attached _ -> ()

  let delivery_bytes (delivery : delivery) =
    Int64.of_int (String.length (Nats.Message.payload delivery.message))

  let pending_message_limit_reached t =
    match t.pending_messages_limit with
    | Some limit -> Int.compare t.pending_messages limit >= 0
    | None -> false

  let pending_bytes_limit_reached t delivery =
    match t.pending_bytes_limit with
    | None -> false
    | Some limit ->
        Int64.compare t.pending_bytes
          (Int64.sub limit (delivery_bytes delivery))
        > 0

  let push t (delivery : delivery) =
    if (not t.active) || Option.is_some t.terminal then false
    else if pending_message_limit_reached t then false
    else if pending_bytes_limit_reached t delivery then false
    else if Eio.Stream.length t.queue >= t.capacity then false
    else (
      Eio.Stream.add t.queue (Message delivery);
      t.pending_messages <- t.pending_messages + 1;
      t.pending_bytes <- Int64.add t.pending_bytes (delivery_bytes delivery);
      true)

  let consume_delivery t delivery =
    t.pending_messages <- t.pending_messages - 1;
    t.pending_bytes <- Int64.sub t.pending_bytes (delivery_bytes delivery)

  let complete_drain_waiter waiter result =
    if not waiter.completed then (
      waiter.completed <- true;
      waiter.on_complete result;
      Eio.Promise.resolve waiter.resolver result)

  let clear_drain_state t =
    t.drain_promise <- None;
    t.drain_resolver <- None;
    t.drain_requested <- false

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
      Eio.Stream.add t.queue (Done error);
      signal_recovery t);
    fail_pending_drain t error

  let terminate_after_drain t =
    t.active <- false;
    t.drain_requested <- false;
    if Option.is_none t.terminal then (
      t.terminal <- Some Error.Closed;
      Eio.Stream.add t.queue (Done Error.Closed);
      signal_recovery t)

  let terminal_error t =
    match t.terminal with Some error -> Error error | None -> assert false

  let mark_done t error =
    t.done_seen <- true;
    match t.drain_waiter with
    | None -> ()
    | Some waiter ->
        waiter.done_seen <- true;
        if waiter.server_flushed then complete_drain t waiter (Ok ())

  let rec next t =
    if t.done_seen then terminal_error t
    else
      match Eio.Stream.take t.queue with
      | Message delivery ->
          consume_delivery t delivery;
          Ok delivery
      | Recovery ->
          t.recovery_queued <- false;
          next t
      | Done error ->
          mark_done t error;
          Error error

  let rec next_nonblocking t =
    if t.done_seen then Some (terminal_error t)
    else
      match Eio.Stream.take_nonblocking t.queue with
      | None -> None
      | Some (Message delivery) ->
          consume_delivery t delivery;
          Some (Ok delivery)
      | Some Recovery ->
          t.recovery_queued <- false;
          next_nonblocking t
      | Some (Done error) ->
          mark_done t error;
          Some (Error error)

  let next_or_recovery t =
    if t.done_seen then terminal_error t
    else
      match Eio.Stream.take t.queue with
      | Message delivery ->
          consume_delivery t delivery;
          Ok (Delivery delivery)
      | Recovery ->
          t.recovery_queued <- false;
          Ok Recovery
      | Done error ->
          mark_done t error;
          Error error

  let next_or_recovery_nonblocking t =
    if t.done_seen then Some (terminal_error t)
    else
      match Eio.Stream.take_nonblocking t.queue with
      | None -> None
      | Some (Message delivery) ->
          consume_delivery t delivery;
          Some (Ok (Delivery delivery))
      | Some Recovery ->
          t.recovery_queued <- false;
          Some (Ok Recovery)
      | Some (Done error) ->
          mark_done t error;
          Some (Error error)

  let next_or_recovery_with_timeout ~timeout t =
    if Mtime.Span.compare timeout Mtime.Span.zero <= 0 then
      Error (Error.Invalid_timeout "subscription")
    else
      let prefer first second =
        match (first, second) with
        | (Ok _ as value), _ | _, (Ok _ as value) -> value
        | Error Error.Timeout, other | other, Error Error.Timeout -> other
        | first, _ -> first
      in
      Eio.Fiber.first ~combine:prefer
        (fun () -> next_or_recovery t)
        (fun () ->
          match t.wait timeout with
          | Ok () -> Error Error.Timeout
          | Error error -> Error error)

  let next_with_timeout ~timeout t =
    if Mtime.Span.compare timeout Mtime.Span.zero <= 0 then
      Error (Error.Invalid_timeout "subscription")
    else
      let prefer first second =
        match (first, second) with
        | (Ok _ as value), _ | _, (Ok _ as value) -> value
        | Error Error.Timeout, other | other, Error Error.Timeout -> other
        | first, _ -> first
      in
      Eio.Fiber.first ~combine:prefer
        (fun () -> next t)
        (fun () ->
          match t.wait timeout with
          | Ok () -> Error Error.Timeout
          | Error error -> Error error)

  let await_recovery ?timeout ~from t =
    let rec wait () =
      match t.terminal with
      | Some error -> Error error
      | None -> (
          let current = t.recovery in
          if not (equal_recovery current from) then Ok current
          else
            let signal = t.recovery_signal in
            let wait_signal () =
              Eio.Promise.await signal;
              Ok ()
            in
            let wait_result =
              match timeout with
              | None -> wait_signal ()
              | Some timeout ->
                  if Mtime.Span.compare timeout Mtime.Span.zero <= 0 then
                    Error (Error.Invalid_timeout "subscription recovery")
                  else
                    let choose first second =
                      match (first, second) with
                      | (Ok _ as value), _ | _, (Ok _ as value) -> value
                      | Error Error.Timeout, other | other, Error Error.Timeout
                        ->
                          other
                      | first, _ -> first
                    in
                    Eio.Fiber.first ~combine:choose wait_signal (fun () ->
                        match t.wait timeout with
                        | Ok () -> Error Error.Timeout
                        | Error error -> Error error)
            in
            match wait_result with
            | Ok () -> wait ()
            | Error error -> Error error)
    in
    wait ()

  let unsubscribe t =
    if not t.active then Ok ()
    else if t.drain_requested then Error Error.Draining
    else t.unsubscribe_request ()

  let auto_unsubscribe t ~max_messages =
    if not t.active then Ok ()
    else if t.drain_requested then Error Error.Draining
    else t.auto_unsubscribe_request ~max_messages

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
              t.drain_requested <- true;
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
      replay_on_reconnect : bool;
      pending_messages : int option;
      pending_bytes : int option;
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
  | Resume_subscription_drain of subscription_drain_waiter
  | Cancel_subscription_drain of { sid : int }
  | Request of {
      message : Nats.Message.t;
      timeout : Mtime.Span.t;
      setup : (int, Error.t) result Eio.Promise.u;
      resolver : (Nats.Message.t, Error.t) result Eio.Promise.u;
      cancelled : bool ref;
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

type endpoint = Nats.Endpoint.t
type error = Error.t
type dial = unit -> (flow * endpoint, Error.t) result
type transport = { mutable flow : flow; mutable closed : bool }

type reader = {
  cancel : Eio.Cancel.t;
  done_ : unit Eio.Promise.t;
  finished : bool ref;
  stopped : bool ref;
}

type monotonic_clock = { now : unit -> Mtime.t; sleep_until : Mtime.t -> unit }

type t = {
  sw : Eio.Switch.t;
  dial : dial;
  pool : Nats.Endpoint.Pool.t ref;
  mutable current_endpoint : endpoint option;
  flow : transport;
  clock : monotonic_clock;
  config : Config.t;
  random : Random.State.t;
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
  mutable reconnecting : bool;
  reconnect_pending : string Queue.t;
  mutable reconnect_pending_bytes : int;
  mutable reconnect_publish_state : Nats.Client.t option;
  mutable reconnect_signal : (unit, Error.t) result Eio.Promise.t;
  mutable reconnect_signal_u : (unit, Error.t) result Eio.Promise.u;
  mutable reconnect_attempts : int;
  mutable reconnect_wait : Mtime.Span.t;
  mutable reconnect_deadline : Mtime.t option;
  mutable connect_sent : bool;
  mutable tls_active : bool;
  mutable connect_info_ready : bool;
  mutable pending_commands : int;
  mutable active_request_setup : (int, Error.t) result Eio.Promise.u option;
  mutable handshake_deadline : Mtime.t option;
  mutable timer_generation : int;
  mutable scheduled_deadline : Mtime.t option;
  ready_promise : (unit, Error.t) result Eio.Promise.t;
  ready : (unit, Error.t) result Eio.Promise.u;
  barriers : barrier Queue.t;
}

type connection = t

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

let close_transport (transport : transport) =
  if not transport.closed then (
    transport.closed <- true;
    close_flow transport.flow)

let transport_error t error =
  if t.tls_active then Error (Error.Tls error) else Error (Error.Io error)

let now t = t.clock.now ()
let inbox_counter = Atomic.make 0
let inbox_process_nonce = Random.State.bits (Random.State.make_self_init ())

let fresh_inbox t =
  let sequence = Atomic.fetch_and_add inbox_counter 1 in
  let timestamp = Mtime.to_uint64_ns (now t) in
  Nats.Subject.literal
    (Format.asprintf "%s.%Ld.%d.%d"
       (Nats.Subject.to_string t.config.Config.inbox_prefix)
       timestamp inbox_process_nonce sequence)

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

let resolve_reconnect t value =
  let resolver = t.reconnect_signal_u in
  let signal, signal_resolver = Eio.Promise.create () in
  t.reconnect_signal <- signal;
  t.reconnect_signal_u <- signal_resolver;
  Eio.Promise.resolve resolver value

let await_reconnect ?timeout t =
  if t.closed then Error Error.Closed
  else if not t.reconnecting then Ok ()
  else
    let signal = t.reconnect_signal in
    let wait =
      match timeout with
      | None -> Eio.Promise.await signal
      | Some timeout -> (
          if Mtime.Span.compare timeout Mtime.Span.zero <= 0 then
            Error Error.Timeout
          else
            match Mtime.add_span (now t) timeout with
            | None -> Error Error.Timeout
            | Some deadline ->
                let choose first second =
                  match (first, second) with
                  | (Ok _ as value), _ | _, (Ok _ as value) -> value
                  | Error Error.Timeout, other | other, Error Error.Timeout ->
                      other
                  | first, _ -> first
                in
                Eio.Fiber.first ~combine:choose
                  (fun () -> Eio.Promise.await signal)
                  (fun () ->
                    t.clock.sleep_until deadline;
                    Error Error.Timeout))
    in
    wait

let resolve_setup t resolver value =
  t.active_request_setup <- None;
  resolve_unit resolver value

let fail_active_request_setup t error =
  match t.active_request_setup with
  | None -> ()
  | Some resolver ->
      t.active_request_setup <- None;
      resolve_unit resolver (Error error)

let event t event =
  match event with
  | Event.Core _ when t.reconnecting -> Ok ()
  | _ ->
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

let fail_command error = function
  | Publish { resolver; _ } -> fail_waiter resolver error
  | Subscribe { resolver; _ } -> fail_waiter resolver error
  | Auto_unsubscribe { resolver; _ } -> fail_waiter resolver error
  | Drain_subscription { subscription; _ } ->
      Subscription.fail_pending_drain subscription error
  | Resume_subscription_drain _ -> ()
  | Cancel_subscription_drain _ -> ()
  | Request { setup; _ } -> resolve_unit setup (Error error)
  | Cancel_request _ -> ()
  | Unsubscribe { resolver; _ } -> fail_waiter resolver error
  | Flush { resolver; _ } -> fail_waiter resolver error
  | Drain { resolver; _ } -> fail_waiter resolver error
  | Close { resolver } -> resolve_unit resolver (Ok ())

let clear_reconnect_pending t =
  Queue.clear t.reconnect_pending;
  t.reconnect_pending_bytes <- 0

let fail_pending_commands t =
  let rec loop () =
    match Eio.Stream.take_nonblocking t.work with
    | None -> ()
    | Some (Command command) ->
        fail_command Error.Closed command;
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
    (fun waiter ->
      match Hashtbl.find_opt t.subscriptions waiter.sid with
      | Some subscription -> Subscription.fail_drain subscription error
      | None ->
          Hashtbl.remove t.subscription_drains waiter.sid;
          Subscription.complete_drain_waiter waiter (Error error))
    waiters

let finish t error =
  if not t.closed then (
    let suppress_disconnect = t.reconnecting in
    t.closed <- true;
    t.reconnecting <- false;
    clear_reconnect_pending t;
    t.reconnect_publish_state <- None;
    resolve_reconnect t (Error error);
    Event_stream.end_control_sequence t.events;
    fail_active_request_setup t error;
    (match error with
    | Error.Disconnected | Error.Io _ | Error.Tls _ | Error.Tls_required
    | Error.Tls_unexpected_input | Error.Timeout ->
        if not suppress_disconnect then
          Event_stream.push_terminal t.events Event.Disconnected
    | Error.Slow_consumer kind ->
        Event_stream.push_terminal t.events (Event.Slow_consumer kind)
    | Error.Invalid_endpoints | Error.Invalid_capacity _
    | Error.Invalid_pending_limit _ | Error.Command_queue_full _
    | Error.Invalid_chunk_size _ | Error.Invalid_inbox_prefix _
    | Error.Invalid_reconnect_attempts _ | Error.Invalid_retry_attempts _
    | Error.Invalid_reconnect_buffer_size _ | Error.Invalid_reconnect_delay _
    | Error.Invalid_reconnect_jitter _ | Error.Invalid_timeout _
    | Error.No_responders | Error.Reconnect_buffer_exceeded _ | Error.Auth _ ->
        ()
    | Error.Protocol _ | Error.Connection_reconnecting | Error.Draining
    | Error.Closed ->
        ());
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
    stop_reader t;
    close_transport t.flow)

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
  | Ok transition -> (
      match write_outputs t transition.output with
      | Error error -> Error error
      | Ok () ->
          t.state <- transition.state;
          Ok ())

let unsubscribe_and_forget t sid =
  match unsubscribe_sid t sid with
  | Ok () -> Ok ()
  | Error error ->
      t.state <- Nats.Client.forget_subscription t.state sid;
      Error error

let reconnect_pending_output t =
  if Queue.is_empty t.reconnect_pending then None
  else
    let output = Buffer.create t.reconnect_pending_bytes in
    Queue.iter (Buffer.add_string output) t.reconnect_pending;
    clear_reconnect_pending t;
    Some (Buffer.contents output)

let flush_reconnect_pending t =
  match reconnect_pending_output t with
  | None -> Ok ()
  | Some output -> write_outputs t [ output ]

let push_reconnect_core_events t =
  let events =
    match Nats.Client.info t.state with
    | None -> [ Nats.Event.Connected ]
    | Some info ->
        if Nats.Info.lame_duck_mode info then
          [
            Nats.Event.Info info;
            Nats.Event.Lame_duck_mode;
            Nats.Event.Connected;
          ]
        else [ Nats.Event.Info info; Nats.Event.Connected ]
  in
  let rec loop = function
    | [] -> Ok ()
    | event_value :: rest ->
        if Event_stream.push_control t.events (Event.Core event_value) then
          loop rest
        else Error (Error.Slow_consumer Error.Events)
  in
  loop events

let attach_replayed_subscriptions t =
  Hashtbl.iter
    (fun _ subscription ->
      if Subscription.replay_on_reconnect subscription then
        Subscription.attach subscription)
    t.subscriptions

let core_has_subscription t sid =
  List.exists
    (fun (subscription : Nats.Client.subscription) ->
      Int.equal sid subscription.sid)
    (Nats.Client.subscriptions t.state)

let handle_event t event =
  match event with
  | Nats.Event.Info info ->
      let default_scheme =
        match t.current_endpoint with
        | Some endpoint -> Nats.Endpoint.scheme endpoint
        | None -> if t.tls_active then Nats.Endpoint.Tls else Nats.Endpoint.Nats
      in
      let endpoints =
        List.filter_map
          (fun value ->
            match Nats.Endpoint.of_connect_url ~default_scheme value with
            | Ok endpoint -> Some endpoint
            | Error _ -> None)
          (Nats.Info.connect_urls info)
      in
      t.pool := Nats.Endpoint.Pool.update_discovered !(t.pool) endpoints;
      Ok ()
  | Nats.Event.Connected ->
      t.handshake_deadline <- None;
      if t.reconnecting then
        match flush_reconnect_pending t with
        | Error error -> Error error
        | Ok () -> (
            match push_reconnect_core_events t with
            | Error error -> Error error
            | Ok () ->
                if Event_stream.push_control t.events Event.Reconnected then (
                  attach_replayed_subscriptions t;
                  Hashtbl.iter
                    (fun _sid (waiter : subscription_drain_waiter) ->
                      Eio.Stream.add t.work
                        (Command (Resume_subscription_drain waiter)))
                    t.subscription_drains;
                  Event_stream.end_control_sequence t.events;
                  t.reconnecting <- false;
                  t.reconnect_publish_state <- None;
                  resolve_reconnect t (Ok ());
                  Ok ())
                else Error (Error.Slow_consumer Error.Events))
      else if not t.connect_sent then Ok ()
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
                  if
                    waiter.done_seen || not (core_has_subscription t waiter.sid)
                  then Subscription.complete_drain subscription waiter (Ok ())
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
            match unsubscribe_and_forget t delivery.sid with
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
                      match unsubscribe_and_forget t delivery.sid with
                      | Ok () -> loop rest
                      | Error error -> Error error))))
  in
  loop deliveries

let apply_transition t (transition : Nats.Client.transition) =
  match write_outputs t transition.output with
  | Error error -> Error error
  | Ok () -> (
      t.state <- transition.state;
      if
        t.tls_active
        && List.exists
             (function Nats.Event.Info _ -> true | _ -> false)
             transition.events
      then t.connect_info_ready <- true;
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

let tls_handshake ~clock ~deadline flow config =
  let (Flow flow) = flow in
  try
    Eio.Fiber.first
      (fun () -> Ok (Tls_eio.client_of_flow config flow))
      (fun () ->
        match deadline with
        | None -> Error Error.Timeout
        | Some deadline ->
            clock.sleep_until deadline;
            Error Error.Timeout)
  with
  | Eio.Cancel.Cancelled _ as error -> raise error
  | End_of_file -> (
      match deadline with
      | Some deadline when Mtime.compare (clock.now ()) deadline >= 0 ->
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
            match
              tls_handshake ~clock:t.clock ~deadline:t.handshake_deadline
                t.flow.flow config
            with
            | Error error -> Error error
            | Ok flow ->
                t.flow.flow <- Flow flow;
                t.tls_active <- true;
                (* A server-required upgrade follows the plaintext INFO; that
                   INFO is already sufficient to construct CONNECT. TLS-first
                   connections set this flag when their encrypted INFO arrives. *)
                t.connect_info_ready <- true;
                start_reader t;
                Ok ()))

let rec connect_after_info t =
  if
    Nats.Client.phase t.state = Nats.Client.Awaiting_connect
    && not t.connect_sent
  then
    if
      (tls_required_by_server t || t.config.Config.tls_required)
      && not t.tls_active
    then
      match upgrade_tls t with
      | Error error -> Error error
      | Ok () -> connect_after_info t
    else if t.tls_active && not t.connect_info_ready then Ok ()
    else
      match Nats.Client.info t.state with
      | None -> Error (protocol Nats.Error.Info_not_received)
      | Some info -> (
          match Nats.Auth.connect t.config.Config.auth info with
          | Error error -> Error (Error.Auth error)
          | Ok credentials -> (
              match
                Nats.Client.outgoing t.state
                  (Nats.Client.Connect
                     { credentials; tls_required = t.tls_active })
              with
              | Error error -> Error (protocol error)
              | Ok transition ->
                  t.connect_sent <- true;
                  apply_transition t transition))
  else Ok ()

let consume_pending t length =
  if length > 0 then (
    let value = Buffer.contents t.pending in
    let remaining = String.length value - length in
    Buffer.clear t.pending;
    if remaining > 0 then Buffer.add_substring t.pending value length remaining)

let apply_incoming t =
  let was_established = Eio.Promise.is_resolved t.ready_promise in
  let previous_state = t.state in
  let was_draining =
    match Nats.Client.phase t.state with
    | Nats.Client.Draining -> true
    | _ -> false
  in
  match drain_input t with
  | Error error -> Error error
  | Ok () ->
      let () =
        if t.eof_seen && was_established && not was_draining then
          Event_stream.begin_control_sequence t.events
      in
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
                    if Nats.Client.phase t.state = Nats.Client.Closed then
                      if was_established && not was_draining then (
                        if Option.is_none t.reconnect_publish_state then
                          t.reconnect_publish_state <- Some previous_state;
                        Error Error.Disconnected)
                      else (
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

let fail_barriers t error =
  Queue.iter
    (function
      | Flush_waiter waiter ->
          if not waiter.completed then (
            waiter.completed <- true;
            fail_waiter waiter.resolver error)
      | Drain_waiter waiter -> fail_waiter waiter.resolver error
      | Subscription_drain_waiter waiter -> waiter.server_flushed <- false)
    t.barriers;
  Queue.clear t.barriers

let discard_input t =
  let rec loop () =
    match Eio.Stream.take_nonblocking t.input with
    | None -> ()
    | Some _ -> loop ()
  in
  loop ()

let invalidate_timer t =
  t.timer_generation <- t.timer_generation + 1;
  t.scheduled_deadline <- None

let reset_reconnect_attempt ?(preserve_pending_pings = false) t =
  (match t.current_endpoint with
  | None -> ()
  | Some endpoint -> t.pool := Nats.Endpoint.Pool.failed !(t.pool) endpoint);
  t.current_endpoint <- None;
  stop_reader t;
  discard_input t;
  Buffer.clear t.pending;
  t.eof_seen <- false;
  t.handshake_deadline <- None;
  t.connect_sent <- false;
  t.tls_active <- false;
  t.connect_info_ready <- false;
  t.reconnect_deadline <- None;
  invalidate_timer t;
  close_transport t.flow;
  t.state <- Nats.Client.prepare_reconnect ~preserve_pending_pings t.state

let preserve_reconnect_publish_state t =
  match t.reconnect_publish_state with
  | Some _ -> ()
  | None -> t.reconnect_publish_state <- Some t.state

let reconnect_limit_reached t =
  match t.config.Config.max_reconnect_attempts with
  | None -> false
  | Some limit -> t.reconnect_attempts >= limit

let next_reconnect_wait t =
  let doubled = Mtime.Span.add t.reconnect_wait t.reconnect_wait in
  let doubled =
    if Mtime.Span.compare doubled t.reconnect_wait < 0 then Mtime.Span.max_span
    else doubled
  in
  if Mtime.Span.compare doubled t.config.Config.reconnect_max_delay > 0 then
    t.config.Config.reconnect_max_delay
  else doubled

let clamp_float ~minimum ~maximum value =
  if Float.compare value minimum < 0 then minimum
  else if Float.compare value maximum > 0 then maximum
  else value

let reconnect_wait_with_jitter t wait =
  let jitter = t.config.Config.reconnect_jitter in
  if Mtime.Span.compare jitter Mtime.Span.zero = 0 then wait
  else
    let sample = Random.State.float t.random 1. in
    let offset = ((2. *. sample) -. 1.) *. Mtime.Span.to_float_ns jitter in
    let wait = Mtime.Span.to_float_ns wait +. offset in
    let maximum = Mtime.Span.to_float_ns t.config.Config.reconnect_max_delay in
    match Mtime.Span.of_float_ns (clamp_float ~minimum:0. ~maximum wait) with
    | Some value -> value
    | None -> Mtime.Span.max_span

let schedule_reconnect t error =
  if reconnect_limit_reached t then Error error
  else
    let wait = reconnect_wait_with_jitter t t.reconnect_wait in
    t.reconnect_wait <- next_reconnect_wait t;
    match Mtime.add_span (now t) wait with
    | None -> Error error
    | Some deadline ->
        t.reconnect_deadline <- Some deadline;
        Ok ()

let start_reconnect_attempt t =
  t.reconnect_attempts <- t.reconnect_attempts + 1;
  match t.dial () with
  | Error error -> schedule_reconnect t error
  | Ok (flow, endpoint) ->
      t.pool := Nats.Endpoint.Pool.connected !(t.pool) endpoint;
      t.current_endpoint <- Some endpoint;
      t.tls_active <-
        (match Nats.Endpoint.scheme endpoint with
        | Nats.Endpoint.Nats -> false
        | Nats.Endpoint.Tls -> true);
      t.connect_info_ready <- false;
      t.flow.flow <- flow;
      t.flow.closed <- false;
      t.handshake_deadline <-
        Mtime.add_span (now t) t.config.Config.handshake_timeout;
      start_reader t;
      Ok ()

let retry_reconnect t error =
  reset_reconnect_attempt ~preserve_pending_pings:true t;
  schedule_reconnect t error

let recover_transport t initial_error =
  Hashtbl.iter
    (fun _ subscription ->
      if Subscription.replay_on_reconnect subscription then
        Subscription.detach subscription)
    t.subscriptions;
  let non_reconnecting_sids =
    Hashtbl.fold
      (fun sid subscription acc ->
        if Subscription.replay_on_reconnect subscription then acc
        else sid :: acc)
      t.subscriptions []
  in
  List.iter
    (fun sid ->
      close_subscription t sid Error.Disconnected;
      t.state <- Nats.Client.forget_subscription t.state sid)
    non_reconnecting_sids;
  fail_active_request_setup t Error.Disconnected;
  fail_barriers t Error.Disconnected;
  preserve_reconnect_publish_state t;
  reset_reconnect_attempt t;
  t.reconnect_attempts <- 0;
  t.reconnect_wait <- t.config.Config.reconnect_delay;
  t.reconnect_deadline <- None;
  t.handshake_deadline <- None;
  if reconnect_limit_reached t then Error initial_error
  else (
    Event_stream.begin_control_sequence t.events;
    if not (Event_stream.push_control t.events Event.Disconnected) then (
      Event_stream.end_control_sequence t.events;
      Error (Error.Slow_consumer Error.Events))
    else (
      t.reconnecting <- true;
      start_reconnect_attempt t))

let enqueue_reconnect_output t output =
  Queue.add output t.reconnect_pending;
  t.reconnect_pending_bytes <- t.reconnect_pending_bytes + String.length output

let enqueue_reconnect_publish_output t message =
  match t.reconnect_publish_state with
  | None -> Error Error.Disconnected
  | Some state -> (
      match Nats.Client.outgoing state (Nats.Client.Publish message) with
      | Error error -> Error (command_error error)
      | Ok transition -> (
          match t.config.Config.reconnect_buffer_size with
          | Some limit when Int.compare t.reconnect_pending_bytes limit >= 0 ->
              Error (Error.Reconnect_buffer_exceeded { limit })
          | None | Some _ ->
              List.iter (enqueue_reconnect_output t) transition.output;
              Ok ()))

let enqueue_reconnect_publish t message resolver =
  match enqueue_reconnect_publish_output t message with
  | Ok () ->
      resolve_unit resolver (Ok ());
      Ok ()
  | Error error ->
      fail_waiter resolver error;
      Ok ()

type subscription_drain_setup =
  | Drain_subscription_closed
  | Drain_subscription_timeout
  | Drain_subscription_ready of subscription_drain_waiter

let arm_subscription_drain t ~sid ~timeout ~subscription ~promise ~resolver =
  match Hashtbl.find_opt t.subscriptions sid with
  | None -> Drain_subscription_closed
  | Some subscription -> (
      match Mtime.add_span (now t) timeout with
      | None -> Drain_subscription_timeout
      | Some deadline ->
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
                  Subscription.terminate_after_drain subscription;
                  Subscription.clear_drain_state subscription;
                  t.state <- Nats.Client.forget_subscription t.state sid;
                  Hashtbl.remove t.subscription_drains sid;
                  Hashtbl.remove t.subscriptions sid);
            }
          in
          Subscription.arm_drain subscription waiter;
          Hashtbl.replace t.subscription_drains sid waiter;
          Drain_subscription_ready waiter)

let resume_subscription_drain t (waiter : subscription_drain_waiter) =
  if waiter.completed then Ok ()
  else
    match Hashtbl.find_opt t.subscriptions waiter.sid with
    | None ->
        Subscription.complete_drain_waiter waiter (Error Error.Closed);
        Ok ()
    | Some subscription -> (
        match
          Nats.Client.outgoing t.state
            (Nats.Client.Drain_subscription { sid = waiter.sid })
        with
        | Error (Nats.Error.Unknown_subscription _) ->
            t.state <- Nats.Client.forget_subscription t.state waiter.sid;
            close_subscription t waiter.sid Error.Closed;
            Ok ()
        | Error error ->
            t.state <- Nats.Client.forget_subscription t.state waiter.sid;
            close_subscription t waiter.sid (command_error error);
            Ok ()
        | Ok transition -> (
            (* Go suppresses the UNSUB issued by a drain while reconnecting,
               then flushes the replayed subscription state. Keep the pure
               drain origin and its local state transition, but do not send a
               second server mutation after replay. *)
            let transition = { transition with output = [] } in
            match apply_transition t transition with
            | Error error ->
                t.state <- Nats.Client.forget_subscription t.state waiter.sid;
                Subscription.complete_drain subscription waiter (Error error);
                Error error
            | Ok () ->
                Queue.add (Subscription_drain_waiter waiter) t.barriers;
                Ok ()))

let apply_outgoing t command =
  match command with
  | Resume_subscription_drain waiter -> resume_subscription_drain t waiter
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
  | Subscribe
      {
        subject;
        queue_group;
        replay_on_reconnect;
        pending_messages;
        pending_bytes;
        resolver;
      } -> (
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
              let unsubscribe_request () =
                let result =
                  if t.closed then Error Error.Closed
                  else if t.pending_commands >= t.config.Config.command_capacity
                  then
                    Error
                      (Error.Command_queue_full
                         { capacity = t.config.Config.command_capacity })
                  else
                    let promise, resolver = Eio.Promise.create () in
                    t.pending_commands <- t.pending_commands + 1;
                    Eio.Stream.add t.work
                      (Command (Unsubscribe { sid; resolver }));
                    Eio.Promise.await promise
                in
                match result with
                | Ok () -> Ok ()
                | Error error ->
                    close_subscription t sid error;
                    Error error
              in
              let subscription =
                Subscription.create ~sid
                  ~capacity:t.config.subscription_capacity ~pending_messages
                  ~pending_bytes ~unsubscribe_request ~replay_on_reconnect
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
                  ~wait:(fun timeout ->
                    match Mtime.add_span (now t) timeout with
                    | None -> Error Error.Timeout
                    | Some deadline ->
                        t.clock.sleep_until deadline;
                        Ok ())
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
      match
        arm_subscription_drain t ~sid ~timeout ~subscription ~promise ~resolver
      with
      | Drain_subscription_closed ->
          Subscription.fail_pending_drain subscription Error.Closed;
          Ok ()
      | Drain_subscription_timeout ->
          Subscription.fail_pending_drain subscription Error.Timeout;
          Ok ()
      | Drain_subscription_ready waiter -> (
          match
            Nats.Client.outgoing t.state
              (Nats.Client.Drain_subscription { sid })
          with
          | Error (Nats.Error.Unknown_subscription _) ->
              t.state <- Nats.Client.forget_subscription t.state sid;
              close_subscription t sid Error.Closed;
              Ok ()
          | Error error ->
              t.state <- Nats.Client.forget_subscription t.state sid;
              close_subscription t sid (command_error error);
              Ok ()
          | Ok unsubscribe_transition -> (
              match apply_transition t unsubscribe_transition with
              | Error error ->
                  t.state <- Nats.Client.forget_subscription t.state sid;
                  Subscription.complete_drain subscription waiter (Error error);
                  Error error
              | Ok () ->
                  Queue.add (Subscription_drain_waiter waiter) t.barriers;
                  Ok ())))
  | Cancel_subscription_drain { sid } -> (
      match Hashtbl.find_opt t.subscriptions sid with
      | None -> Ok ()
      | Some subscription ->
          Subscription.cancel_drain subscription;
          Ok ())
  | Request { setup; cancelled; _ } when !cancelled ->
      resolve_setup t setup (Error Error.Closed);
      Ok ()
  | Request { message; timeout; setup; resolver; _ } -> (
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
                          match unsubscribe_and_forget t sid with
                          | Ok () -> Ok ()
                          | Error cleanup_error -> Error cleanup_error)
                      | Ok auto_unsubscribe -> (
                          match apply_transition t auto_unsubscribe with
                          | Error error -> (
                              resolve_setup t setup (Error error);
                              match unsubscribe_and_forget t sid with
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
                              if t.reconnecting then (
                                match
                                  enqueue_reconnect_publish_output t message
                                with
                                | Error error ->
                                    Hashtbl.remove t.requests sid;
                                    t.state <-
                                      Nats.Client.forget_subscription t.state
                                        sid;
                                    resolve_setup t setup (Error error);
                                    Ok ()
                                | Ok () ->
                                    resolve_setup t setup (Ok sid);
                                    Ok ())
                              else
                                match
                                  Nats.Client.outgoing t.state
                                    (Nats.Client.Publish message)
                                with
                                | Error error -> (
                                    Hashtbl.remove t.requests sid;
                                    resolve_setup t setup
                                      (Error (command_error error));
                                    match unsubscribe_and_forget t sid with
                                    | Ok () -> Ok ()
                                    | Error error -> Error error)
                                | Ok transition -> (
                                    match apply_transition t transition with
                                    | Error error ->
                                        Hashtbl.remove t.requests sid;
                                        t.state <-
                                          Nats.Client.forget_subscription
                                            t.state sid;
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
          if t.reconnecting then (
            t.state <- Nats.Client.forget_subscription t.state sid;
            Ok ())
          else
            match unsubscribe_and_forget t sid with
            | Error error -> Error error
            | Ok () -> Ok ()))
  | Unsubscribe { sid; resolver } -> (
      match Nats.Client.outgoing t.state (Nats.Client.Unsubscribe { sid }) with
      | Error error ->
          let error = command_error error in
          close_subscription t sid error;
          fail_waiter resolver error;
          Ok ()
      | Ok transition -> (
          match apply_transition t transition with
          | Error error ->
              close_subscription t sid error;
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
            if t.reconnecting then (
              t.state <- Nats.Client.forget_subscription t.state sid;
              loop rest)
            else
              match unsubscribe_and_forget t sid with
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
    match t.reconnect_deadline with
    | None -> deadline
    | Some candidate -> earliest deadline candidate
  in
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

let may_recover t =
  Eio.Promise.is_resolved t.ready_promise
  && (not t.closed) && (not t.reconnecting)
  && (match t.config.Config.max_reconnect_attempts with
    | None -> true
    | Some attempts -> attempts > 0)
  &&
  match Nats.Client.phase t.state with
  | Nats.Client.Draining -> false
  | _ -> true

let recoverable_transport_error = function
  | Error.Disconnected | Error.Io _ -> true
  | Error.Invalid_endpoints | Error.Invalid_capacity _
  | Error.Invalid_pending_limit _ | Error.Command_queue_full _
  | Error.Invalid_chunk_size _ | Error.Invalid_inbox_prefix _
  | Error.Invalid_reconnect_attempts _ | Error.Invalid_retry_attempts _
  | Error.Invalid_reconnect_buffer_size _ | Error.Invalid_reconnect_delay _
  | Error.Invalid_reconnect_jitter _ | Error.Invalid_timeout _
  | Error.Tls_required | Error.Tls_unexpected_input | Error.Tls _
  | Error.Timeout | Error.Slow_consumer _ | Error.Auth _ | Error.Protocol _
  | Error.No_responders | Error.Reconnect_buffer_exceeded _
  | Error.Connection_reconnecting | Error.Draining | Error.Closed ->
      false

let reconnectable_attempt_error = function
  | Error.Disconnected | Error.Io _ | Error.Tls _ | Error.Timeout | Error.Auth _
    ->
      true
  | Error.Invalid_endpoints | Error.Invalid_capacity _
  | Error.Invalid_pending_limit _ | Error.Command_queue_full _
  | Error.Invalid_chunk_size _ | Error.Invalid_inbox_prefix _
  | Error.Invalid_reconnect_attempts _ | Error.Invalid_retry_attempts _
  | Error.Invalid_reconnect_buffer_size _ | Error.Invalid_reconnect_delay _
  | Error.Invalid_reconnect_jitter _ | Error.Invalid_timeout _
  | Error.Tls_required | Error.Tls_unexpected_input | Error.Slow_consumer _
  | Error.Protocol _ | Error.No_responders | Error.Reconnect_buffer_exceeded _
  | Error.Connection_reconnecting | Error.Draining | Error.Closed ->
      false

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
    let drain_barrier_due =
      match Queue.peek_opt t.barriers with
      | None -> false
      | Some (Drain_waiter waiter) -> Mtime.compare current waiter.deadline >= 0
      | Some (Flush_waiter _) | Some (Subscription_drain_waiter _) -> false
    in
    let reconnect_due =
      match t.reconnect_deadline with
      | Some deadline -> Mtime.compare current deadline >= 0
      | None -> false
    in
    if reconnect_due then (
      t.reconnect_deadline <- None;
      match start_reconnect_attempt t with
      | Ok () -> ()
      | Error error -> finish t error)
    else if handshake_due then
      if t.reconnecting then
        match retry_reconnect t Error.Timeout with
        | Ok () -> ()
        | Error error -> finish t error
      else finish t Error.Timeout
    else if drain_barrier_due then finish t Error.Timeout
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

let apply_reconnect_flush t timeout resolver =
  match Nats.Client.outgoing t.state Nats.Client.Flush with
  | Error error ->
      fail_waiter resolver (command_error error);
      Ok ()
  | Ok transition -> (
      List.iter (enqueue_reconnect_output t) transition.output;
      t.state <- transition.state;
      match Mtime.add_span (now t) timeout with
      | None ->
          fail_waiter resolver Error.Timeout;
          Error Error.Timeout
      | Some deadline ->
          Queue.add
            (Flush_waiter { resolver; deadline; completed = false })
            t.barriers;
          Ok ())

let apply_command t command =
  if not t.reconnecting then
    match command with
    | Resume_subscription_drain waiter -> resume_subscription_drain t waiter
    | _ -> apply_outgoing t command
  else
    match command with
    | Publish { message; resolver } ->
        enqueue_reconnect_publish t message resolver
    | Close { resolver } ->
        resolve_unit resolver (Ok ());
        finish t Error.Closed;
        Ok ()
    | Drain { resolver } ->
        fail_waiter resolver Error.Connection_reconnecting;
        finish t Error.Closed;
        Ok ()
    | Flush { timeout; resolver } -> apply_reconnect_flush t timeout resolver
    | Resume_subscription_drain waiter -> resume_subscription_drain t waiter
    | Drain_subscription { sid; timeout; subscription; promise; resolver } -> (
        match
          arm_subscription_drain t ~sid ~timeout ~subscription ~promise
            ~resolver
        with
        | Drain_subscription_closed ->
            Subscription.fail_pending_drain subscription Error.Closed;
            Ok ()
        | Drain_subscription_timeout ->
            Subscription.fail_pending_drain subscription Error.Timeout;
            Ok ()
        | Drain_subscription_ready _ -> Ok ())
    | ( Subscribe _ | Request _ | Auto_unsubscribe _ | Cancel_request _
      | Unsubscribe _ | Cancel_subscription_drain _ ) as command ->
        apply_outgoing t command

let handle_transport_error t error =
  if t.reconnecting && reconnectable_attempt_error error then
    retry_reconnect t error
  else if may_recover t && recoverable_transport_error error then
    recover_transport t error
  else Error error

let rec owner_loop t =
  try
    if not t.closed then
      match Eio.Stream.take t.work with
      | Command command -> (
          (match command with
          | Cancel_request _ | Cancel_subscription_drain _
          | Resume_subscription_drain _ ->
              ()
          | _ -> t.pending_commands <- t.pending_commands - 1);
          (match command with
          | Request { setup; _ } -> t.active_request_setup <- Some setup
          | _ -> ());
          match apply_command t command with
          | Ok () ->
              t.active_request_setup <- None;
              schedule_timer t;
              owner_loop t
          | Error error -> (
              t.active_request_setup <- None;
              match handle_transport_error t error with
              | Ok () ->
                  schedule_timer t;
                  owner_loop t
              | Error error -> finish t error))
      | Input_ready -> (
          match apply_incoming t with
          | Ok () ->
              schedule_timer t;
              owner_loop t
          | Error error -> (
              match handle_transport_error t error with
              | Ok () ->
                  schedule_timer t;
                  owner_loop t
              | Error error -> finish t error))
      | Timer generation ->
          apply_timer t generation;
          schedule_timer t;
          owner_loop t
  with Eio.Cancel.Cancelled _ ->
    Eio.Cancel.protect (fun () -> finish t Error.Closed)

let make_monotonic_clock clock =
  {
    now = (fun () -> Eio.Time.Mono.now clock);
    sleep_until = (fun deadline -> Eio.Time.Mono.sleep_until clock deadline);
  }

let create ~sw ~clock ~config ~(dial : dial) ~pool ~current_endpoint ~tls_active
    flow =
  let handshake_deadline =
    Mtime.add_span (Eio.Time.Mono.now clock) config.Config.handshake_timeout
  in
  let clock =
    {
      now = (fun () -> Eio.Time.Mono.now clock);
      sleep_until = (fun deadline -> Eio.Time.Mono.sleep_until clock deadline);
    }
  in
  let transport = { flow; closed = false } in
  let input = Eio.Stream.create config.Config.read_capacity in
  let ready, ready_resolver = Eio.Promise.create () in
  let reconnect_signal, reconnect_signal_u = Eio.Promise.create () in
  let connection =
    {
      sw;
      dial;
      pool;
      current_endpoint;
      flow = transport;
      clock;
      config;
      random = Config.random_for_connection config;
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
      reconnecting = false;
      reconnect_pending = Queue.create ();
      reconnect_pending_bytes = 0;
      reconnect_publish_state = None;
      reconnect_signal;
      reconnect_signal_u;
      reconnect_attempts = 0;
      reconnect_wait = config.Config.reconnect_delay;
      reconnect_deadline = None;
      connect_sent = false;
      tls_active;
      connect_info_ready = false;
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

let endpoint_uses_tls endpoint =
  match Nats.Endpoint.scheme endpoint with
  | Nats.Endpoint.Nats -> false
  | Nats.Endpoint.Tls -> true

let resolve_endpoint ~net endpoint =
  try
    Ok
      (Eio.Net.getaddrinfo_stream net
         ~service:(Int.to_string (Nats.Endpoint.port endpoint))
         (Nats.Endpoint.host endpoint))
  with
  | Eio.Cancel.Cancelled _ as error -> raise error
  | End_of_file -> Error Error.Disconnected
  | Eio.Io (_, _) as error -> Error (io_error error)
  | error -> Error (Error.Io error)

let connect_address ~sw ~net address =
  try Ok (Flow (Eio.Net.connect ~sw net address)) with
  | Eio.Cancel.Cancelled _ as error -> raise error
  | End_of_file -> Error Error.Disconnected
  | Eio.Io (_, _) as error -> Error (io_error error)
  | error -> Error (Error.Io error)

let wrap_tls_flow ~clock ~deadline ~config flow =
  match config.Config.tls with
  | None -> Error Error.Tls_required
  | Some tls_config -> (
      match tls_handshake ~clock ~deadline flow tls_config with
      | Ok flow -> Ok (Flow flow)
      | Error error ->
          close_flow flow;
          Error error)

let connect_endpoint ~sw ~net ~clock ~config endpoint =
  let needs_tls = endpoint_uses_tls endpoint in
  if needs_tls && Option.is_none config.Config.tls then Error Error.Tls_required
  else
    let tls_deadline =
      if needs_tls then
        Mtime.add_span (clock.now ()) config.Config.handshake_timeout
      else None
    in
    match resolve_endpoint ~net endpoint with
    | Error error -> Error error
    | Ok addresses -> (
        let result = ref None in
        let last_error = ref None in
        List.iter
          (fun address ->
            match !result with
            | Some _ -> ()
            | None -> (
                match connect_address ~sw ~net address with
                | Error error -> last_error := Some error
                | Ok flow ->
                    if needs_tls then
                      match
                        wrap_tls_flow ~clock ~deadline:tls_deadline ~config flow
                      with
                      | Ok flow -> result := Some flow
                      | Error error -> last_error := Some error
                    else result := Some flow))
          addresses;
        match !result with
        | Some flow -> Ok flow
        | None -> (
            match !last_error with
            | Some error -> Error error
            | None -> Error Error.Disconnected))

let remove_endpoint endpoint endpoints =
  List.filter (fun value -> not (Nats.Endpoint.equal endpoint value)) endpoints

let dial_candidates ~sw ~net ~clock ~config ~on_failure candidates =
  let result = ref None in
  let last_error = ref None in
  let remember_error error =
    match error with
    | Error.Tls_required ->
        if Option.is_none !last_error then last_error := Some error
    | _ -> last_error := Some error
  in
  List.iter
    (fun endpoint ->
      match !result with
      | Some _ -> ()
      | None -> (
          match connect_endpoint ~sw ~net ~clock ~config endpoint with
          | Ok flow -> result := Some (flow, endpoint)
          | Error error ->
              on_failure endpoint error;
              remember_error error))
    candidates;
  match !result with
  | Some value -> Ok value
  | None -> (
      match !last_error with
      | Some error -> Error error
      | None -> Error Error.Invalid_endpoints)

let make_dial ~sw ~net ~clock ~config pool =
 fun () ->
  let on_failure endpoint _error =
    pool := Nats.Endpoint.Pool.failed !pool endpoint
  in
  dial_candidates ~sw ~net ~clock ~config ~on_failure
    (Nats.Endpoint.Pool.candidates !pool)

let make_initial_dial ~sw ~net ~clock ~config pool candidates =
  let remaining = ref candidates in
  let dial () =
    let on_failure endpoint _error =
      remaining := remove_endpoint endpoint !remaining;
      pool := Nats.Endpoint.Pool.failed !pool endpoint
    in
    match dial_candidates ~sw ~net ~clock ~config ~on_failure !remaining with
    | Error error -> Error error
    | Ok (flow, endpoint) ->
        remaining := remove_endpoint endpoint !remaining;
        Ok (flow, endpoint)
  in
  (dial, remaining)

let initial_connect_retryable = function
  | Error.Disconnected | Error.Io _ | Error.Tls _ | Error.Timeout
  | Error.Tls_required | Error.Tls_unexpected_input | Error.Auth _ ->
      true
  | Error.Invalid_endpoints | Error.Invalid_capacity _
  | Error.Invalid_pending_limit _ | Error.Command_queue_full _
  | Error.Invalid_chunk_size _ | Error.Invalid_inbox_prefix _
  | Error.Invalid_reconnect_attempts _ | Error.Invalid_retry_attempts _
  | Error.Invalid_reconnect_buffer_size _ | Error.Invalid_reconnect_delay _
  | Error.Invalid_reconnect_jitter _ | Error.Invalid_timeout _
  | Error.Slow_consumer _ | Error.Protocol _ | Error.No_responders
  | Error.Reconnect_buffer_exceeded _ | Error.Connection_reconnecting
  | Error.Draining | Error.Closed ->
      false

let connect ~sw ~net ~clock ?(config = Config.default) endpoints =
  if Int.equal (List.length endpoints) 0 then Error Error.Invalid_endpoints
  else
    let monotonic_clock = make_monotonic_clock clock in
    let pool = ref (Nats.Endpoint.Pool.v endpoints) in
    let dial = make_dial ~sw ~net ~clock:monotonic_clock ~config pool in
    let initial_dial, remaining =
      make_initial_dial ~sw ~net ~clock:monotonic_clock ~config pool
        (Nats.Endpoint.Pool.candidates !pool)
    in
    let rec establish () =
      match initial_dial () with
      | Error error -> Error error
      | Ok (flow, endpoint) -> (
          pool := Nats.Endpoint.Pool.connected !pool endpoint;
          let connection, ready =
            create ~sw ~clock ~config ~dial ~pool
              ~current_endpoint:(Some endpoint)
              ~tls_active:(endpoint_uses_tls endpoint)
              flow
          in
          match Eio.Promise.await ready with
          | Ok () -> Ok connection
          | Error error
            when initial_connect_retryable error
                 && not (Int.equal (List.length !remaining) 0) ->
              pool := Nats.Endpoint.Pool.failed !pool endpoint;
              establish ()
          | Error error ->
              pool := Nats.Endpoint.Pool.failed !pool endpoint;
              Error error)
    in
    establish ()

let send t command promise =
  if t.closed then Error Error.Closed
  else if
    match command with
    | Close _ -> false
    | _ -> t.pending_commands >= t.config.Config.command_capacity
  then
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

let validate_pending_limit name = function
  | None -> Ok None
  | Some value when Int.equal value (-1) -> Ok None
  | Some value when Int.compare value 0 > 0 -> Ok (Some value)
  | Some value -> Error (Error.Invalid_pending_limit { name; value })

let subscribe t ?queue_group ?(replay_on_reconnect = true) ?pending_messages
    ?pending_bytes subject =
  let ( let* ) result f =
    match result with Ok value -> f value | Error _ as error -> error
  in
  let* pending_messages = validate_pending_limit "messages" pending_messages in
  let* pending_bytes = validate_pending_limit "bytes" pending_bytes in
  let promise, resolver = Eio.Promise.create () in
  let result =
    Eio.Cancel.protect (fun () ->
        send t
          (Subscribe
             {
               subject;
               queue_group;
               replay_on_reconnect;
               pending_messages;
               pending_bytes;
               resolver;
             })
          promise)
  in
  match result with
  | Ok subscription when Eio.Fiber.is_cancelled () ->
      ignore
        (Eio.Cancel.protect (fun () ->
             ignore (Subscription.unsubscribe subscription)));
      Eio.Fiber.check ();
      assert false
  | result -> result

let validate_timeout name timeout =
  if Mtime.Span.compare timeout Mtime.Span.zero > 0 then Ok timeout
  else Error (Error.Invalid_timeout name)

module Request = struct
  type t = {
    connection : connection;
    sid : int;
    response : (Nats.Message.t, Error.t) result Eio.Promise.t;
    cancelled : bool ref;
  }

  let await request = Eio.Promise.await request.response

  let cancel request =
    if !(request.cancelled) then Ok ()
    else (
      request.cancelled := true;
      cancel_request request.connection request.sid)
end

let request_async ?timeout t message =
  let timeout = Option.value timeout ~default:t.config.Config.request_timeout in
  match validate_timeout "request" timeout with
  | Error error -> Error error
  | Ok timeout -> (
      let setup, setup_resolver = Eio.Promise.create () in
      let response, response_resolver = Eio.Promise.create () in
      let sid = ref None in
      let cancelled = ref false in
      let setup_result =
        try
          Eio.Cancel.protect (fun () ->
              let result =
                send t
                  (Request
                     {
                       message;
                       timeout;
                       setup = setup_resolver;
                       resolver = response_resolver;
                       cancelled;
                     })
                  setup
              in
              (match result with
              | Error _ -> ()
              | Ok request_sid -> sid := Some request_sid);
              result)
        with Eio.Cancel.Cancelled _ as cancellation ->
          cancelled := true;
          (match !sid with
          | None -> ()
          | Some sid ->
              ignore (Eio.Cancel.protect (fun () -> cancel_request t sid)));
          raise cancellation
      in
      match setup_result with
      | Error error -> Error error
      | Ok request_sid ->
          sid := Some request_sid;
          Ok { Request.connection = t; sid = request_sid; response; cancelled })

let request_msg ?timeout t message =
  match request_async ?timeout t message with
  | Error error -> Error error
  | Ok request -> (
      try Request.await request
      with Eio.Cancel.Cancelled _ as cancellation ->
        ignore (Eio.Cancel.protect (fun () -> Request.cancel request));
        raise cancellation)

let request_msg_retry ?timeout ~retry_wait ~retry_attempts t message =
  match validate_timeout "request retry" retry_wait with
  | Error error -> Error error
  | Ok _ -> (
      match retry_attempts with
      | Some attempts when Int.compare attempts 0 < 0 ->
          Error (Error.Invalid_retry_attempts attempts)
      | _ -> (
          let deadline =
            match timeout with
            | None -> Ok None
            | Some timeout -> (
                match validate_timeout "request" timeout with
                | Error error -> Error error
                | Ok timeout -> (
                    match Mtime.add_span (now t) timeout with
                    | None -> Error Error.Timeout
                    | Some deadline -> Ok (Some deadline)))
          in
          match deadline with
          | Error error -> Error error
          | Ok deadline ->
              let remaining_timeout () =
                match deadline with
                | None -> Ok None
                | Some deadline ->
                    let now = now t in
                    if Mtime.compare now deadline >= 0 then Error Error.Timeout
                    else Ok (Some (Mtime.span now deadline))
              in
              let wait_before_retry () =
                let current = now t in
                match Mtime.add_span current retry_wait with
                | None -> Error Error.Timeout
                | Some retry_deadline -> (
                    match deadline with
                    | None ->
                        t.clock.sleep_until retry_deadline;
                        Ok ()
                    | Some deadline ->
                        let wait_until =
                          if Mtime.compare retry_deadline deadline < 0 then
                            retry_deadline
                          else deadline
                        in
                        t.clock.sleep_until wait_until;
                        if Mtime.compare (now t) deadline >= 0 then
                          Error Error.Timeout
                        else Ok ())
              in
              let rec loop retries =
                match remaining_timeout () with
                | Error error -> Error error
                | Ok timeout -> (
                    match request_msg ?timeout t message with
                    | Error Error.No_responders as result -> (
                        let should_retry =
                          match retry_attempts with
                          | None -> true
                          | Some attempts -> Int.compare retries attempts < 0
                        in
                        if not should_retry then result
                        else
                          match wait_before_retry () with
                          | Error error -> Error error
                          | Ok () -> loop (retries + 1))
                    | result -> result)
              in
              loop 0))

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
