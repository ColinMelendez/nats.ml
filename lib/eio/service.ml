module Core_error = Error

module Error = struct
  type config =
    | Empty_name
    | Invalid_name_character of { position : int; character : char }
    | Invalid_version of string
    | Duplicate_metadata of string

  type endpoint =
    | Empty_name
    | Invalid_name_character of { position : int; character : char }
    | Duplicate_metadata of string
    | Invalid_pending_limits
    | Invalid_pending_limit of { field : string; value : int }

  type selector =
    | Empty_name
    | Invalid_name_character of { position : int; character : char }

  type t =
    | Connection of Connection.error
    | Invalid_config of config
    | Invalid_endpoint of endpoint
    | Invalid_group_subject of Nats.Subject.error
    | Duplicate_endpoint of string
    | Invalid_headers of Nats.Header.error
    | Invalid_service_error of { code : string; description : string }
    | Invalid_selector of selector
    | Invalid_discovery_subject of Nats.Subject.error
    | Invalid_metadata of string
    | No_reply_subject
    | Already_responded
    | No_response
    | Service_error of { code : string; description : string }
    | Handler_raised
    | Encode of Jsont.Error.t
    | Decode of Jsont.Error.t
    | Unexpected_response_type of { expected : string; actual : string }
    | Unexpected_status of { code : int; description : string }
    | Stopped

  let pp_config ppf (error : config) =
    match error with
    | Invalid_version value ->
        Format.fprintf ppf "invalid service version %S" value
    | Empty_name -> Format.pp_print_string ppf "name is empty"
    | Invalid_name_character { position; character } ->
        Format.fprintf ppf "invalid name character %C at position %d" character
          position
    | Duplicate_metadata name ->
        Format.fprintf ppf "metadata repeats key %S" name

  let pp_endpoint ppf (error : endpoint) =
    match error with
    | Empty_name -> Format.pp_print_string ppf "name is empty"
    | Invalid_name_character { position; character } ->
        Format.fprintf ppf "invalid name character %C at position %d" character
          position
    | Duplicate_metadata name ->
        Format.fprintf ppf "metadata repeats key %S" name
    | Invalid_pending_limits ->
        Format.pp_print_string ppf
          "message and byte pending limits cannot both be zero"
    | Invalid_pending_limit { field; value } ->
        Format.fprintf ppf "invalid %s pending limit %d" field value

  let pp_selector ppf (error : selector) =
    match error with
    | Empty_name -> Format.pp_print_string ppf "name is empty"
    | Invalid_name_character { position; character } ->
        Format.fprintf ppf "invalid name character %C at position %d" character
          position

  let pp ppf = function
    | Connection error ->
        Format.fprintf ppf "connection: %a" Core_error.pp error
    | Invalid_config error ->
        Format.fprintf ppf "invalid service config: %a" pp_config error
    | Invalid_endpoint error ->
        Format.fprintf ppf "invalid service endpoint: %a" pp_endpoint error
    | Invalid_group_subject error ->
        Format.fprintf ppf "invalid service group subject: %a"
          Nats.Subject.pp_error error
    | Duplicate_endpoint name ->
        Format.fprintf ppf "service endpoint %S is already registered" name
    | Invalid_headers error ->
        Format.fprintf ppf "invalid service response headers: %a"
          Nats.Header.pp_error error
    | Invalid_service_error { code; description } ->
        Format.fprintf ppf "invalid service error %S: %S" code description
    | Invalid_selector error ->
        Format.fprintf ppf "invalid discovery selector: %a" pp_selector error
    | Invalid_discovery_subject error ->
        Format.fprintf ppf "invalid discovery response subject: %a"
          Nats.Subject.pp_error error
    | Invalid_metadata name ->
        Format.fprintf ppf "invalid service metadata at %S" name
    | No_reply_subject ->
        Format.pp_print_string ppf "request has no reply subject"
    | Already_responded ->
        Format.pp_print_string ppf "request has already been answered"
    | No_response -> Format.pp_print_string ppf "request received no response"
    | Service_error { code; description } ->
        Format.fprintf ppf "service error %S: %s" code description
    | Handler_raised -> Format.pp_print_string ppf "service handler raised"
    | Encode error ->
        Format.fprintf ppf "service JSON encode: %a" Jsont.Error.pp error
    | Decode error ->
        Format.fprintf ppf "service JSON decode: %a" Jsont.Error.pp error
    | Unexpected_response_type { expected; actual } ->
        Format.fprintf ppf "expected service response type %S, got %S" expected
          actual
    | Unexpected_status { code; description } ->
        Format.fprintf ppf "unexpected service response status %d %s" code
          description
    | Stopped -> Format.pp_print_string ppf "service is stopped"
end

type identifier_error =
  | Identifier_empty
  | Identifier_invalid of { position : int; character : char }

let validate_identifier value =
  let length = String.length value in
  if Int.equal length 0 then Error Identifier_empty
  else
    let invalid = ref None in
    for position = 0 to length - 1 do
      match !invalid with
      | Some _ -> ()
      | None ->
          let character = String.get value position in
          let code = Char.code character in
          let valid =
            (code >= Char.code 'A' && code <= Char.code 'Z')
            || (code >= Char.code 'a' && code <= Char.code 'z')
            || (code >= Char.code '0' && code <= Char.code '9')
            || Char.equal character '-' || Char.equal character '_'
          in
          if not valid then
            invalid := Some (Identifier_invalid { position; character })
    done;
    match !invalid with None -> Ok () | Some error -> Error error

let validate_version value =
  let length = String.length value in
  let index = ref 0 in
  let is_alphanumeric character =
    let code = Char.code character in
    (code >= Char.code 'A' && code <= Char.code 'Z')
    || (code >= Char.code 'a' && code <= Char.code 'z')
    || (code >= Char.code '0' && code <= Char.code '9')
  in
  let consume_numeric ~leading_zeroes =
    let first = !index in
    while
      !index < length
      &&
      let code = Char.code (String.get value !index) in
      code >= Char.code '0' && code <= Char.code '9'
    do
      incr index
    done;
    let count = !index - first in
    count > 0
    && (leading_zeroes || count = 1
       || not (Char.equal (String.get value first) '0'))
  in
  let consume_identifier ~numeric_rules =
    let first = !index in
    while
      !index < length
      && (is_alphanumeric (String.get value !index)
         || Char.equal (String.get value !index) '-')
    do
      incr index
    done;
    let count = !index - first in
    if count = 0 then false
    else if numeric_rules then (
      let numeric = ref true in
      for position = first to !index - 1 do
        if
          not
            (let code = Char.code (String.get value position) in
             code >= Char.code '0' && code <= Char.code '9')
        then numeric := false
      done;
      (not !numeric) || count = 1
      || not (Char.equal (String.get value first) '0'))
    else true
  in
  let consume_identifiers ~numeric_rules =
    if not (consume_identifier ~numeric_rules) then false
    else
      let valid = ref true in
      while !index < length && Char.equal (String.get value !index) '.' do
        incr index;
        if not (consume_identifier ~numeric_rules) then valid := false
      done;
      !valid
  in
  let valid =
    consume_numeric ~leading_zeroes:false
    && !index < length
    && Char.equal (String.get value !index) '.'
  in
  if valid then incr index;
  let valid = valid && consume_numeric ~leading_zeroes:false in
  let valid =
    valid && !index < length && Char.equal (String.get value !index) '.'
  in
  if valid then incr index;
  let valid = valid && consume_numeric ~leading_zeroes:false in
  let valid =
    if (not valid) || !index = length then valid
    else
      let marker = String.get value !index in
      if Char.equal marker '-' then (
        incr index;
        consume_identifiers ~numeric_rules:true)
      else true
  in
  let valid =
    if (not valid) || !index = length then valid
    else if Char.equal (String.get value !index) '+' then (
      incr index;
      consume_identifiers ~numeric_rules:false)
    else false
  in
  valid && Int.equal !index length

let config_name_error : identifier_error -> Error.config = function
  | Identifier_empty -> Error.Empty_name
  | Identifier_invalid { position; character } ->
      Error.Invalid_name_character { position; character }

let endpoint_name_error : identifier_error -> Error.endpoint = function
  | Identifier_empty -> Error.Empty_name
  | Identifier_invalid { position; character } ->
      Error.Invalid_name_character { position; character }

let selector_name_error : identifier_error -> Error.selector = function
  | Identifier_empty -> Error.Empty_name
  | Identifier_invalid { position; character } ->
      Error.Invalid_name_character { position; character }

let duplicate_metadata metadata =
  let seen = ref [] in
  let duplicate = ref None in
  List.iter
    (fun (name, _) ->
      match !duplicate with
      | Some _ -> ()
      | None ->
          if List.exists (String.equal name) !seen then duplicate := Some name
          else seen := name :: !seen)
    metadata;
  !duplicate

module Stats = struct
  type endpoint = {
    name : string;
    subject : Nats.Subject.Filter.t;
    queue : Nats.Queue_group.t option;
    metadata : (string * string) list option;
    data : Jsont.json option;
    num_requests : int64;
    num_errors : int64;
    last_error : string;
    processing_time : int64;
    average_processing_time : int64;
  }

  type t = {
    name : string;
    id : string;
    version : string;
    metadata : (string * string) list;
    started : string;
    endpoints : endpoint list;
  }

  let name value = value.name
  let id value = value.id
  let version value = value.version
  let metadata value = value.metadata
  let started value = value.started
  let endpoints value = value.endpoints
  let endpoint_name (value : endpoint) = value.name
  let endpoint_subject (value : endpoint) = value.subject
  let endpoint_queue (value : endpoint) = value.queue
  let endpoint_metadata (value : endpoint) = value.metadata
  let data (value : endpoint) = value.data
  let num_requests (value : endpoint) = value.num_requests
  let num_errors (value : endpoint) = value.num_errors
  let last_error (value : endpoint) = value.last_error
  let processing_time (value : endpoint) = value.processing_time
  let average_processing_time (value : endpoint) = value.average_processing_time

  let with_data data value = { value with data }
  let with_endpoints endpoints value = { value with endpoints }
end

module Config = struct
  type queue_policy = Default | Queue of Nats.Queue_group.t | Disabled

  type t = {
    name : string;
    version : string;
    description : string option;
    metadata : (string * string) list;
    queue : queue_policy;
    stats_handler : (Stats.endpoint -> Jsont.json option) option;
    error_handler : (Error.t -> unit) option;
    done_handler : (unit -> unit) option;
  }

  let v ~name ~version ?description ?(metadata = []) ?(queue = Default)
      ?stats_handler ?error_handler ?done_handler () =
    match validate_identifier name with
    | Error error -> Error (Error.Invalid_config (config_name_error error))
    | Ok () -> (
        if not (validate_version version) then
          Error (Error.Invalid_config (Error.Invalid_version version))
        else
          match duplicate_metadata metadata with
          | Some name ->
              Error (Error.Invalid_config (Error.Duplicate_metadata name))
          | None ->
              Ok
                {
                  name;
                  version;
                  description;
                  metadata;
                  queue;
                  stats_handler;
                  error_handler;
                  done_handler;
                })

  let name value = value.name
  let version value = value.version
  let description value = value.description
  let metadata value = value.metadata
  let queue value = value.queue
  let stats_handler value = value.stats_handler
  let error_handler value = value.error_handler
  let done_handler value = value.done_handler
end

module Request = struct
  type response_state = Available | Sending | Responded | Aborted

  type t = {
    connection : Connection.t;
    message : Nats.Message.t;
    mutex : Eio.Mutex.t;
    mutable response_state : response_state;
    mutable last_error : Error.t option;
    mutable service_error : (string * string) option;
  }

  let make connection message =
    {
      connection;
      message;
      mutex = Eio.Mutex.create ();
      response_state = Available;
      last_error = None;
      service_error = None;
    }

  let subject value = Nats.Message.subject value.message
  let reply value = Nats.Message.reply_to value.message
  let headers value = Nats.Message.headers value.message
  let payload value = Nats.Message.payload value.message

  let fail value error =
    Eio.Mutex.use_rw ~protect:true value.mutex (fun () ->
        value.last_error <- Some error);
    Error error

  let begin_response value =
    Eio.Mutex.use_rw ~protect:true value.mutex (fun () ->
        match value.response_state with
        | Available ->
            value.response_state <- Sending;
            Ok ()
        | Sending | Responded | Aborted -> Error Error.Already_responded)

  let finish_response value result =
    Eio.Mutex.use_rw ~protect:true value.mutex (fun () ->
        match value.response_state with
        | Sending -> (
            match result with
            | Ok () -> value.response_state <- Responded
            | Error error ->
                value.response_state <- Available;
                value.last_error <- Some error)
        | Available | Responded | Aborted -> ());
    result

  let abort_response value error =
    Eio.Mutex.use_rw ~protect:true value.mutex (fun () ->
        match value.response_state with
        | Sending ->
            value.response_state <- Aborted;
            value.last_error <- Some error
        | Available | Responded | Aborted -> ())

  let respond ?(headers = Nats.Header.empty) value payload =
    match begin_response value with
    | Error error -> Error error
    | Ok () -> (
        try
          let result =
            match reply value with
            | None -> Error Error.No_reply_subject
            | Some reply -> (
                let message = Nats.Message.v ~subject:reply ~headers payload in
                match Connection.publish_msg value.connection message with
                | Ok () -> Ok ()
                | Error error -> Error (Error.Connection error))
          in
          finish_response value result
        with Eio.Cancel.Cancelled _ as cancellation ->
          Eio.Cancel.protect (fun () ->
              abort_response value (Error.Connection Core_error.Closed));
          raise cancellation)

  let respond_error ~code ~description ?(headers = Nats.Header.empty)
      ?(payload = "") value =
    if
      Int.equal (String.length code) 0
      || Int.equal (String.length description) 0
    then fail value (Error.Invalid_service_error { code; description })
    else
      match
        Nats.Header.add ~name:"Nats-Service-Error" ~value:description headers
      with
      | Error error -> fail value (Error.Invalid_headers error)
      | Ok headers -> (
          match
            Nats.Header.add ~name:"Nats-Service-Error-Code" ~value:code headers
          with
          | Error error -> fail value (Error.Invalid_headers error)
          | Ok headers -> (
              match respond ~headers value payload with
              | Error error -> Error error
              | Ok () ->
                  Eio.Mutex.use_rw ~protect:true value.mutex (fun () ->
                      value.service_error <- Some (code, description));
                  Ok ()))

  let responded value =
    Eio.Mutex.use_ro value.mutex (fun () ->
        match value.response_state with
        | Available -> false
        | Sending | Responded | Aborted -> true)

  let last_error value =
    Eio.Mutex.use_ro value.mutex (fun () -> value.last_error)

  let service_error value =
    Eio.Mutex.use_ro value.mutex (fun () -> value.service_error)
end

module Endpoint = struct
  module Pending_limits = struct
    type t = { messages : int; bytes : int }

    let valid value = Int.compare value 0 > 0 || Int.equal value (-1)

    let v ~messages ~bytes =
      if Int.equal messages 0 && Int.equal bytes 0 then
        Error (Error.Invalid_endpoint Error.Invalid_pending_limits)
      else if not (valid messages) then
        Error
          (Error.Invalid_endpoint
             (Error.Invalid_pending_limit
                { field = "messages"; value = messages }))
      else if not (valid bytes) then
        Error
          (Error.Invalid_endpoint
             (Error.Invalid_pending_limit { field = "bytes"; value = bytes }))
      else Ok { messages; bytes }

    let messages value = value.messages
    let bytes value = value.bytes
  end

  type t = {
    name : string;
    subject : Nats.Subject.Filter.t;
    metadata : (string * string) list option;
    queue : Config.queue_policy;
    pending_limits : Pending_limits.t option;
    handler : Request.t -> (unit, Error.t) result;
  }

  type handler = Request.t -> (unit, Error.t) result

  let v ~name ?subject ?metadata ?(queue = Config.Default) ?pending_limits
      handler =
    match validate_identifier name with
    | Error error -> Error (Error.Invalid_endpoint (endpoint_name_error error))
    | Ok () -> (
        match metadata with
        | Some metadata -> (
            match duplicate_metadata metadata with
            | Some name ->
                Error (Error.Invalid_endpoint (Error.Duplicate_metadata name))
            | None ->
                Ok
                  {
                    name;
                    subject =
                      Option.value
                        ~default:(Nats.Subject.Filter.literal name)
                        subject;
                    metadata = Some metadata;
                    queue;
                    pending_limits;
                    handler;
                  })
        | None ->
            Ok
              {
                name;
                subject =
                  Option.value
                    ~default:(Nats.Subject.Filter.literal name)
                    subject;
                metadata = None;
                queue;
                pending_limits;
                handler;
              })

  let name (value : t) = value.name
  let subject (value : t) = value.subject
  let metadata value = value.metadata
  let queue value = value.queue
  let pending_limits value = value.pending_limits
end

module Info = struct
  type endpoint = {
    name : string;
    subject : Nats.Subject.Filter.t;
    queue : Nats.Queue_group.t option;
    metadata : (string * string) list option;
  }

  type t = {
    name : string;
    id : string;
    version : string;
    description : string option;
    metadata : (string * string) list;
    endpoints : endpoint list;
  }

  let name value = value.name
  let id value = value.id
  let version value = value.version
  let description value = value.description
  let metadata value = value.metadata
  let endpoints value = value.endpoints
  let endpoint_name (value : endpoint) = value.name
  let endpoint_subject (value : endpoint) = value.subject
  let endpoint_queue (value : endpoint) = value.queue
  let endpoint_metadata (value : endpoint) = value.metadata
end

type service_state = Open | Stopping | Stopped | Failed of Error.t
type control_kind = Ping | Info | Stats

type callback_event =
  | Error_event of Error.t
  | Done_event of unit Eio.Promise.u
  | Failure_event

type callback_dispatch = { events : callback_event Eio.Stream.t }

type owned_subscription = {
  subscription : Connection.Subscription.t;
  done_promise : unit Eio.Promise.t;
  done_resolver : unit Eio.Promise.u;
}

type control = { kind : control_kind; worker : owned_subscription }

type service = {
  sw : Eio.Switch.t;
  connection : Connection.t;
  config : Config.t;
  name : string;
  id : string;
  mutable started : string;
  wall_now : unit -> float;
  mutex : Eio.Mutex.t;
  mutable state : service_state;
  controls : control list;
  mutable endpoints : endpoint_instance list;
  mutable pending_endpoint_names : string list;
  mutable stop_promise : (unit, Error.t) result Eio.Promise.t option;
  mutable hook : Eio.Switch.hook option;
  callbacks : callback_dispatch option;
}

and endpoint_instance = {
  service : service;
  key : string;
  name : string;
  subject : Nats.Subject.Filter.t;
  queue : Nats.Queue_group.t option;
  metadata : (string * string) list option;
  handler : Endpoint.handler;
  worker : owned_subscription;
  mutable num_requests : int64;
  mutable num_errors : int64;
  mutable last_error : string;
  mutable processing_time : int64;
}

type group = {
  service : service;
  name : string;
  subject : Nats.Subject.t;
  queue : Config.queue_policy;
  parent : group option;
}

type t = service

let default_queue = Nats.Queue_group.literal "q"

let queue_policy = function
  | Config.Default -> Some default_queue
  | Config.Queue queue -> Some queue
  | Config.Disabled -> None

let rec effective_group_policy (group : group) =
  match group.queue with
  | Config.Default -> (
      match group.parent with
      | Some parent -> effective_group_policy parent
      | None -> Config.queue group.service.config)
  | policy -> policy

let effective_endpoint_queue parent endpoint =
  let policy =
    match Endpoint.queue endpoint with
    | Config.Default -> parent
    | value -> value
  in
  queue_policy policy

let endpoint_full_name prefix endpoint_name =
  match prefix with
  | None -> endpoint_name
  | Some prefix -> prefix ^ "." ^ endpoint_name

let append_filter prefix subject =
  match prefix with
  | None -> Ok subject
  | Some prefix ->
      Nats.Subject.Filter.of_string
        (prefix ^ "." ^ Nats.Subject.Filter.to_string subject)

let metadata_json metadata =
  Jsont.Json.object'
    (List.map
       (fun (name, value) ->
         Jsont.Json.mem (Jsont.Json.name name) (Jsont.Json.string value))
       metadata)

let metadata_or_null = function
  | None -> Jsont.Json.null ()
  | Some metadata -> metadata_json metadata

let string_of_error error = Format.asprintf "%a" Error.pp error

let info_endpoint (endpoint : endpoint_instance) : Info.endpoint =
  {
    Info.name = endpoint.name;
    subject = endpoint.subject;
    queue = endpoint.queue;
    metadata = endpoint.metadata;
  }

let stats_endpoint (endpoint : endpoint_instance) : Stats.endpoint =
  {
    Stats.name = endpoint.name;
    subject = endpoint.subject;
    queue = endpoint.queue;
    metadata = endpoint.metadata;
    data = None;
    num_requests = endpoint.num_requests;
    num_errors = endpoint.num_errors;
    last_error = endpoint.last_error;
    processing_time = endpoint.processing_time;
    average_processing_time =
      (if Int64.equal endpoint.num_requests 0L then 0L
       else Int64.div endpoint.processing_time endpoint.num_requests);
  }

let info_without_lock (service : service) =
  {
    Info.name = service.name;
    id = service.id;
    version = Config.version service.config;
    description = Config.description service.config;
    metadata = Config.metadata service.config;
    endpoints = List.rev_map info_endpoint service.endpoints;
  }

let stats_without_lock (service : service) =
  {
    Stats.name = service.name;
    id = service.id;
    version = Config.version service.config;
    metadata = Config.metadata service.config;
    started = service.started;
    endpoints = List.rev_map stats_endpoint service.endpoints;
  }

let stats_with_handler (service : service) (value : Stats.t) =
  match Config.stats_handler service.config with
  | None -> value
  | Some handler ->
      Stats.with_endpoints
        (List.map
           (fun endpoint ->
             Stats.with_data (handler endpoint) endpoint)
           (Stats.endpoints value))
        value

let info service =
  Eio.Mutex.use_ro service.mutex (fun () -> info_without_lock service)

let stats service =
  let value = Eio.Mutex.use_ro service.mutex (fun () -> stats_without_lock service) in
  stats_with_handler service value

type wire_identity = {
  type_ : string;
  name : string;
  id : string;
  version : string;
  metadata : Jsont.json;
}

let wire_identity_codec =
  Jsont.Object.map ~kind:"NATS service identity"
    (fun type_ name id version metadata ->
      { type_; name; id; version; metadata })
  |> Jsont.Object.mem "type" Jsont.string ~enc:(fun value -> value.type_)
  |> Jsont.Object.mem "name" Jsont.string ~enc:(fun value -> value.name)
  |> Jsont.Object.mem "id" Jsont.string ~enc:(fun value -> value.id)
  |> Jsont.Object.mem "version" Jsont.string ~enc:(fun value -> value.version)
  |> Jsont.Object.mem "metadata" Jsont.json ~enc:(fun value -> value.metadata)
  |> Jsont.Object.finish

type wire_endpoint_info = {
  name : string;
  subject : string;
  queue_group : string;
  metadata : Jsont.json;
}

let wire_endpoint_info_codec =
  Jsont.Object.map ~kind:"NATS service endpoint"
    (fun name subject queue_group metadata ->
      { name; subject; queue_group; metadata })
  |> Jsont.Object.mem "name" Jsont.string ~enc:(fun value -> value.name)
  |> Jsont.Object.mem "subject" Jsont.string ~enc:(fun value -> value.subject)
  |> Jsont.Object.mem "queue_group" Jsont.string ~enc:(fun value ->
      value.queue_group)
  |> Jsont.Object.mem "metadata" Jsont.json ~enc:(fun value -> value.metadata)
  |> Jsont.Object.finish

type wire_info = {
  type_ : string;
  name : string;
  id : string;
  version : string;
  description : string;
  metadata : Jsont.json;
  endpoints : wire_endpoint_info list;
}

let wire_info_codec =
  Jsont.Object.map ~kind:"NATS service information"
    (fun type_ name id version description metadata endpoints ->
      { type_; name; id; version; description; metadata; endpoints })
  |> Jsont.Object.mem "type" Jsont.string ~enc:(fun value -> value.type_)
  |> Jsont.Object.mem "name" Jsont.string ~enc:(fun value -> value.name)
  |> Jsont.Object.mem "id" Jsont.string ~enc:(fun value -> value.id)
  |> Jsont.Object.mem "version" Jsont.string ~enc:(fun value -> value.version)
  |> Jsont.Object.mem "description" Jsont.string ~enc:(fun value ->
      value.description)
  |> Jsont.Object.mem "metadata" Jsont.json ~enc:(fun value -> value.metadata)
  |> Jsont.Object.mem "endpoints" (Jsont.list wire_endpoint_info_codec)
       ~enc:(fun value -> value.endpoints)
  |> Jsont.Object.finish

type wire_endpoint_stats = {
  name : string;
  subject : string;
  queue_group : string;
  metadata : Jsont.json;
  num_requests : int64;
  num_errors : int64;
  last_error : string;
  processing_time : int64;
  average_processing_time : int64;
  data : Jsont.json option;
}

let wire_endpoint_stats_codec =
  Jsont.Object.map ~kind:"NATS service endpoint statistics"
    (fun
      name
      subject
      queue_group
      metadata
      num_requests
      num_errors
      last_error
      processing_time
      average_processing_time
      data
    ->
      {
        name;
        subject;
        queue_group;
        metadata;
        num_requests;
        num_errors;
        last_error;
        processing_time;
        average_processing_time;
        data;
      })
  |> Jsont.Object.mem "name" Jsont.string ~enc:(fun value -> value.name)
  |> Jsont.Object.mem "subject" Jsont.string ~enc:(fun value -> value.subject)
  |> Jsont.Object.mem "queue_group" Jsont.string ~enc:(fun value ->
      value.queue_group)
  |> Jsont.Object.mem "metadata" Jsont.json ~dec_absent:(Jsont.Json.null ())
       ~enc:(fun value -> value.metadata)
  |> Jsont.Object.mem "num_requests" Jsont.int64 ~enc:(fun value ->
      value.num_requests)
  |> Jsont.Object.mem "num_errors" Jsont.int64 ~enc:(fun value ->
      value.num_errors)
  |> Jsont.Object.mem "last_error" Jsont.string ~enc:(fun value ->
      value.last_error)
  |> Jsont.Object.mem "processing_time" Jsont.int64 ~enc:(fun value ->
      value.processing_time)
  |> Jsont.Object.mem "average_processing_time" Jsont.int64 ~enc:(fun value ->
      value.average_processing_time)
  |> Jsont.Object.opt_mem "data" Jsont.json ~enc:(fun value -> value.data)
  |> Jsont.Object.finish

type wire_stats = {
  type_ : string;
  name : string;
  id : string;
  version : string;
  metadata : Jsont.json;
  started : string;
  endpoints : wire_endpoint_stats list;
}

let wire_stats_codec =
  Jsont.Object.map ~kind:"NATS service statistics"
    (fun type_ name id version metadata started endpoints ->
      { type_; name; id; version; metadata; started; endpoints })
  |> Jsont.Object.mem "type" Jsont.string ~enc:(fun value -> value.type_)
  |> Jsont.Object.mem "name" Jsont.string ~enc:(fun value -> value.name)
  |> Jsont.Object.mem "id" Jsont.string ~enc:(fun value -> value.id)
  |> Jsont.Object.mem "version" Jsont.string ~enc:(fun value -> value.version)
  |> Jsont.Object.mem "metadata" Jsont.json ~enc:(fun value -> value.metadata)
  |> Jsont.Object.mem "started" Jsont.string ~enc:(fun value -> value.started)
  |> Jsont.Object.mem "endpoints" (Jsont.list wire_endpoint_stats_codec)
       ~enc:(fun value -> value.endpoints)
  |> Jsont.Object.finish

let decode codec payload =
  match Jsont_bytesrw.decode_string' codec payload with
  | Ok value -> Ok value
  | Error error -> Error (Error.Decode error)

let expect_response_type ~expected actual =
  if String.equal expected actual then Ok ()
  else Error (Error.Unexpected_response_type { expected; actual })

let metadata_values ~allow_null value =
  match value with
  | Jsont.Null _ when allow_null -> Ok None
  | Jsont.Object (members, _) -> (
      let values = ref [] in
      let invalid = ref None in
      List.iter
        (fun ((name, _), member) ->
          match !invalid with
          | Some _ -> ()
          | None -> (
              match member with
              | Jsont.String (value, _) ->
                  if
                    List.exists
                      (fun (existing, _) -> String.equal existing name)
                      !values
                  then invalid := Some (Error.Invalid_metadata name)
                  else values := (name, value) :: !values
              | _ -> invalid := Some (Error.Invalid_metadata name)))
        members;
      match !invalid with
      | Some error -> Error error
      | None -> Ok (Some (List.rev !values)))
  | Jsont.Null _ -> Error (Error.Invalid_metadata "metadata")
  | _ -> Error (Error.Invalid_metadata "metadata")

let required_metadata value =
  match metadata_values ~allow_null:false value with
  | Error error -> Error error
  | Ok (Some metadata) -> Ok metadata
  | Ok None -> Error (Error.Invalid_metadata "metadata")

let discovery_filter value =
  match Nats.Subject.Filter.of_string value with
  | Ok subject -> Ok subject
  | Error error -> Error (Error.Invalid_discovery_subject error)

let discovery_queue_group value =
  if Int.equal (String.length value) 0 then Ok None
  else
    match Nats.Queue_group.of_string value with
    | Ok queue -> Ok (Some queue)
    | Error error -> Error (Error.Invalid_discovery_subject error)

let discovery_info_endpoint (wire : wire_endpoint_info) =
  match discovery_filter wire.subject with
  | Error error -> Error error
  | Ok subject -> (
      match discovery_queue_group wire.queue_group with
      | Error error -> Error error
      | Ok queue -> (
          match metadata_values ~allow_null:true wire.metadata with
          | Error error -> Error error
          | Ok metadata ->
              Ok { Info.name = wire.name; subject; queue; metadata }))

let discovery_stats_endpoint (wire : wire_endpoint_stats) =
  match discovery_filter wire.subject with
  | Error error -> Error error
  | Ok subject -> (
      match discovery_queue_group wire.queue_group with
      | Error error -> Error error
      | Ok queue -> (
          match metadata_values ~allow_null:true wire.metadata with
          | Error error -> Error error
          | Ok metadata ->
              Ok
                {
                  Stats.name = wire.name;
                  subject;
                  queue;
                  metadata;
                  num_requests = wire.num_requests;
                  num_errors = wire.num_errors;
                  last_error = wire.last_error;
                  processing_time = wire.processing_time;
                  average_processing_time = wire.average_processing_time;
                  data = wire.data;
                }))

let discovery_info_of_wire (wire : wire_info) =
  match
    expect_response_type ~expected:"io.nats.micro.v1.info_response" wire.type_
  with
  | Error error -> Error error
  | Ok () -> (
      match required_metadata wire.metadata with
      | Error error -> Error error
      | Ok metadata -> (
          let endpoints = ref [] in
          let error = ref None in
          List.iter
            (fun endpoint ->
              match !error with
              | Some _ -> ()
              | None -> (
                  match discovery_info_endpoint endpoint with
                  | Ok endpoint -> endpoints := endpoint :: !endpoints
                  | Error endpoint_error -> error := Some endpoint_error))
            wire.endpoints;
          match !error with
          | Some error -> Error error
          | None ->
              Ok
                {
                  Info.name = wire.name;
                  id = wire.id;
                  version = wire.version;
                  description =
                    (if Int.equal (String.length wire.description) 0 then None
                     else Some wire.description);
                  metadata;
                  endpoints = List.rev !endpoints;
                }))

let discovery_stats_of_wire (wire : wire_stats) =
  match
    expect_response_type ~expected:"io.nats.micro.v1.stats_response" wire.type_
  with
  | Error error -> Error error
  | Ok () -> (
      match required_metadata wire.metadata with
      | Error error -> Error error
      | Ok metadata -> (
          let endpoints = ref [] in
          let error = ref None in
          List.iter
            (fun endpoint ->
              match !error with
              | Some _ -> ()
              | None -> (
                  match discovery_stats_endpoint endpoint with
                  | Ok endpoint -> endpoints := endpoint :: !endpoints
                  | Error endpoint_error -> error := Some endpoint_error))
            wire.endpoints;
          match !error with
          | Some error -> Error error
          | None ->
              Ok
                {
                  Stats.name = wire.name;
                  id = wire.id;
                  version = wire.version;
                  metadata;
                  started = wire.started;
                  endpoints = List.rev !endpoints;
                }))

module Discovery = struct
  type target =
    | All
    | Named of string
    | Instance of { service : string; id : string }

  module Ping = struct
    type t = {
      name : string;
      id : string;
      version : string;
      metadata : (string * string) list;
    }

    let name value = value.name
    let id value = value.id
    let version value = value.version
    let metadata value = value.metadata
  end

  let default_timeout = Mtime.Span.(1 * s)

  let validate_target = function
    | All -> Ok ()
    | Named name -> (
        match validate_identifier name with
        | Ok () -> Ok ()
        | Error error ->
            Error (Error.Invalid_selector (selector_name_error error)))
    | Instance { service; id } -> (
        match validate_identifier service with
        | Error error ->
            Error (Error.Invalid_selector (selector_name_error error))
        | Ok () -> (
            match validate_identifier id with
            | Ok () -> Ok ()
            | Error error ->
                Error (Error.Invalid_selector (selector_name_error error))))

  let subject ~verb = function
    | All -> Nats.Subject.literal ("$SRV." ^ verb)
    | Named name -> Nats.Subject.literal ("$SRV." ^ verb ^ "." ^ name)
    | Instance { service; id } ->
        Nats.Subject.literal ("$SRV." ^ verb ^ "." ^ service ^ "." ^ id)

  let collect ~timeout ~target connection ~verb decode_response =
    match validate_target target with
    | Error error -> Error error
    | Ok () -> (
        if Mtime.Span.compare timeout Mtime.Span.zero <= 0 then
          Error (Error.Connection (Core_error.Invalid_timeout "discovery"))
        else
          let inbox = Connection.fresh_inbox connection in
          let filter =
            Nats.Subject.Filter.literal (Nats.Subject.to_string inbox)
          in
          match
            Connection.subscribe connection ~replay_on_reconnect:false filter
          with
          | Error error -> Error (Error.Connection error)
          | Ok subscription -> (
              let finish result =
                Eio.Cancel.protect (fun () ->
                    match Connection.Subscription.unsubscribe subscription with
                    | Ok () -> result
                    | Error error -> (
                        match result with
                        | Ok _ -> Error (Error.Connection error)
                        | Error _ -> result))
              in
              let collect_responses () =
                let subject = subject ~verb target in
                let request = Nats.Message.v ~subject ~reply_to:inbox "" in
                match Connection.publish_msg connection request with
                | Error error -> Error (Error.Connection error)
                | Ok () ->
                    let deadline =
                      match
                        Mtime.add_span (Connection.now connection) timeout
                      with
                      | Some deadline -> deadline
                      | None -> Mtime.max_stamp
                    in
                    let rec loop responses =
                      let current = Connection.now connection in
                      if Mtime.compare current deadline >= 0 then
                        Ok (List.rev responses)
                      else
                        let remaining = Mtime.span current deadline in
                        match
                          Connection.Subscription.next_with_timeout
                            ~timeout:remaining subscription
                        with
                        | Error Core_error.Timeout -> Ok (List.rev responses)
                        | Error error -> Error (Error.Connection error)
                        | Ok delivery -> (
                            match delivery.status with
                            | Some { code = 503; _ } -> loop responses
                            | Some { code; description } ->
                                Error
                                  (Error.Unexpected_status { code; description })
                            | None -> (
                                match decode_response delivery.message with
                                | Error error -> Error error
                                | Ok response -> loop (response :: responses)))
                    in
                    loop []
              in
              try finish (collect_responses ())
              with Eio.Cancel.Cancelled _ as cancellation ->
                Eio.Cancel.protect (fun () ->
                    ignore (Connection.Subscription.unsubscribe subscription));
                raise cancellation))

  let ping ?(timeout = default_timeout) ?(target = All) connection =
    collect ~timeout ~target connection ~verb:"PING" (fun message ->
        match decode wire_identity_codec (Nats.Message.payload message) with
        | Error error -> Error error
        | Ok wire -> (
            match
              expect_response_type ~expected:"io.nats.micro.v1.ping_response"
                wire.type_
            with
            | Error error -> Error error
            | Ok () -> (
                match required_metadata wire.metadata with
                | Error error -> Error error
                | Ok metadata ->
                    Ok
                      {
                        Ping.name = wire.name;
                        id = wire.id;
                        version = wire.version;
                        metadata;
                      })))

  let info ?(timeout = default_timeout) ?(target = All) connection =
    collect ~timeout ~target connection ~verb:"INFO" (fun message ->
        match decode wire_info_codec (Nats.Message.payload message) with
        | Error error -> Error error
        | Ok wire -> discovery_info_of_wire wire)

  let stats ?(timeout = default_timeout) ?(target = All) connection =
    collect ~timeout ~target connection ~verb:"STATS" (fun message ->
        match decode wire_stats_codec (Nats.Message.payload message) with
        | Error error -> Error error
        | Ok wire -> discovery_stats_of_wire wire)
end

let encode codec value =
  match Jsont_bytesrw.encode_string' codec value with
  | Ok payload -> Ok payload
  | Error error -> Error (Error.Encode error)

let queue_group_string = function
  | None -> ""
  | Some queue -> Nats.Queue_group.to_string queue

let wire_endpoint_info_of_info (endpoint : Info.endpoint) =
  {
    name = Info.endpoint_name endpoint;
    subject = Nats.Subject.Filter.to_string (Info.endpoint_subject endpoint);
    queue_group = queue_group_string (Info.endpoint_queue endpoint);
    metadata = metadata_or_null (Info.endpoint_metadata endpoint);
  }

let wire_endpoint_stats_of_stats (endpoint : Stats.endpoint) =
  {
    name = Stats.endpoint_name endpoint;
    subject = Nats.Subject.Filter.to_string (Stats.endpoint_subject endpoint);
    queue_group = queue_group_string (Stats.endpoint_queue endpoint);
    metadata = metadata_or_null (Stats.endpoint_metadata endpoint);
    num_requests = Stats.num_requests endpoint;
    num_errors = Stats.num_errors endpoint;
    last_error = Stats.last_error endpoint;
    processing_time = Stats.processing_time endpoint;
    average_processing_time = Stats.average_processing_time endpoint;
    data = Stats.data endpoint;
  }

let control_payload (service : service) = function
  | Ping ->
      encode wire_identity_codec
        {
          type_ = "io.nats.micro.v1.ping_response";
          name = service.name;
          id = service.id;
          version = Config.version service.config;
          metadata = metadata_json (Config.metadata service.config);
        }
  | Info ->
      let value = info service in
      encode wire_info_codec
        {
          type_ = "io.nats.micro.v1.info_response";
          name = Info.name value;
          id = Info.id value;
          version = Info.version value;
          description = Option.value ~default:"" (Info.description value);
          metadata = metadata_json (Info.metadata value);
          endpoints = List.map wire_endpoint_info_of_info (Info.endpoints value);
        }
  | Stats ->
      let value = stats service in
      encode wire_stats_codec
        {
          type_ = "io.nats.micro.v1.stats_response";
          name = Stats.name value;
          id = Stats.id value;
          version = Stats.version value;
          metadata = metadata_json (Stats.metadata value);
          started = Stats.started value;
          endpoints =
            List.map wire_endpoint_stats_of_stats (Stats.endpoints value);
        }

let make_owned_subscription subscription =
  let done_promise, done_resolver = Eio.Promise.create () in
  { subscription; done_promise; done_resolver }

let all_workers (service : service) =
  List.map (fun (control : control) -> control.worker) service.controls
  @ List.map
      (fun (endpoint : endpoint_instance) -> endpoint.worker)
      service.endpoints

let callback_loop (service : service) (dispatch : callback_dispatch) =
  let running = ref true in
  while !running do
    match Eio.Stream.take dispatch.events with
    | Error_event error ->
        Option.iter
          (fun handler -> handler error)
          (Config.error_handler service.config)
    | Done_event resolver ->
        Fun.protect
          (fun () ->
            Option.iter
              (fun handler -> handler ())
              (Config.done_handler service.config))
          ~finally:(fun () -> Eio.Promise.resolve resolver ());
        running := false
    | Failure_event -> running := false
  done

let enqueue_failure_callbacks service error =
  match service.callbacks with
  | None -> ()
  | Some dispatch ->
      Eio.Stream.add dispatch.events (Error_event error);
      Eio.Stream.add dispatch.events Failure_event

let fail_service (service : service) error =
  let subscriptions =
    Eio.Mutex.use_rw ~protect:true service.mutex (fun () ->
        match service.state with
        | Open ->
            service.state <- Failed error;
            List.map (fun worker -> worker.subscription) (all_workers service)
        | Stopping | Stopped | Failed _ -> [])
  in
  List.iter
    (fun subscription ->
      ignore (Connection.Subscription.unsubscribe subscription))
    subscriptions;
  match subscriptions with
  | [] -> ()
  | _ -> enqueue_failure_callbacks service error

let record_endpoint (endpoint : endpoint_instance) ~handler_error request
    started =
  let duration =
    Mtime.Span.to_uint64_ns
      (Mtime.span started (Connection.now endpoint.service.connection))
  in
  let failure =
    match handler_error with
    | Some failure -> Some failure
    | None -> (
        match Request.last_error request with
        | Some error -> Some (error, string_of_error error)
        | None -> (
            match Request.service_error request with
            | Some (code, description) ->
                Some (Error.Service_error { code; description }, description)
            | None when not (Request.responded request) ->
                Some (Error.No_response, "no response")
            | None -> None))
  in
  Eio.Mutex.use_rw ~protect:true endpoint.service.mutex (fun () ->
      endpoint.num_requests <- Int64.add endpoint.num_requests 1L;
      endpoint.processing_time <- Int64.add endpoint.processing_time duration;
      match failure with
      | None -> ()
      | Some (_, message) ->
          endpoint.num_errors <- Int64.add endpoint.num_errors 1L;
          endpoint.last_error <- message)

let process_endpoint (endpoint : endpoint_instance)
    (delivery : Connection.Subscription.delivery) =
  let request = Request.make endpoint.service.connection delivery.message in
  let started = Connection.now endpoint.service.connection in
  let handler_error =
    try
      match endpoint.handler request with
      | Ok () -> None
      | Error error -> Some (error, string_of_error error)
    with
    | Eio.Cancel.Cancelled _ as cancellation -> raise cancellation
    | exception_value ->
        Some (Error.Handler_raised, Printexc.to_string exception_value)
  in
  record_endpoint endpoint ~handler_error request started

let endpoint_loop endpoint =
  Fun.protect
    (fun () ->
      let running = ref true in
      while !running do
        match Connection.Subscription.next endpoint.worker.subscription with
        | Ok delivery -> process_endpoint endpoint delivery
        | Error (Core_error.Closed | Core_error.Draining) -> running := false
        | Error error ->
            running := false;
            fail_service endpoint.service (Error.Connection error)
      done)
    ~finally:(fun () -> Eio.Promise.resolve endpoint.worker.done_resolver ())

let fatal_response_error = function
  | Error.Connection (Core_error.Closed | Core_error.Draining) -> true
  | _ -> false

let monitor_loop (service : service) (control : control) =
  Fun.protect
    (fun () ->
      let running = ref true in
      while !running do
        match Connection.Subscription.next control.worker.subscription with
        | Ok delivery -> (
            let request = Request.make service.connection delivery.message in
            match control_payload service control.kind with
            | Error _ -> ()
            | Ok payload -> (
                match Request.respond request payload with
                | Ok () | Error Error.No_reply_subject -> ()
                | Error error ->
                    if fatal_response_error error then running := false))
        | Error (Core_error.Closed | Core_error.Draining) -> running := false
        | Error error ->
            running := false;
            fail_service service (Error.Connection error)
      done)
    ~finally:(fun () -> Eio.Promise.resolve control.worker.done_resolver ())

let make_group service ~parent ~prefix ~queue name =
  match Nats.Subject.of_string name with
  | Error error -> Error (Error.Invalid_group_subject error)
  | Ok name_subject -> (
      let subject_string =
        match prefix with
        | None -> Nats.Subject.to_string name_subject
        | Some prefix ->
            Nats.Subject.to_string prefix
            ^ "."
            ^ Nats.Subject.to_string name_subject
      in
      match Nats.Subject.of_string subject_string with
      | Error error -> Error (Error.Invalid_group_subject error)
      | Ok subject ->
          Ok
            {
              service;
              name = Nats.Subject.to_string name_subject;
              subject;
              queue;
              parent;
            })

let add_endpoint_to service ~prefix ~parent_queue (endpoint : Endpoint.t) =
  let key = endpoint_full_name prefix (Endpoint.name endpoint) in
  match append_filter prefix (Endpoint.subject endpoint) with
  | Error error -> Error (Error.Invalid_group_subject error)
  | Ok subject -> (
      let queue = effective_endpoint_queue parent_queue endpoint in
      let pending_limits = Endpoint.pending_limits endpoint in
      let reservation =
        Eio.Mutex.use_rw ~protect:true service.mutex (fun () ->
            match service.state with
            | Stopping | Stopped -> Error Error.Stopped
            | Failed error -> Error error
            | Open ->
                if
                  List.exists
                    (fun (value : endpoint_instance) ->
                      String.equal value.key key)
                    service.endpoints
                  || List.exists (String.equal key)
                       service.pending_endpoint_names
                then Error (Error.Duplicate_endpoint key)
                else (
                  service.pending_endpoint_names <-
                    key :: service.pending_endpoint_names;
                  Ok ()))
      in
      match reservation with
      | Error error -> Error error
      | Ok () -> (
          match
            Connection.subscribe service.connection ?queue_group:queue
              ?pending_messages:
                (Option.map Endpoint.Pending_limits.messages pending_limits)
              ?pending_bytes:(Option.map Endpoint.Pending_limits.bytes pending_limits)
              subject
          with
          | Error error ->
              Eio.Mutex.use_rw ~protect:true service.mutex (fun () ->
                  service.pending_endpoint_names <-
                    List.filter
                      (fun value -> not (String.equal value key))
                      service.pending_endpoint_names);
              Error (Error.Connection error)
          | Ok subscription -> (
              let worker = make_owned_subscription subscription in
              let endpoint_instance =
                {
                  service;
                  key;
                  name = Endpoint.name endpoint;
                  subject;
                  queue;
                  metadata = Endpoint.metadata endpoint;
                  handler = endpoint.handler;
                  worker;
                  num_requests = 0L;
                  num_errors = 0L;
                  last_error = "";
                  processing_time = 0L;
                }
              in
              let inserted =
                Eio.Mutex.use_rw ~protect:true service.mutex (fun () ->
                    service.pending_endpoint_names <-
                      List.filter
                        (fun value -> not (String.equal value key))
                        service.pending_endpoint_names;
                    match service.state with
                    | Open ->
                        service.endpoints <-
                          endpoint_instance :: service.endpoints;
                        Ok ()
                    | Stopping | Stopped -> Error Error.Stopped
                    | Failed error -> Error error)
              in
              match inserted with
              | Error error ->
                  ignore
                    (Connection.Subscription.unsubscribe
                       endpoint_instance.worker.subscription);
                  Error error
              | Ok () ->
                  Eio.Fiber.fork ~sw:service.sw (fun () ->
                      endpoint_loop endpoint_instance);
                  Ok ())))

let add_endpoint service endpoint =
  add_endpoint_to service ~prefix:None
    ~parent_queue:(Config.queue service.config)
    endpoint

module Group = struct
  type t = group

  let name (value : group) = value.name
  let subject (value : group) = value.subject

  let add_endpoint (value : group) endpoint =
    add_endpoint_to value.service
      ~prefix:(Some (Nats.Subject.to_string value.subject))
      ~parent_queue:(effective_group_policy value)
      endpoint

  let add_group ?(queue = Config.Default) (value : group) ~name =
    Eio.Mutex.use_ro value.service.mutex (fun () ->
        match value.service.state with
        | Open ->
            make_group value.service ~parent:(Some value)
              ~prefix:(Some value.subject) ~queue name
        | Stopping | Stopped -> Error Error.Stopped
        | Failed error -> Error error)
end

let name (value : service) = value.name
let id (value : service) = value.id

let nuid_alphabet =
  "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

let fresh_id random =
  let value = Bytes.create 22 in
  for position = 0 to Bytes.length value - 1 do
    let index = Random.State.int random (String.length nuid_alphabet) in
    Bytes.set value position (String.get nuid_alphabet index)
  done;
  Bytes.to_string value

let rfc3339 timestamp =
  let seconds = Int64.of_float (Float.floor timestamp) in
  let fraction = timestamp -. Int64.to_float seconds in
  let nanos = Int64.of_float (Float.floor ((fraction *. 1e9) +. 0.5)) in
  let seconds, nanos =
    if Int64.equal nanos 1_000_000_000L then (Int64.add seconds 1L, 0L)
    else (seconds, nanos)
  in
  let time = Unix.gmtime (Int64.to_float seconds) in
  Format.sprintf "%04d-%02d-%02dT%02d:%02d:%02d.%09LdZ"
    (time.Unix.tm_year + 1900) (time.Unix.tm_mon + 1) time.Unix.tm_mday
    time.Unix.tm_hour time.Unix.tm_min time.Unix.tm_sec nanos

let reset service =
  Eio.Mutex.use_rw ~protect:true service.mutex (fun () ->
      service.started <- rfc3339 (service.wall_now ());
      List.iter
        (fun (endpoint : endpoint_instance) ->
          endpoint.num_requests <- 0L;
          endpoint.num_errors <- 0L;
          endpoint.last_error <- "";
          endpoint.processing_time <- 0L)
        service.endpoints)

let stopped service =
  Eio.Mutex.use_ro service.mutex (fun () ->
      match service.state with
      | Stopped -> true
      | Open | Stopping | Failed _ -> false)

let control_subject kind suffix =
  let verb =
    match kind with Ping -> "PING" | Info -> "INFO" | Stats -> "STATS"
  in
  Nats.Subject.Filter.literal
    (match suffix with
    | None -> "$SRV." ^ verb
    | Some suffix -> "$SRV." ^ verb ^ "." ^ suffix)

let subscribe_controls connection name id =
  let subjects =
    List.concat_map
      (fun kind ->
        [
          (kind, control_subject kind None);
          (kind, control_subject kind (Some name));
          (kind, control_subject kind (Some (name ^ "." ^ id)));
        ])
      [ Ping; Info; Stats ]
  in
  let rec loop remaining subscribed =
    match remaining with
    | [] -> Ok (List.rev subscribed)
    | (kind, subject) :: rest -> (
        match Connection.subscribe connection subject with
        | Ok subscription ->
            loop rest
              ({ kind; worker = make_owned_subscription subscription }
              :: subscribed)
        | Error error ->
            List.iter
              (fun (control : control) ->
                ignore
                  (Connection.Subscription.unsubscribe
                     control.worker.subscription))
              subscribed;
            Error (Error.Connection error))
  in
  loop subjects []

let add_group ?(queue = Config.Default) service ~name =
  Eio.Mutex.use_ro service.mutex (fun () ->
      match service.state with
      | Open -> make_group service ~parent:None ~prefix:None ~queue name
      | Stopping | Stopped -> Error Error.Stopped
      | Failed error -> Error error)

type stop_action =
  | Stop_done of (unit, Error.t) result
  | Stop_wait of (unit, Error.t) result Eio.Promise.t
  | Stop_start of {
      resolver : (unit, Error.t) result Eio.Promise.u;
      subscriptions : Connection.Subscription.t list;
      done_promises : unit Eio.Promise.t list;
      hook : Eio.Switch.hook option;
    }

let stop_action service =
  Eio.Mutex.use_rw ~protect:true service.mutex (fun () ->
      match service.state with
      | Stopped -> Stop_done (Ok ())
      | Failed error -> Stop_done (Error error)
      | Stopping -> (
          match service.stop_promise with
          | Some promise -> Stop_wait promise
          | None -> Stop_done (Error Error.Stopped))
      | Open ->
          let promise, resolver = Eio.Promise.create () in
          service.state <- Stopping;
          service.stop_promise <- Some promise;
          let workers = all_workers service in
          let subscriptions =
            List.map (fun worker -> worker.subscription) workers
          in
          let done_promises =
            List.map (fun worker -> worker.done_promise) workers
          in
          let hook = service.hook in
          service.hook <- None;
          Stop_start { resolver; subscriptions; done_promises; hook })

let run_stop service ~timeout ~resolver ~subscriptions ~done_promises =
  let first_error = ref None in
  List.iter
    (fun subscription ->
      match Connection.Subscription.drain ?timeout subscription with
      | Ok () -> ()
      | Error error ->
          if Option.is_none !first_error then first_error := Some error)
    subscriptions;
  (match !first_error with
  | None -> ()
  | Some _ ->
      List.iter
        (fun subscription ->
          ignore (Connection.Subscription.unsubscribe subscription))
        subscriptions);
  List.iter Eio.Promise.await done_promises;
  let result =
    match !first_error with
    | None -> Ok ()
    | Some error -> Error (Error.Connection error)
  in
  Eio.Mutex.use_rw ~protect:true service.mutex (fun () ->
      service.state <- Stopped);
  (match service.callbacks with
  | None -> ()
  | Some dispatch ->
      let done_promise, done_resolver = Eio.Promise.create () in
      Eio.Stream.add dispatch.events (Done_event done_resolver);
      Eio.Promise.await done_promise);
  Eio.Promise.resolve resolver result;
  result

let stop ?timeout service =
  match stop_action service with
  | Stop_done result -> result
  | Stop_wait promise -> Eio.Promise.await promise
  | Stop_start { resolver; subscriptions; done_promises; hook } ->
      Option.iter (fun hook -> ignore (Eio.Switch.try_remove_hook hook)) hook;
      Eio.Cancel.protect (fun () ->
          run_stop service ~timeout ~resolver ~subscriptions ~done_promises)

let v ~sw ~clock ?random connection config =
  let random = Option.value ~default:(Random.State.make_self_init ()) random in
  let id = fresh_id random in
  let wall_now () = Eio.Time.now clock in
  let started = rfc3339 (wall_now ()) in
  match subscribe_controls connection (Config.name config) id with
  | Error error -> Error error
  | Ok controls ->
      let service =
        let callbacks =
          match
            (Config.error_handler config, Config.done_handler config)
          with
          | None, None -> None
          | Some _, _ | _, Some _ ->
              Some { events = Eio.Stream.create max_int }
        in
        {
          sw;
          connection;
          config;
          name = Config.name config;
          id;
          started;
          wall_now;
          mutex = Eio.Mutex.create ();
          state = Open;
          controls;
          endpoints = [];
          pending_endpoint_names = [];
          stop_promise = None;
          hook = None;
          callbacks;
        }
      in
      Option.iter
        (fun dispatch ->
          Eio.Fiber.fork ~sw (fun () -> callback_loop service dispatch))
        service.callbacks;
      List.iter
        (fun control ->
          Eio.Fiber.fork ~sw (fun () -> monitor_loop service control))
        controls;
      let hook =
        Eio.Switch.on_release_cancellable sw (fun () ->
            Eio.Cancel.protect (fun () -> ignore (stop service)))
      in
      service.hook <- Some hook;
      Ok service
