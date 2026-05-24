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

  type t =
    | Connection of Connection.error
    | Invalid_config of config
    | Invalid_endpoint of endpoint
    | Invalid_group_subject of Nats.Subject.error
    | Duplicate_endpoint of string
    | Invalid_headers of Nats.Header.error
    | Invalid_service_error of { code : string; description : string }
    | No_reply_subject
    | Already_responded
    | No_response
    | Service_error of { code : string; description : string }
    | Handler_raised
    | Encode of Jsont.Error.t
    | Stopped

  let pp_config ppf = function
    | Invalid_version value ->
        Format.fprintf ppf "invalid service version %S" value
    | Empty_name -> Format.pp_print_string ppf "name is empty"
    | Invalid_name_character { position; character } ->
        Format.fprintf ppf "invalid name character %C at position %d" character
          position
    | Duplicate_metadata name ->
        Format.fprintf ppf "metadata repeats key %S" name

  let pp_endpoint ppf = function
    | Empty_name -> Format.pp_print_string ppf "name is empty"
    | Invalid_name_character { position; character } ->
        Format.fprintf ppf "invalid name character %C at position %d" character
          position
    | Duplicate_metadata name ->
        Format.fprintf ppf "metadata repeats key %S" name

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

module Config = struct
  type queue_policy = Default | Queue of Nats.Queue_group.t | Disabled

  type t = {
    name : string;
    version : string;
    description : string option;
    metadata : (string * string) list;
    queue : queue_policy;
  }

  let v ~name ~version ?description ?(metadata = []) ?(queue = Default) () =
    match validate_identifier name with
    | Error error -> Error (Error.Invalid_config (config_name_error error))
    | Ok () -> (
        if not (validate_version version) then
          Error (Error.Invalid_config (Error.Invalid_version version))
        else
          match duplicate_metadata metadata with
          | Some name ->
              Error (Error.Invalid_config (Error.Duplicate_metadata name))
          | None -> Ok { name; version; description; metadata; queue })

  let name value = value.name
  let version value = value.version
  let description value = value.description
  let metadata value = value.metadata
  let queue value = value.queue
end

module Request = struct
  type t = {
    connection : Connection.t;
    message : Nats.Message.t;
    mutable responded : bool;
    mutable last_error : Error.t option;
    mutable service_error : (string * string) option;
  }

  let make connection message =
    {
      connection;
      message;
      responded = false;
      last_error = None;
      service_error = None;
    }

  let subject value = Nats.Message.subject value.message
  let reply value = Nats.Message.reply_to value.message
  let headers value = Nats.Message.headers value.message
  let payload value = Nats.Message.payload value.message

  let fail value error =
    value.last_error <- Some error;
    Error error

  let respond ?(headers = Nats.Header.empty) value payload =
    if value.responded then fail value Error.Already_responded
    else
      match reply value with
      | None -> fail value Error.No_reply_subject
      | Some reply -> (
          value.responded <- true;
          let message = Nats.Message.v ~subject:reply ~headers payload in
          match Connection.publish_msg value.connection message with
          | Ok () -> Ok ()
          | Error error -> fail value (Error.Connection error))

  let respond_error ~code ~description ?(headers = Nats.Header.empty)
      ?(payload = "") value =
    if
      Int.equal (String.length code) 0
      || Int.equal (String.length description) 0
    then fail value (Error.Invalid_service_error { code; description })
    else if value.responded then fail value Error.Already_responded
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
                  value.service_error <- Some (code, description);
                  Ok ()))

  let responded value = value.responded
  let last_error value = value.last_error
  let service_error value = value.service_error
end

module Endpoint = struct
  type t = {
    name : string;
    subject : Nats.Subject.Filter.t;
    metadata : (string * string) list option;
    queue : Config.queue_policy;
    handler : Request.t -> unit;
  }

  type handler = Request.t -> unit

  let v ~name ?subject ?metadata ?(queue = Config.Default) handler =
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
                handler;
              })

  let name (value : t) = value.name
  let subject (value : t) = value.subject
  let metadata value = value.metadata
  let queue value = value.queue
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

module Stats = struct
  type endpoint = {
    name : string;
    subject : Nats.Subject.Filter.t;
    queue : Nats.Queue_group.t option;
    metadata : (string * string) list option;
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
  let num_requests (value : endpoint) = value.num_requests
  let num_errors (value : endpoint) = value.num_errors
  let last_error (value : endpoint) = value.last_error
  let processing_time (value : endpoint) = value.processing_time
  let average_processing_time (value : endpoint) = value.average_processing_time
end

type service_state = Open | Stopping | Stopped | Failed of Error.t
type control_kind = Ping | Info | Stats
type control = { kind : control_kind; subscription : Connection.Subscription.t }

type service = {
  sw : Eio.Switch.t;
  connection : Connection.t;
  config : Config.t;
  name : string;
  id : string;
  started : string;
  mutex : Eio.Mutex.t;
  mutable state : service_state;
  controls : control list;
  mutable endpoints : endpoint_instance list;
  mutable stop_promise : (unit, Error.t) result Eio.Promise.t option;
  mutable hook : Eio.Switch.hook option;
}

and endpoint_instance = {
  service : service;
  name : string;
  subject : Nats.Subject.Filter.t;
  queue : Nats.Queue_group.t option;
  metadata : (string * string) list option;
  handler : Request.t -> unit;
  subscription : Connection.Subscription.t;
  done_promise : unit Eio.Promise.t;
  done_resolver : unit Eio.Promise.u;
  mutable num_requests : int64;
  mutable num_errors : int64;
  mutable last_error : string;
  mutable processing_time : int64;
}

and group = {
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

let append_subject prefix subject =
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
    num_requests = endpoint.num_requests;
    num_errors = endpoint.num_errors;
    last_error = endpoint.last_error;
    processing_time = endpoint.processing_time;
    average_processing_time =
      (if Int64.equal endpoint.num_requests 0L then 0L
       else Int64.div endpoint.processing_time endpoint.num_requests);
  }

let info_without_lock service =
  {
    Info.name = service.name;
    id = service.id;
    version = Config.version service.config;
    description = Config.description service.config;
    metadata = Config.metadata service.config;
    endpoints = List.rev_map info_endpoint service.endpoints;
  }

let stats_without_lock service =
  {
    Stats.name = service.name;
    id = service.id;
    version = Config.version service.config;
    metadata = Config.metadata service.config;
    started = service.started;
    endpoints = List.rev_map stats_endpoint service.endpoints;
  }

let info service =
  Eio.Mutex.use_ro service.mutex (fun () -> info_without_lock service)

let stats service =
  Eio.Mutex.use_ro service.mutex (fun () -> stats_without_lock service)

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
  queue_group : string option;
  metadata : Jsont.json;
}

let wire_endpoint_info_codec =
  Jsont.Object.map ~kind:"NATS service endpoint"
    (fun name subject queue_group metadata ->
      { name; subject; queue_group; metadata })
  |> Jsont.Object.mem "name" Jsont.string ~enc:(fun value -> value.name)
  |> Jsont.Object.mem "subject" Jsont.string ~enc:(fun value -> value.subject)
  |> Jsont.Object.opt_mem "queue_group" Jsont.string ~enc:(fun value ->
      value.queue_group)
  |> Jsont.Object.mem "metadata" Jsont.json ~enc:(fun value -> value.metadata)
  |> Jsont.Object.finish

type wire_info = {
  type_ : string;
  name : string;
  id : string;
  version : string;
  description : string option;
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
  |> Jsont.Object.opt_mem "description" Jsont.string ~enc:(fun value ->
      value.description)
  |> Jsont.Object.mem "metadata" Jsont.json ~enc:(fun value -> value.metadata)
  |> Jsont.Object.mem "endpoints" (Jsont.list wire_endpoint_info_codec)
       ~enc:(fun value -> value.endpoints)
  |> Jsont.Object.finish

type wire_endpoint_stats = {
  name : string;
  subject : string;
  queue_group : string option;
  metadata : Jsont.json;
  num_requests : int64;
  num_errors : int64;
  last_error : string;
  processing_time : int64;
  average_processing_time : int64;
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
      })
  |> Jsont.Object.mem "name" Jsont.string ~enc:(fun value -> value.name)
  |> Jsont.Object.mem "subject" Jsont.string ~enc:(fun value -> value.subject)
  |> Jsont.Object.opt_mem "queue_group" Jsont.string ~enc:(fun value ->
      value.queue_group)
  |> Jsont.Object.mem "metadata" Jsont.json ~enc:(fun value -> value.metadata)
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

let encode codec value =
  match Jsont_bytesrw.encode_string' codec value with
  | Ok payload -> Ok payload
  | Error error -> Error (Error.Encode error)

let wire_endpoint_info_of_info (endpoint : Info.endpoint) =
  {
    name = Info.endpoint_name endpoint;
    subject = Nats.Subject.Filter.to_string (Info.endpoint_subject endpoint);
    queue_group =
      Option.map Nats.Queue_group.to_string (Info.endpoint_queue endpoint);
    metadata = metadata_or_null (Info.endpoint_metadata endpoint);
  }

let wire_endpoint_stats_of_stats (endpoint : Stats.endpoint) =
  {
    name = Stats.endpoint_name endpoint;
    subject = Nats.Subject.Filter.to_string (Stats.endpoint_subject endpoint);
    queue_group =
      Option.map Nats.Queue_group.to_string (Stats.endpoint_queue endpoint);
    metadata = metadata_or_null (Stats.endpoint_metadata endpoint);
    num_requests = Stats.num_requests endpoint;
    num_errors = Stats.num_errors endpoint;
    last_error = Stats.last_error endpoint;
    processing_time = Stats.processing_time endpoint;
    average_processing_time = Stats.average_processing_time endpoint;
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
          description = Info.description value;
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

let fail_service (service : service) error =
  let subscriptions =
    Eio.Mutex.use_rw ~protect:true service.mutex (fun () ->
        match service.state with
        | Open ->
            service.state <- Failed error;
            List.map
              (fun (control : control) -> control.subscription)
              service.controls
            @ List.map
                (fun (endpoint : endpoint_instance) -> endpoint.subscription)
                service.endpoints
        | Stopping | Stopped | Failed _ -> [])
  in
  List.iter
    (fun subscription ->
      ignore (Connection.Subscription.unsubscribe subscription))
    subscriptions

let record_endpoint (endpoint : endpoint_instance) ~completed request started =
  let duration =
    Mtime.Span.to_uint64_ns
      (Mtime.span started (Connection.now endpoint.service.connection))
  in
  let failure =
    if not completed then Some (Error.Handler_raised, "handler raised")
    else
      match (Request.last_error request, Request.service_error request) with
      | Some error, _ -> Some (error, string_of_error error)
      | None, Some (code, description) ->
          Some (Error.Service_error { code; description }, description)
      | None, None when not (Request.responded request) ->
          Some (Error.No_response, "no response")
      | None, None -> None
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
  let completed = ref false in
  Fun.protect
    (fun () ->
      endpoint.handler request;
      completed := true)
    ~finally:(fun () ->
      record_endpoint endpoint ~completed:!completed request started)

let endpoint_loop endpoint =
  Fun.protect
    (fun () ->
      let running = ref true in
      while !running do
        match Connection.Subscription.next endpoint.subscription with
        | Ok delivery -> process_endpoint endpoint delivery
        | Error (Core_error.Closed | Core_error.Draining) -> running := false
        | Error error ->
            running := false;
            fail_service endpoint.service (Error.Connection error)
      done)
    ~finally:(fun () -> Eio.Promise.resolve endpoint.done_resolver ())

let monitor_loop (service : service) (control : control) =
  Fun.protect
    (fun () ->
      let running = ref true in
      while !running do
        match Connection.Subscription.next control.subscription with
        | Ok delivery -> (
            let request = Request.make service.connection delivery.message in
            match control_payload service control.kind with
            | Error error ->
                running := false;
                fail_service service error
            | Ok payload -> (
                match Request.respond request payload with
                | Ok () | Error Error.No_reply_subject -> ()
                | Error error ->
                    running := false;
                    fail_service service error))
        | Error (Core_error.Closed | Core_error.Draining) -> running := false
        | Error error ->
            running := false;
            fail_service service (Error.Connection error)
      done)
    ~finally:ignore

let add_endpoint_to (service : service) ~prefix ~parent_queue
    (endpoint : Endpoint.t) =
  Eio.Mutex.use_rw ~protect:true service.mutex (fun () ->
      match service.state with
      | Stopping | Stopped -> Error Error.Stopped
      | Failed error -> Error error
      | Open -> (
          let name = endpoint_full_name prefix (Endpoint.name endpoint) in
          if
            List.exists
              (fun (value : endpoint_instance) -> String.equal value.name name)
              service.endpoints
          then Error (Error.Duplicate_endpoint name)
          else
            match append_subject prefix (Endpoint.subject endpoint) with
            | Error error -> Error (Error.Invalid_group_subject error)
            | Ok subject -> (
                let queue = effective_endpoint_queue parent_queue endpoint in
                match
                  Connection.subscribe service.connection ?queue_group:queue
                    subject
                with
                | Error error -> Error (Error.Connection error)
                | Ok subscription ->
                    let done_promise, done_resolver = Eio.Promise.create () in
                    let endpoint =
                      {
                        service;
                        name;
                        subject;
                        queue;
                        metadata = Endpoint.metadata endpoint;
                        handler = endpoint.handler;
                        subscription;
                        done_promise;
                        done_resolver;
                        num_requests = 0L;
                        num_errors = 0L;
                        last_error = "";
                        processing_time = 0L;
                      }
                    in
                    service.endpoints <- endpoint :: service.endpoints;
                    Eio.Fiber.fork ~sw:service.sw (fun () ->
                        endpoint_loop endpoint);
                    Ok ())))

let make_group service ~parent ~prefix ~queue name =
  match Nats.Subject.of_string name with
  | Error error -> Error (Error.Invalid_group_subject error)
  | Ok name -> (
      let subject_string =
        match prefix with
        | None -> Nats.Subject.to_string name
        | Some prefix ->
            Nats.Subject.to_string prefix ^ "." ^ Nats.Subject.to_string name
      in
      match Nats.Subject.of_string subject_string with
      | Error error -> Error (Error.Invalid_group_subject error)
      | Ok subject ->
          Ok { service; name = subject_string; subject; queue; parent })

module Group = struct
  type t = group

  let name (value : group) = value.name
  let subject (value : group) = value.subject

  let add_endpoint (value : group) endpoint =
    add_endpoint_to value.service ~prefix:(Some value.name)
      ~parent_queue:(effective_group_policy value)
      endpoint

  let add_group ?(queue = Config.Default) (value : group) ~name =
    make_group value.service ~parent:(Some value) ~prefix:(Some value.subject)
      ~queue name
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
        | Ok subscription -> loop rest ({ kind; subscription } :: subscribed)
        | Error error ->
            List.iter
              (fun (control : control) ->
                ignore
                  (Connection.Subscription.unsubscribe control.subscription))
              subscribed;
            Error (Error.Connection error))
  in
  loop subjects []

let add_endpoint service endpoint =
  add_endpoint_to service ~prefix:None
    ~parent_queue:(Config.queue service.config)
    endpoint

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
          let subscriptions =
            List.map
              (fun (control : control) -> control.subscription)
              service.controls
            @ List.map
                (fun (endpoint : endpoint_instance) -> endpoint.subscription)
                service.endpoints
          in
          let done_promises =
            List.map
              (fun (endpoint : endpoint_instance) -> endpoint.done_promise)
              service.endpoints
          in
          Option.iter
            (fun hook -> ignore (Eio.Switch.try_remove_hook hook))
            service.hook;
          service.hook <- None;
          Stop_start { resolver; subscriptions; done_promises })

let run_stop service ~timeout ~resolver ~subscriptions ~done_promises =
  let first_error = ref None in
  List.iter
    (fun subscription ->
      match Connection.Subscription.drain ?timeout subscription with
      | Ok () -> ()
      | Error error ->
          if Option.is_none !first_error then first_error := Some error)
    subscriptions;
  Option.iter
    (fun _ ->
      List.iter
        (fun subscription ->
          ignore (Connection.Subscription.unsubscribe subscription))
        subscriptions)
    !first_error;
  List.iter Eio.Promise.await done_promises;
  let result =
    match !first_error with
    | None -> Ok ()
    | Some error -> Error (Error.Connection error)
  in
  Eio.Mutex.use_rw ~protect:true service.mutex (fun () ->
      service.state <- Stopped);
  Eio.Promise.resolve resolver result;
  result

let stop ?timeout service =
  match stop_action service with
  | Stop_done result -> result
  | Stop_wait promise -> Eio.Promise.await promise
  | Stop_start { resolver; subscriptions; done_promises } ->
      run_stop service ~timeout ~resolver ~subscriptions ~done_promises

let v ~sw ~clock ?random connection config =
  let random = Option.value ~default:(Random.State.make_self_init ()) random in
  let id = fresh_id random in
  let started = rfc3339 (Eio.Time.now clock) in
  match subscribe_controls connection (Config.name config) id with
  | Error error -> Error error
  | Ok controls ->
      let service =
        {
          sw;
          connection;
          config;
          name = Config.name config;
          id;
          started;
          mutex = Eio.Mutex.create ();
          state = Open;
          controls;
          endpoints = [];
          stop_promise = None;
          hook = None;
        }
      in
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
