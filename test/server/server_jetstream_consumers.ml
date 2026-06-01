let failf format =
  Format.kasprintf (fun message -> raise (Failure message)) format

let error_message error = Format.asprintf "%a" Nats_eio.Error.pp error

let jetstream_error_message error =
  Format.asprintf "%a" Nats_eio.Jetstream.Error.pp error

let expect_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (error_message error)

let expect_jetstream_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %s" label (jetstream_error_message error)

let expect_jetstream_config_ok label = function
  | Ok value -> value
  | Error error -> failf "%s: %a" label Nats_eio.Jetstream.Error.pp_config error

let safe_credential value =
  String.length value > 0
  && String.for_all
       (fun character ->
         let code = Char.code character in
         (code >= Char.code 'A' && code <= Char.code 'Z')
         || (code >= Char.code 'a' && code <= Char.code 'z')
         || (code >= Char.code '0' && code <= Char.code '9')
         || code = Char.code '_'
         || code = Char.code '-')
       value

let auth () =
  match
    ( Sys.getenv_opt "NATS_TEST_USER",
      Sys.getenv_opt "NATS_TEST_PASS",
      Sys.getenv_opt "NATS_TEST_TOKEN" )
  with
  | None, None, None -> None
  | None, None, Some token when safe_credential token ->
      Some (Nats.Auth.token token)
  | Some user, Some pass, None when safe_credential user && safe_credential pass
    -> Some (Nats.Auth.user_pass ~user ~pass)
  | Some _, Some _, None ->
      failf
        "NATS_TEST_USER and NATS_TEST_PASS must be non-empty ASCII letters, digits, underscores, or hyphens"
  | None, None, Some _ ->
      failf
        "NATS_TEST_TOKEN must be non-empty ASCII letters, digits, underscores, or hyphens"
  | _ ->
      failf
        "set either NATS_TEST_TOKEN or both NATS_TEST_USER and NATS_TEST_PASS"

let endpoint () =
  let value =
    match Sys.getenv_opt "NATS_TEST_SERVER" with
    | Some value -> value
    | None -> "nats://127.0.0.1:4222"
  in
  match Nats.Endpoint.of_string value with
  | Ok endpoint -> endpoint
  | Error error ->
      failf "invalid NATS_TEST_SERVER %S: %a" value Nats.Endpoint.pp_error error

let connect ~sw ~net ~clock ?config endpoint =
  expect_ok "connect"
    (Nats_eio.Connection.connect ~sw ~net ~clock ?config [ endpoint ])

let publish ~timeout jetstream ~msg_id subject payload =
  expect_jetstream_ok ("publish " ^ msg_id)
    (Nats_eio.Jetstream.publish ~timeout ~msg_id jetstream subject payload)

let expect_payload label expected message =
  let actual = Nats_eio.Jetstream.Msg.payload message in
  if not (String.equal actual expected) then
    failf "%s payload was %S, expected %S" label actual expected

let close_push label push =
  match Nats_eio.Jetstream.Consumer.Push.close push with
  | Ok () -> ()
  | Error error -> failf "%s close: %s" label (jetstream_error_message error)

let close_ordered label ordered =
  match Nats_eio.Jetstream.Consumer.Ordered.close ordered with
  | Ok () -> ()
  | Error error -> failf "%s close: %s" label (jetstream_error_message error)

let run_durable_push ~sw ~timeout ~jetstream ~stream ~filter ~subject =
  let delivery_subject =
    Nats.Subject.literal "ocaml.integration.delivery.push"
  in
  let config =
    expect_jetstream_config_ok "durable push config"
      (Nats_eio.Jetstream.Consumer.Config.v
         ~durable_name:"OCAML_TEST_PUSH"
         ~deliver_subject:delivery_subject ~filter_subject:filter ())
  in
  let consumer =
    expect_jetstream_ok "durable push create"
      (Nats_eio.Jetstream.Consumer.create stream config)
  in
  let deleted = ref false in
  Fun.protect
    ~finally:(fun () ->
      if not !deleted then
        match Nats_eio.Jetstream.Consumer.delete consumer with
        | Ok () -> ()
        | Error error ->
            prerr_endline
              (Format.asprintf "durable push cleanup failed: %a"
                 Nats_eio.Jetstream.Error.pp error))
    (fun () ->
      let push =
        expect_jetstream_ok "durable push open"
          (Nats_eio.Jetstream.Consumer.Push.v ~sw consumer)
      in
      Fun.protect
        ~finally:(fun () -> close_push "durable push" push)
        (fun () ->
          if not (Int64.equal (Nats_eio.Jetstream.Consumer.Push.initial_pending push) 0L)
          then failf "durable push was not initially empty";
          ignore
            (publish ~timeout jetstream ~msg_id:"consumer-push-1" subject
               "push-one");
          let message =
            expect_jetstream_ok "durable push delivery"
              (Nats_eio.Jetstream.Consumer.Push.next_with_timeout ~timeout push)
          in
          expect_payload "durable push" "push-one" message;
          expect_jetstream_ok "durable push ack"
            (Nats_eio.Jetstream.Msg.ack message);
          print_endline "jetstream_durable_push: ok");
      expect_jetstream_ok "durable push delete"
        (Nats_eio.Jetstream.Consumer.delete consumer);
      deleted := true)

let run_owned_push ~sw ~timeout ~jetstream ~stream ~filter ~subject =
  let config =
    expect_jetstream_config_ok "owned push config"
      (Nats_eio.Jetstream.Consumer.Config.v ~filter_subject:filter
         ~deliver_policy:Nats_eio.Jetstream.Consumer.Config.New ())
  in
  let push =
    expect_jetstream_ok "owned push create"
      (Nats_eio.Jetstream.Consumer.Push.create ~sw stream config)
  in
  let consumer_name =
    Nats_eio.Jetstream.Consumer.name
      (Nats_eio.Jetstream.Consumer.Push.consumer push)
  in
  Fun.protect
    ~finally:(fun () -> close_push "owned push" push)
    (fun () ->
      ignore
        (publish ~timeout jetstream ~msg_id:"consumer-push-2" subject
           "push-two");
      let message =
        expect_jetstream_ok "owned push delivery"
          (Nats_eio.Jetstream.Consumer.Push.next_with_timeout ~timeout push)
      in
      expect_payload "owned push" "push-two" message;
      expect_jetstream_ok "owned push ack" (Nats_eio.Jetstream.Msg.ack message);
      print_endline "jetstream_owned_push: ok";
      close_push "owned push" push;
      let rebound =
        expect_jetstream_ok "owned push bind after close"
          (Nats_eio.Jetstream.Consumer.bind stream ~name:consumer_name)
      in
      match Nats_eio.Jetstream.Consumer.info rebound with
      | Error (Nats_eio.Jetstream.Error.Api _) -> ()
      | Ok _ -> failf "owned push consumer survived close"
      | Error error ->
          failf "owned push info after close: %s" (jetstream_error_message error))

let run_ordered ~sw ~clock ~timeout ~jetstream ~stream ~filter ~subject ~other_subject =
  ignore
    (publish ~timeout jetstream ~msg_id:"consumer-ordered-1" subject "ordered-one");
  ignore
    (publish ~timeout jetstream ~msg_id:"consumer-ordered-gap" other_subject
       "not-ordered");
  ignore
    (publish ~timeout jetstream ~msg_id:"consumer-ordered-2" subject "ordered-two");
  let ordered =
    expect_jetstream_ok "ordered create"
      (Nats_eio.Jetstream.Consumer.Ordered.v ~sw ~batch:1
         ~filter_subject:filter stream)
  in
  let closed = ref false in
  Fun.protect
    ~finally:(fun () ->
      if not !closed then
        match Nats_eio.Jetstream.Consumer.Ordered.close ordered with
        | Ok () -> ()
        | Error error ->
            prerr_endline
              (Format.asprintf "ordered cleanup failed: %a"
                 Nats_eio.Jetstream.Error.pp error))
    (fun () ->
      let first =
        expect_jetstream_ok "ordered first delivery"
          (Nats_eio.Jetstream.Consumer.Ordered.next_with_timeout ~timeout ordered)
      in
      let second =
        expect_jetstream_ok "ordered second delivery"
          (Nats_eio.Jetstream.Consumer.Ordered.next_with_timeout ~timeout ordered)
      in
      expect_payload "ordered first" "ordered-one" first;
      expect_payload "ordered second" "ordered-two" second;
      if
        Int64.compare
          (Nats_eio.Jetstream.Msg.stream_sequence second)
          (Nats_eio.Jetstream.Msg.stream_sequence first)
        <= 0
      then failf "ordered stream sequence did not advance";
      if
        not
          (Int64.equal
             (Nats_eio.Jetstream.Msg.consumer_sequence first)
             1L)
      then failf "ordered first consumer sequence was not one";
      if
        not
          (Int64.equal
             (Nats_eio.Jetstream.Msg.consumer_sequence second)
             2L)
      then failf "ordered second consumer sequence was not two";
      let consumer_infos =
        expect_jetstream_ok "ordered consumer list"
          (Nats_eio.Jetstream.Consumer.list stream)
      in
      let consumer_name =
        match consumer_infos with
        | [ info ] -> Nats_eio.Jetstream.Consumer.Info.name info
        | infos -> failf "ordered created %d consumers" (List.length infos)
      in
      let rebound =
        expect_jetstream_ok "ordered bind for deletion"
          (Nats_eio.Jetstream.Consumer.bind stream ~name:consumer_name)
      in
      let recovery_result, recovery_result_u = Eio.Promise.create () in
      Eio.Fiber.fork ~sw (fun () ->
          Eio.Promise.resolve recovery_result_u
            (Nats_eio.Jetstream.Consumer.Ordered.next_with_timeout ~timeout
               ordered));
      let waiting = ref false in
      for _ = 1 to 20 do
        if not !waiting then (
          let info =
            expect_jetstream_ok "ordered waiting info"
              (Nats_eio.Jetstream.Consumer.info rebound)
          in
          if Int.compare (Nats_eio.Jetstream.Consumer.Info.num_waiting info) 0 > 0
          then waiting := true
          else Eio.Time.Mono.sleep clock 0.05)
      done;
      if not !waiting then failf "ordered pull did not become outstanding";
      expect_jetstream_ok "ordered delete current consumer"
        (Nats_eio.Jetstream.Consumer.delete rebound);
      ignore
        (publish ~timeout jetstream ~msg_id:"consumer-ordered-3" subject
           "ordered-after-delete");
      let recovered =
        expect_jetstream_ok "ordered recovery delivery"
          (Eio.Promise.await recovery_result)
      in
      expect_payload "ordered recovery" "ordered-after-delete" recovered;
      if
        not
          (Int64.equal
             (Nats_eio.Jetstream.Msg.consumer_sequence recovered)
             1L)
      then failf "ordered recovery consumer sequence was not reset";
      print_endline "jetstream_ordered: ok";
      close_ordered "ordered" ordered;
      closed := true;
      match
        Nats_eio.Jetstream.Consumer.Ordered.next ordered
      with
      | Error Nats_eio.Jetstream.Error.Ordered_closed -> ()
      | Ok _ -> failf "closed ordered consumer returned a message"
      | Error error ->
          failf "closed ordered consumer: %s" (jetstream_error_message error))

let run env =
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.mono_clock env in
  let timeout = Mtime.Span.(3 * s) in
  let endpoint = endpoint () in
  let config =
    match auth () with
    | None -> None
    | Some auth ->
        Some
          (expect_ok "auth config"
             (Nats_eio.Connection.Config.v ~auth ()))
  in
  let connection = connect ~sw ~net ~clock ?config endpoint in
  Fun.protect
    ~finally:(fun () -> ignore (Nats_eio.Connection.close connection))
    (fun () ->
      let jetstream =
        expect_jetstream_ok "jetstream" (Nats_eio.Jetstream.v connection)
      in
      let run_id =
        match Sys.getenv_opt "NATS_TEST_JETSTREAM_RUN_ID" with
        | Some value when safe_credential value -> value
        | _ -> "direct"
      in
      let stream_name = "OCAML_TEST_CONSUMERS_" ^ run_id in
      let stream_filter =
        Nats.Subject.Filter.literal "ocaml.integration.consumer.>"
      in
      let stream_config =
        expect_jetstream_config_ok "consumer stream config"
          (Nats_eio.Jetstream.Stream.Config.v ~name:stream_name
             ~subjects:[ stream_filter ]
             ~storage:Nats_eio.Jetstream.Stream.Config.Memory ())
      in
      let stream =
        expect_jetstream_ok "consumer stream create"
          (Nats_eio.Jetstream.Stream.create jetstream stream_config)
      in
      let deleted = ref false in
      Fun.protect
        ~finally:(fun () ->
          if not !deleted then
            match Nats_eio.Jetstream.Stream.delete stream with
            | Ok () -> ()
            | Error error ->
                prerr_endline
                  (Format.asprintf "consumer stream cleanup failed: %a"
                     Nats_eio.Jetstream.Error.pp error))
        (fun () ->
          let push_filter =
            Nats.Subject.Filter.literal "ocaml.integration.consumer.push"
          in
          let push_subject =
            Nats.Subject.literal "ocaml.integration.consumer.push"
          in
          run_durable_push ~sw ~timeout ~jetstream ~stream ~filter:push_filter
            ~subject:push_subject;
          run_owned_push ~sw ~timeout ~jetstream ~stream ~filter:push_filter
            ~subject:push_subject;
          let ordered_filter =
            Nats.Subject.Filter.literal "ocaml.integration.consumer.ordered"
          in
          let ordered_subject =
            Nats.Subject.literal "ocaml.integration.consumer.ordered"
          in
          let other_subject =
            Nats.Subject.literal "ocaml.integration.consumer.other"
          in
          run_ordered ~sw ~clock ~timeout ~jetstream ~stream
            ~filter:ordered_filter ~subject:ordered_subject ~other_subject;
          expect_jetstream_ok "consumer stream delete"
            (Nats_eio.Jetstream.Stream.delete stream);
          deleted := true;
          print_endline "jetstream_consumers: ok"))

let () =
  try Eio_main.run run with
  | Failure message ->
      prerr_endline ("server JetStream consumers failed: " ^ message);
      exit 1
  | error ->
      prerr_endline
        ("server JetStream consumers failed: " ^ Printexc.to_string error);
      exit 1
