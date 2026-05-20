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

let expect_header_ok = function
  | Ok value -> value
  | Error error -> failf "invalid test header: %a" Nats.Header.pp_error error

let expect_subject_reply delivery =
  match Nats.Message.reply_to delivery.Nats_eio.Subscription.message with
  | Some subject -> subject
  | None -> failf "request responder received no reply subject"

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
  match (Sys.getenv_opt "NATS_TEST_USER", Sys.getenv_opt "NATS_TEST_PASS") with
  | None, None -> None
  | Some user, Some pass when safe_credential user && safe_credential pass ->
      Some (Nats.Auth.user_pass ~user ~pass)
  | Some _, Some _ ->
      failf
        "NATS_TEST_USER and NATS_TEST_PASS must be non-empty ASCII letters, \
         digits, underscores, or hyphens"
  | _ ->
      failf
        "NATS_TEST_USER and NATS_TEST_PASS must both be non-empty or both unset"

let next_with_timeout ~clock ~timeout subscription =
  let timeout = Mtime.Span.to_float_ns timeout /. 1e9 in
  Eio.Fiber.first
    (fun () -> Nats_eio.Subscription.next subscription)
    (fun () ->
      Eio.Time.Mono.sleep clock timeout;
      Error Nats_eio.Error.Timeout)

let collect_queue_payloads ~sw ~clock ~timeout ~expected worker_one worker_two =
  let payloads = Eio.Stream.create expected in
  let collect subscription =
    let finished = ref false in
    while not !finished do
      match Nats_eio.Subscription.next subscription with
      | Ok delivery ->
          Eio.Stream.add payloads (Nats.Message.payload delivery.message)
      | Error Nats_eio.Error.Closed -> finished := true
      | Error error -> failf "queue delivery: %s" (error_message error)
    done
  in
  Eio.Fiber.fork ~sw (fun () -> collect worker_one);
  Eio.Fiber.fork ~sw (fun () -> collect worker_two);
  let received = ref [] in
  Eio.Fiber.first
    (fun () ->
      for _ = 1 to expected do
        received := Eio.Stream.take payloads :: !received
      done)
    (fun () ->
      Eio.Time.Mono.sleep clock (Mtime.Span.to_float_ns timeout /. 1e9);
      failf "queue group timed out after receiving %d of %d deliveries"
        (List.length !received) expected);
  List.rev !received

let expect_headers delivery expected =
  let actual = Nats.Header.find_all "x-trace" (Nats.Message.headers delivery) in
  if not (List.equal String.equal actual expected) then
    failf "header values were [%s]" (String.concat ", " actual)

let safe_identifier value =
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

let contains_substring ~needle value =
  let needle_length = String.length needle in
  let value_length = String.length value in
  if Int.equal needle_length 0 then true
  else
    let found = ref false in
    let last_position = value_length - needle_length in
    for position = 0 to last_position do
      if String.equal (String.sub value position needle_length) needle then
        found := true
    done;
    !found

let run_jetstream ~sw ~client ~timeout =
  let jetstream =
    expect_jetstream_ok "jetstream" (Nats_eio.Jetstream.v client)
  in
  let run_id =
    match Sys.getenv_opt "NATS_TEST_JETSTREAM_RUN_ID" with
    | Some value when safe_identifier value -> value
    | _ -> "direct"
  in
  let stream_name = "OCAML_TEST_STREAM_" ^ run_id in
  let subject = Nats.Subject.literal "ocaml.integration.js.events" in
  let filter = Nats.Subject.Filter.literal "ocaml.integration.js.events" in
  let config =
    expect_jetstream_config_ok "jetstream config"
      (Nats_eio.Jetstream.Stream.Config.v ~name:stream_name ~subjects:[ filter ]
         ~storage:Nats_eio.Jetstream.Stream.Config.Memory ())
  in
  let stream =
    expect_jetstream_ok "jetstream stream create"
      (Nats_eio.Jetstream.Stream.create jetstream config)
  in
  let deleted = ref false in
  Fun.protect
    ~finally:(fun () ->
      if not !deleted then
        match Nats_eio.Jetstream.Stream.delete stream with
        | Ok () -> ()
        | Error error ->
            prerr_endline
              (Format.asprintf "JetStream cleanup failed: %a"
                 Nats_eio.Jetstream.Error.pp error))
    (fun () ->
      let initial_info =
        expect_jetstream_ok "jetstream initial stream info"
          (Nats_eio.Jetstream.Stream.info stream)
      in
      if
        not
          (Int64.equal
             (Nats_eio.Jetstream.Stream.Info.messages initial_info)
             0L)
      then failf "new JetStream stream was not empty";
      let raw_stream_name = stream_name ^ "_RAW" in
      let raw_stream_subject =
        Nats.Subject.literal "ocaml.integration.js.raw"
      in
      let raw_stream_created = ref false in
      let raw_stream_deleted = ref false in
      Fun.protect
        ~finally:(fun () ->
          if !raw_stream_created && not !raw_stream_deleted then
            match
              Nats_eio.Jetstream.Stream.bind jetstream ~name:raw_stream_name
            with
            | Error error ->
                prerr_endline
                  (Format.asprintf "raw JetStream cleanup bind failed: %a"
                     Nats_eio.Jetstream.Error.pp error)
            | Ok raw_stream -> (
                match Nats_eio.Jetstream.Stream.delete raw_stream with
                | Ok () -> ()
                | Error error ->
                    prerr_endline
                      (Format.asprintf "raw JetStream cleanup failed: %a"
                         Nats_eio.Jetstream.Error.pp error)))
        (fun () ->
          let raw_create_subject =
            Nats.Subject.literal ("$JS.API.STREAM.CREATE." ^ raw_stream_name)
          in
          let raw_config =
            Format.sprintf
              {|{"name":%S,"subjects":[%S],"storage":"memory","retention":"limits","discard":"old","description":"preserve-me"}|}
              raw_stream_name
              (Nats.Subject.to_string raw_stream_subject)
          in
          let raw_create_response =
            expect_ok "raw JetStream stream create"
              (Nats_eio.Connection.request ~timeout client raw_create_subject
                 raw_config)
          in
          raw_stream_created := true;
          let raw_create_payload = Nats.Message.payload raw_create_response in
          if
            (not
               (contains_substring ~needle:"\"description\"" raw_create_payload))
            || not (contains_substring ~needle:"preserve-me" raw_create_payload)
          then
            failf "raw JetStream stream create lost its description: %s"
              raw_create_payload;
          let raw_stream =
            expect_jetstream_ok "raw JetStream stream bind"
              (Nats_eio.Jetstream.Stream.bind jetstream ~name:raw_stream_name)
          in
          let raw_current_info =
            expect_jetstream_ok "raw JetStream stream info before update"
              (Nats_eio.Jetstream.Stream.info raw_stream)
          in
          let raw_update_config =
            expect_jetstream_config_ok "raw JetStream stream update config"
              (Nats_eio.Jetstream.Stream.Config.with_max_msgs
                 (Nats_eio.Jetstream.Stream.Info.config raw_current_info)
                 (Some 10_000L))
          in
          let raw_updated_info =
            expect_jetstream_ok "raw JetStream stream update"
              (Nats_eio.Jetstream.Stream.update raw_stream raw_update_config)
          in
          (match
             Nats_eio.Jetstream.Stream.Config.max_msgs
               (Nats_eio.Jetstream.Stream.Info.config raw_updated_info)
           with
          | Some value when Int64.equal value 10_000L -> ()
          | Some value -> failf "raw stream update applied max_msgs=%Ld" value
          | None -> failf "raw stream update lost max_msgs");
          let raw_info_subject =
            Nats.Subject.literal ("$JS.API.STREAM.INFO." ^ raw_stream_name)
          in
          let raw_info_response =
            expect_ok "raw JetStream stream info"
              (Nats_eio.Connection.request ~timeout client raw_info_subject "")
          in
          let raw_info_payload = Nats.Message.payload raw_info_response in
          if
            (not
               (contains_substring ~needle:"\"description\"" raw_info_payload))
            || not (contains_substring ~needle:"preserve-me" raw_info_payload)
          then
            failf "stream update lost an unmodeled description: %s"
              raw_info_payload;
          expect_jetstream_ok "raw JetStream stream delete"
            (Nats_eio.Jetstream.Stream.delete raw_stream);
          raw_stream_deleted := true);
      let updated_config =
        expect_jetstream_config_ok "jetstream stream update config"
          (Nats_eio.Jetstream.Stream.Config.v ~name:stream_name
             ~subjects:[ filter ]
             ~storage:Nats_eio.Jetstream.Stream.Config.Memory ~max_msgs:10_000L
             ())
      in
      let updated_info =
        expect_jetstream_ok "jetstream stream update"
          (Nats_eio.Jetstream.Stream.update stream updated_config)
      in
      (match
         Nats_eio.Jetstream.Stream.Config.max_msgs
           (Nats_eio.Jetstream.Stream.Info.config updated_info)
       with
      | Some value when Int64.equal value 10_000L -> ()
      | Some value -> failf "stream update applied max_msgs=%Ld" value
      | None -> failf "stream update lost max_msgs");
      let stream_infos =
        expect_jetstream_ok "jetstream stream list"
          (Nats_eio.Jetstream.Stream.list jetstream)
      in
      if
        not
          (List.exists
             (fun info ->
               String.equal
                 (Nats_eio.Jetstream.Stream.Config.name
                    (Nats_eio.Jetstream.Stream.Info.config info))
                 stream_name)
             stream_infos)
      then failf "stream list did not include the created stream";
      let filtered_stream_infos =
        expect_jetstream_ok "jetstream filtered stream list"
          (Nats_eio.Jetstream.Stream.list ~subject:filter jetstream)
      in
      if
        not
          (List.exists
             (fun info ->
               String.equal
                 (Nats_eio.Jetstream.Stream.Config.name
                    (Nats_eio.Jetstream.Stream.Info.config info))
                 stream_name)
             filtered_stream_infos)
      then failf "filtered stream list did not include the created stream";
      let mismatched_config =
        expect_jetstream_config_ok "jetstream mismatched update config"
          (Nats_eio.Jetstream.Stream.Config.v ~name:(stream_name ^ "_OTHER")
             ~subjects:[ filter ]
             ~storage:Nats_eio.Jetstream.Stream.Config.Memory ())
      in
      (match Nats_eio.Jetstream.Stream.update stream mismatched_config with
      | Error
          (Nats_eio.Jetstream.Error.Unexpected_stream_name { expected; actual })
        when String.equal expected stream_name
             && String.equal actual (stream_name ^ "_OTHER") ->
          ()
      | Ok _ -> failf "mismatched stream update unexpectedly succeeded"
      | Error error ->
          failf "mismatched stream update: %s" (jetstream_error_message error));
      let first_ack =
        expect_jetstream_ok "jetstream publish"
          (Nats_eio.Jetstream.publish ~timeout ~msg_id:"integration-message-1"
             jetstream subject "hello")
      in
      if
        not
          (String.equal
             (Nats_eio.Jetstream.Publish_ack.stream first_ack)
             stream_name)
      then failf "JetStream publish ack named the wrong stream";
      if Nats_eio.Jetstream.Publish_ack.duplicate first_ack then
        failf "first JetStream publish was marked duplicate";
      let duplicate_ack =
        expect_jetstream_ok "jetstream duplicate publish"
          (Nats_eio.Jetstream.publish ~timeout ~msg_id:"integration-message-1"
             jetstream subject "hello")
      in
      if not (Nats_eio.Jetstream.Publish_ack.duplicate duplicate_ack) then
        failf "duplicate JetStream publish was not marked duplicate";
      if
        not
          (Int64.equal
             (Nats_eio.Jetstream.Publish_ack.sequence duplicate_ack)
             (Nats_eio.Jetstream.Publish_ack.sequence first_ack))
      then failf "duplicate JetStream publish changed its sequence";
      let final_info =
        expect_jetstream_ok "jetstream final stream info"
          (Nats_eio.Jetstream.Stream.info stream)
      in
      if
        not
          (Int64.equal (Nats_eio.Jetstream.Stream.Info.messages final_info) 1L)
      then failf "JetStream stream retained the wrong message count";
      if
        not
          (Int64.equal
             (Nats_eio.Jetstream.Publish_ack.sequence first_ack)
             (Nats_eio.Jetstream.Stream.Info.last_sequence final_info))
      then failf "JetStream stream info disagreed with the publish ack";
      let consumer_name = "OCAML_TEST_CONSUMER_" ^ run_id in
      let consumer_config =
        expect_jetstream_config_ok "jetstream consumer config"
          (Nats_eio.Jetstream.Consumer.Config.v ~durable_name:consumer_name
             ~filter_subject:filter ())
      in
      let consumer =
        expect_jetstream_ok "jetstream consumer create"
          (Nats_eio.Jetstream.Consumer.create stream consumer_config)
      in
      let consumer_deleted = ref false in
      Fun.protect
        ~finally:(fun () ->
          if not !consumer_deleted then
            match Nats_eio.Jetstream.Consumer.delete consumer with
            | Ok () -> ()
            | Error error ->
                prerr_endline
                  (Format.asprintf "JetStream consumer cleanup failed: %a"
                     Nats_eio.Jetstream.Error.pp error))
        (fun () ->
          let consumer_info =
            expect_jetstream_ok "jetstream consumer info"
              (Nats_eio.Jetstream.Consumer.info consumer)
          in
          if
            not
              (String.equal
                 (Nats_eio.Jetstream.Consumer.Info.name consumer_info)
                 consumer_name)
          then failf "JetStream consumer info named the wrong consumer";
          if
            not
              (String.equal
                 (Nats_eio.Jetstream.Consumer.Info.stream_name consumer_info)
                 stream_name)
          then failf "JetStream consumer info named the wrong stream";
          let consumer_infos =
            expect_jetstream_ok "jetstream consumer list"
              (Nats_eio.Jetstream.Consumer.list stream)
          in
          if
            not
              (List.exists
                 (fun info ->
                   String.equal
                     (Nats_eio.Jetstream.Consumer.Info.name info)
                     consumer_name)
                 consumer_infos)
          then failf "consumer list did not include the created consumer";
          let info_config =
            Nats_eio.Jetstream.Consumer.Info.config consumer_info
          in
          if
            not
              (match
                 Nats_eio.Jetstream.Consumer.Config.filter_subject info_config
               with
              | Some value ->
                  String.equal
                    (Nats.Subject.Filter.to_string value)
                    (Nats.Subject.Filter.to_string filter)
              | None -> false)
          then failf "JetStream consumer info lost its filter subject";
          if
            not
              (match
                 Nats_eio.Jetstream.Consumer.Config.durable_name info_config
               with
              | Some value -> String.equal value consumer_name
              | None -> false)
          then failf "JetStream consumer info lost its durable name";
          if
            not
              (match
                 Nats_eio.Jetstream.Consumer.Config.ack_policy info_config
               with
              | Nats_eio.Jetstream.Consumer.Config.Explicit -> true
              | _ -> false)
          then failf "JetStream consumer info changed its ack policy";
          if
            not
              (match
                 Nats_eio.Jetstream.Consumer.Config.max_deliver info_config
               with
              | None -> true
              | Some _ -> false)
          then failf "JetStream consumer info exposed an unlimited max deliver";
          if
            not
              (match
                 Nats_eio.Jetstream.Consumer.Config.deliver_policy info_config
               with
              | Nats_eio.Jetstream.Consumer.Config.All -> true
              | _ -> false)
          then failf "JetStream consumer info changed its deliver policy";
          if
            not
              (Int64.equal
                 (Nats_eio.Jetstream.Consumer.Info.num_pending consumer_info)
                 1L)
          then failf "JetStream consumer info reported the wrong pending count";
          let one_message label = function
            | [ message ] -> message
            | messages ->
                failf "%s returned %d messages"
                  label (List.length messages)
          in
          let first_message =
            one_message "JetStream fetch"
              (expect_jetstream_ok "jetstream fetch"
                 (Nats_eio.Jetstream.Consumer.fetch consumer ~batch:1))
          in
          if
            not
              (String.equal
                 (Nats_eio.Jetstream.Msg.payload first_message)
                 "hello")
          then failf "JetStream fetch returned the wrong payload";
          if
            not
              (Int64.equal
                 (Nats_eio.Jetstream.Msg.stream_sequence first_message)
                 1L)
          then failf "JetStream fetch returned the wrong stream sequence";
          if
            not
              (Int64.equal
                 (Nats_eio.Jetstream.Msg.num_delivered first_message)
                 1L)
          then failf "JetStream fetch returned the wrong delivery count";
          if
            Int64.compare
              (Nats_eio.Jetstream.Msg.timestamp first_message)
              0L
            <= 0
          then failf "JetStream fetch returned an invalid timestamp";
          expect_jetstream_ok "JetStream ack"
            (Nats_eio.Jetstream.Msg.ack first_message);
          (match
             Nats_eio.Jetstream.Consumer.fetch
               ~expires:Mtime.Span.(100 * ms)
               ~idle_heartbeat:Mtime.Span.(100 * ms)
               consumer ~batch:1
           with
          | Error Nats_eio.Jetstream.Error.Idle_heartbeat_expires_too_short ->
              ()
          | Ok messages ->
              failf "invalid JetStream heartbeat request returned %d messages"
                (List.length messages)
          | Error error ->
              failf "invalid JetStream heartbeat request: %s"
                (jetstream_error_message error));
          (match
             expect_jetstream_ok "empty JetStream heartbeat fetch"
               (Nats_eio.Jetstream.Consumer.fetch
                  ~expires:Mtime.Span.(500 * ms)
                  ~idle_heartbeat:Mtime.Span.(100 * ms)
                  consumer ~batch:1)
           with
          | [] -> ()
          | messages ->
              failf "empty JetStream heartbeat fetch returned %d messages"
                (List.length messages));
          (match
             expect_jetstream_ok "empty JetStream fetch"
               (Nats_eio.Jetstream.Consumer.fetch
                  ~expires:Mtime.Span.(1 * ms)
                  consumer ~batch:1)
           with
          | [] -> ()
          | messages ->
              failf "empty JetStream fetch returned %d messages"
                (List.length messages));
          let second_ack =
            expect_jetstream_ok "second JetStream publish"
              (Nats_eio.Jetstream.publish ~timeout ~msg_id:"integration-message-2"
                 jetstream subject "world")
          in
          if Nats_eio.Jetstream.Publish_ack.duplicate second_ack then
            failf "second JetStream publish was marked duplicate";
          let second_message =
            one_message "JetStream NAK fetch"
              (expect_jetstream_ok "JetStream NAK fetch"
                 (Nats_eio.Jetstream.Consumer.fetch consumer ~batch:1))
          in
          expect_jetstream_ok "JetStream NAK"
            (Nats_eio.Jetstream.Msg.nak second_message);
          let redelivered_message =
            one_message "JetStream redelivery"
              (expect_jetstream_ok "JetStream redelivery"
                 (Nats_eio.Jetstream.Consumer.fetch consumer ~batch:1))
          in
          if
            not
              (Int64.equal
                 (Nats_eio.Jetstream.Msg.num_delivered redelivered_message)
                 2L)
          then failf "JetStream NAK did not cause a redelivery";
          expect_jetstream_ok "JetStream redelivery ack"
            (Nats_eio.Jetstream.Msg.ack redelivered_message);
          let max_bytes_ack =
            expect_jetstream_ok "max-bytes JetStream publish"
              (Nats_eio.Jetstream.publish ~timeout
                 ~msg_id:"integration-message-max-bytes" jetstream subject
                 "large")
          in
          if Nats_eio.Jetstream.Publish_ack.duplicate max_bytes_ack then
            failf "max-bytes JetStream publish was marked duplicate";
          (match
             expect_jetstream_ok "max-bytes JetStream fetch"
               (Nats_eio.Jetstream.Consumer.fetch
                  ~expires:Mtime.Span.(250 * ms)
                  ~max_bytes:1 consumer ~batch:1)
           with
          | [] -> ()
          | messages ->
              failf "max-bytes JetStream fetch returned %d messages"
                (List.length messages));
          let max_bytes_message =
            one_message "max-bytes JetStream redelivery"
              (expect_jetstream_ok "max-bytes JetStream redelivery"
                 (Nats_eio.Jetstream.Consumer.fetch consumer ~batch:1))
          in
          if
            not
              (String.equal
                 (Nats_eio.Jetstream.Msg.payload max_bytes_message)
                 "large")
          then failf "max-bytes JetStream fetch lost the pending message";
          expect_jetstream_ok "max-bytes JetStream ack"
            (Nats_eio.Jetstream.Msg.ack max_bytes_message);
          let with_pull ?batch ?expires ?idle_heartbeat ?max_bytes label f =
            let pull =
              expect_jetstream_ok (label ^ " create")
                (Nats_eio.Jetstream.Consumer.Pull.v ~sw ?batch ?expires
                   ?idle_heartbeat ?max_bytes consumer)
            in
            Fun.protect
              ~finally:(fun () ->
                match Nats_eio.Jetstream.Consumer.Pull.close pull with
                | Ok () -> ()
                | Error error ->
                    failf "%s close: %s" label (jetstream_error_message error))
              (fun () -> f pull)
          in
          let pull_one_ack =
            expect_jetstream_ok "JetStream first pull publish"
              (Nats_eio.Jetstream.publish ~timeout
                 ~msg_id:"integration-message-pull-1" jetstream subject
                 "pull-one")
          in
          if Nats_eio.Jetstream.Publish_ack.duplicate pull_one_ack then
            failf "first pull publish was marked duplicate";
          let pull_two_ack =
            expect_jetstream_ok "JetStream second pull publish"
              (Nats_eio.Jetstream.publish ~timeout
                 ~msg_id:"integration-message-pull-2" jetstream subject
                 "pull-two")
          in
          if Nats_eio.Jetstream.Publish_ack.duplicate pull_two_ack then
            failf "second pull publish was marked duplicate";
          with_pull "JetStream single-message pull" (fun pull ->
              let first =
                expect_jetstream_ok "JetStream first pull"
                  (Nats_eio.Jetstream.Consumer.Pull.next pull)
              in
              if
                not
                  (String.equal
                     (Nats_eio.Jetstream.Msg.payload first)
                     "pull-one")
              then failf "first pull returned the wrong payload";
              expect_jetstream_ok "JetStream first pull ack"
                (Nats_eio.Jetstream.Msg.ack first);
              let second =
                expect_jetstream_ok "JetStream second pull"
                  (Nats_eio.Jetstream.Consumer.Pull.next pull)
              in
              if
                not
                  (String.equal
                     (Nats_eio.Jetstream.Msg.payload second)
                     "pull-two")
              then failf "second pull returned the wrong payload";
              expect_jetstream_ok "JetStream second pull ack"
                (Nats_eio.Jetstream.Msg.ack second));
          let pull_three_ack =
            expect_jetstream_ok "JetStream third pull publish"
              (Nats_eio.Jetstream.publish ~timeout
                 ~msg_id:"integration-message-pull-3" jetstream subject
                 "pull-three")
          in
          if Nats_eio.Jetstream.Publish_ack.duplicate pull_three_ack then
            failf "third pull publish was marked duplicate";
          let pull_four_ack =
            expect_jetstream_ok "JetStream fourth pull publish"
              (Nats_eio.Jetstream.publish ~timeout
                 ~msg_id:"integration-message-pull-4" jetstream subject
                 "pull-four")
          in
          if Nats_eio.Jetstream.Publish_ack.duplicate pull_four_ack then
            failf "fourth pull publish was marked duplicate";
          with_pull ~batch:2 "JetStream batched pull" (fun pull ->
              let first =
                expect_jetstream_ok "JetStream batched first pull"
                  (Nats_eio.Jetstream.Consumer.Pull.next pull)
              in
              let second =
                expect_jetstream_ok "JetStream batched second pull"
                  (Nats_eio.Jetstream.Consumer.Pull.next pull)
              in
              if
                not
                  (String.equal
                     (Nats_eio.Jetstream.Msg.payload first)
                     "pull-three")
              then failf "batched first pull returned the wrong payload";
              if
                not
                  (String.equal
                     (Nats_eio.Jetstream.Msg.payload second)
                     "pull-four")
              then failf "batched second pull returned the wrong payload";
              expect_jetstream_ok "JetStream batched first pull ack"
                (Nats_eio.Jetstream.Msg.ack first);
              expect_jetstream_ok "JetStream batched second pull ack"
                (Nats_eio.Jetstream.Msg.ack second));
          with_pull
            ~expires:Mtime.Span.(1 * s)
            "JetStream timed pull"
            (fun pull ->
              (match
                 Nats_eio.Jetstream.Consumer.Pull.next_with_timeout
                   ~timeout:Mtime.Span.(50 * ms)
                   pull
               with
              | Error
                  (Nats_eio.Jetstream.Error.Connection Nats_eio.Error.Timeout)
                ->
                  ()
              | Ok _ -> failf "empty timed pull unexpectedly returned a message"
              | Error error ->
                  failf "empty timed pull: %s" (jetstream_error_message error));
              let timeout_ack =
                expect_jetstream_ok "JetStream post-timeout pull publish"
                  (Nats_eio.Jetstream.publish ~timeout
                     ~msg_id:"integration-message-pull-timeout" jetstream
                     subject "pull-after-timeout")
              in
              if Nats_eio.Jetstream.Publish_ack.duplicate timeout_ack then
                failf "post-timeout pull publish was marked duplicate";
              let message =
                expect_jetstream_ok "JetStream post-timeout pull"
                  (Nats_eio.Jetstream.Consumer.Pull.next pull)
              in
              if
                not
                  (String.equal
                     (Nats_eio.Jetstream.Msg.payload message)
                     "pull-after-timeout")
              then failf "post-timeout pull returned the wrong payload";
              expect_jetstream_ok "JetStream post-timeout pull ack"
                (Nats_eio.Jetstream.Msg.ack message));
          with_pull
            ~expires:Mtime.Span.(1 * s)
            ~idle_heartbeat:Mtime.Span.(100 * ms)
            "JetStream heartbeat pull"
            (fun pull ->
              (match
                 Nats_eio.Jetstream.Consumer.Pull.next_with_timeout
                   ~timeout:Mtime.Span.(350 * ms)
                   pull
               with
              | Error
                  (Nats_eio.Jetstream.Error.Connection Nats_eio.Error.Timeout)
                ->
                  ()
              | Ok _ -> failf "heartbeat pull unexpectedly returned a message"
              | Error Nats_eio.Jetstream.Error.Missing_heartbeat ->
                  failf "heartbeat pull missed an idle heartbeat"
              | Error error ->
                  failf "heartbeat pull timeout: %s"
                    (jetstream_error_message error));
              let heartbeat_ack =
                expect_jetstream_ok "JetStream heartbeat pull publish"
                  (Nats_eio.Jetstream.publish ~timeout
                     ~msg_id:"integration-message-pull-heartbeat" jetstream
                     subject "pull-after-heartbeat")
              in
              if Nats_eio.Jetstream.Publish_ack.duplicate heartbeat_ack then
                failf "heartbeat pull publish was marked duplicate";
              let message =
                expect_jetstream_ok "JetStream heartbeat pull message"
                  (Nats_eio.Jetstream.Consumer.Pull.next pull)
              in
              if
                not
                  (String.equal
                     (Nats_eio.Jetstream.Msg.payload message)
                     "pull-after-heartbeat")
              then failf "heartbeat pull returned the wrong payload";
              expect_jetstream_ok "JetStream heartbeat pull ack"
                (Nats_eio.Jetstream.Msg.ack message));
          with_pull
            ~expires:Mtime.Span.(1 * ms)
            "JetStream server-expiring pull"
            (fun pull ->
              (match
                 Nats_eio.Jetstream.Consumer.Pull.next_with_timeout
                   ~timeout:Mtime.Span.(100 * ms)
                   pull
               with
              | Error
                  (Nats_eio.Jetstream.Error.Connection Nats_eio.Error.Timeout)
                ->
                  ()
              | Ok _ ->
                  failf "server-expiring pull unexpectedly returned a message"
              | Error error ->
                  failf "server-expiring pull: %s"
                    (jetstream_error_message error));
              let server_expiring_ack =
                expect_jetstream_ok "JetStream server-expiring pull publish"
                  (Nats_eio.Jetstream.publish ~timeout
                     ~msg_id:"integration-message-pull-server-expiring"
                     jetstream subject "pull-after-server-expiry")
              in
              if Nats_eio.Jetstream.Publish_ack.duplicate server_expiring_ack
              then failf "server-expiring pull publish was marked duplicate";
              let message =
                expect_jetstream_ok "JetStream server-expiring pull message"
                  (Nats_eio.Jetstream.Consumer.Pull.next pull)
              in
              if
                not
                  (String.equal
                     (Nats_eio.Jetstream.Msg.payload message)
                     "pull-after-server-expiry")
              then failf "server-expiring pull returned the wrong payload";
              expect_jetstream_ok "JetStream server-expiring pull ack"
                (Nats_eio.Jetstream.Msg.ack message));
          let pull_max_bytes_ack =
            expect_jetstream_ok "JetStream pull max-bytes publish"
              (Nats_eio.Jetstream.publish ~timeout
                 ~msg_id:"integration-message-pull-max-bytes" jetstream subject
                 "pull-max-bytes")
          in
          if Nats_eio.Jetstream.Publish_ack.duplicate pull_max_bytes_ack then
            failf "pull max-bytes publish was marked duplicate";
          with_pull ~max_bytes:1 "JetStream max-bytes pull" (fun pull ->
              match Nats_eio.Jetstream.Consumer.Pull.next pull with
              | Error
                  (Nats_eio.Jetstream.Error.Conflict { code = 409; _ })
                ->
                  ()
              | Ok _ -> failf "max-bytes pull returned an oversized message"
              | Error error ->
                  failf "max-bytes pull: %s" (jetstream_error_message error));
          let pull_max_bytes_message =
            one_message "JetStream pull max-bytes redelivery"
              (expect_jetstream_ok "JetStream pull max-bytes redelivery"
                 (Nats_eio.Jetstream.Consumer.fetch consumer ~batch:1))
          in
          if
            not
              (String.equal
                 (Nats_eio.Jetstream.Msg.payload pull_max_bytes_message)
                 "pull-max-bytes")
          then failf "pull max-bytes redelivery returned the wrong payload";
          expect_jetstream_ok "JetStream pull max-bytes ack"
            (Nats_eio.Jetstream.Msg.ack pull_max_bytes_message);
          with_pull "JetStream closed pull" (fun pull ->
              expect_jetstream_ok "JetStream pull close"
                (Nats_eio.Jetstream.Consumer.Pull.close pull);
              (match Nats_eio.Jetstream.Consumer.Pull.next pull with
              | Error Nats_eio.Jetstream.Error.Pull_closed -> ()
              | Ok _ -> failf "closed pull returned a message"
              | Error error ->
                  failf "closed pull next: %s" (jetstream_error_message error));
              match
                Nats_eio.Jetstream.Consumer.Pull.iter pull ~f:(fun _ ->
                    failf "closed pull iter received a message")
              with
              | Ok () -> ()
              | Error error ->
                  failf "closed pull iter: %s" (jetstream_error_message error));
          let rebound =
            expect_jetstream_ok "jetstream consumer bind"
              (Nats_eio.Jetstream.Consumer.bind stream ~name:consumer_name)
          in
          let rebound_info =
            expect_jetstream_ok "jetstream rebound consumer info"
              (Nats_eio.Jetstream.Consumer.info rebound)
          in
          if
            not
              (String.equal
                 (Nats_eio.Jetstream.Consumer.Info.name rebound_info)
                 consumer_name)
          then failf "bound JetStream consumer named the wrong consumer";
          expect_jetstream_ok "jetstream consumer delete"
            (Nats_eio.Jetstream.Consumer.delete consumer);
          consumer_deleted := true;
          (match Nats_eio.Jetstream.Consumer.info consumer with
          | Error (Nats_eio.Jetstream.Error.Api _) -> ()
          | Ok _ -> failf "deleted JetStream consumer still existed"
          | Error error ->
              failf "deleted JetStream consumer info: %s"
                (jetstream_error_message error));
          print_endline "jetstream_consumer: ok");
      expect_jetstream_ok "jetstream stream delete"
        (Nats_eio.Jetstream.Stream.delete stream);
      deleted := true;
      print_endline "jetstream: ok")

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

let expect_auth_required ~sw ~net ~clock endpoint =
  match Nats_eio.Connection.connect ~sw ~net ~clock [ endpoint ] with
  | Error (Nats_eio.Error.Auth Nats.Auth.Auth_required) -> ()
  | Ok connection ->
      expect_ok "close anonymous connection"
        (Nats_eio.Connection.close connection);
      failf "anonymous connection succeeded against an auth server"
  | Error error -> failf "anonymous connection: %s" (error_message error)

let run env =
  Eio.Switch.run @@ fun sw ->
  let endpoint = endpoint () in
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.mono_clock env in
  let timeout = Mtime.Span.(2 * s) in
  let auth = auth () in
  let config =
    match auth with
    | None -> None
    | Some auth ->
        Some (expect_ok "auth config" (Nats_eio.Connection.Config.v ~auth ()))
  in
  let client = connect ~sw ~net ~clock ?config endpoint in
  let responder = connect ~sw ~net ~clock ?config endpoint in
  let worker_one = connect ~sw ~net ~clock ?config endpoint in
  let worker_two = connect ~sw ~net ~clock ?config endpoint in
  let events_subject = Nats.Subject.literal "ocaml.integration.events" in
  let events_filter = Nats.Subject.Filter.literal "ocaml.integration.events" in
  let subscription =
    expect_ok "subscribe" (Nats_eio.Connection.subscribe client events_filter)
  in
  expect_ok "subscribe flush" (Nats_eio.Connection.flush client);
  expect_ok "publish"
    (Nats_eio.Connection.publish responder events_subject "hello");
  expect_ok "publish flush" (Nats_eio.Connection.flush responder);
  let delivery =
    expect_ok "delivery" (next_with_timeout ~clock ~timeout subscription)
  in
  if not (String.equal (Nats.Message.payload delivery.message) "hello") then
    failf "delivery payload was %S" (Nats.Message.payload delivery.message);
  print_endline "pubsub: ok";
  let headers =
    expect_header_ok
      (Nats.Header.of_list [ ("X-Trace", "one"); ("x-trace", "two") ])
  in
  let headers_subject = Nats.Subject.literal "ocaml.integration.headers" in
  let headers_filter =
    Nats.Subject.Filter.literal "ocaml.integration.headers"
  in
  let headers_subscription =
    expect_ok "headers subscribe"
      (Nats_eio.Connection.subscribe client headers_filter)
  in
  expect_ok "headers subscribe flush" (Nats_eio.Connection.flush client);
  expect_ok "headers publish"
    (Nats_eio.Connection.publish responder ~headers headers_subject "payload");
  expect_ok "headers publish flush" (Nats_eio.Connection.flush responder);
  let headers_delivery =
    expect_ok "headers delivery"
      (next_with_timeout ~clock ~timeout headers_subscription)
  in
  if
    not (String.equal (Nats.Message.payload headers_delivery.message) "payload")
  then
    failf "header delivery payload was %S"
      (Nats.Message.payload headers_delivery.message);
  expect_headers headers_delivery.message [ "one"; "two" ];
  print_endline "headers: ok";
  let queue_subject = Nats.Subject.literal "ocaml.integration.queue" in
  let queue_filter = Nats.Subject.Filter.literal "ocaml.integration.queue" in
  let queue_group = Nats.Queue_group.literal "ocaml.integration.workers" in
  let worker_one_subscription =
    expect_ok "worker one subscribe"
      (Nats_eio.Connection.subscribe worker_one ~queue_group queue_filter)
  in
  let worker_two_subscription =
    expect_ok "worker two subscribe"
      (Nats_eio.Connection.subscribe worker_two ~queue_group queue_filter)
  in
  expect_ok "worker one subscribe flush" (Nats_eio.Connection.flush worker_one);
  expect_ok "worker two subscribe flush" (Nats_eio.Connection.flush worker_two);
  let queue_payloads = [ "one"; "two"; "three"; "four" ] in
  List.iter
    (fun payload ->
      expect_ok "queue publish"
        (Nats_eio.Connection.publish responder queue_subject payload))
    queue_payloads;
  expect_ok "queue publish flush" (Nats_eio.Connection.flush responder);
  let delivered_payloads =
    List.sort String.compare
      (collect_queue_payloads ~sw ~clock ~timeout
         ~expected:(List.length queue_payloads)
         worker_one_subscription worker_two_subscription)
  in
  let expected_payloads = List.sort String.compare queue_payloads in
  if not (List.equal String.equal delivered_payloads expected_payloads) then
    failf "queue group delivered [%s]" (String.concat ", " delivered_payloads);
  print_endline "queue_group: ok";
  let request_subject = Nats.Subject.literal "ocaml.integration.request" in
  let request_filter =
    Nats.Subject.Filter.literal "ocaml.integration.request"
  in
  let request_subscription =
    expect_ok "request subscribe"
      (Nats_eio.Connection.subscribe responder request_filter)
  in
  expect_ok "request subscribe flush" (Nats_eio.Connection.flush responder);
  let responder_done, responder_done_u = Eio.Promise.create () in
  Eio.Fiber.fork ~sw (fun () ->
      let result =
        match next_with_timeout ~clock ~timeout request_subscription with
        | Error error -> Error (error_message error)
        | Ok delivery -> (
            let reply_to = expect_subject_reply delivery in
            match Nats_eio.Connection.publish responder reply_to "pong" with
            | Ok () -> Ok ()
            | Error error -> Error (error_message error))
      in
      Eio.Promise.resolve responder_done_u result);
  let response =
    expect_ok "request"
      (Nats_eio.Connection.request ~timeout client request_subject "ping")
  in
  (match Eio.Promise.await responder_done with
  | Ok () -> ()
  | Error message -> failf "request responder: %s" message);
  if not (String.equal (Nats.Message.payload response) "pong") then
    failf "response payload was %S" (Nats.Message.payload response);
  print_endline "request: ok";
  let no_responder_subject =
    Nats.Subject.literal "ocaml.integration.no_responder"
  in
  (match
     Nats_eio.Connection.request ~timeout client no_responder_subject "ping"
   with
  | Error Nats_eio.Error.No_responders -> print_endline "no_responders: ok"
  | Ok response ->
      failf "no-responder request returned %S" (Nats.Message.payload response)
  | Error error -> failf "no-responder request: %s" (error_message error));
  expect_ok "flush" (Nats_eio.Connection.flush client);
  print_endline "flush: ok";
  (match Sys.getenv_opt "NATS_TEST_JETSTREAM" with
  | Some "1" -> run_jetstream ~sw ~client ~timeout
  | _ -> ());
  expect_ok "close responder" (Nats_eio.Connection.close responder);
  expect_ok "close worker one" (Nats_eio.Connection.close worker_one);
  expect_ok "close worker two" (Nats_eio.Connection.close worker_two);
  expect_ok "close client" (Nats_eio.Connection.close client);
  print_endline "close: ok";
  match auth with
  | None -> ()
  | Some _ ->
      expect_auth_required ~sw ~net ~clock endpoint;
      print_endline "auth: user_pass"

let () =
  try Eio_main.run run with
  | Failure message ->
      prerr_endline ("server acceptance failed: " ^ message);
      exit 1
  | error ->
      prerr_endline ("server acceptance failed: " ^ Printexc.to_string error);
      exit 1
