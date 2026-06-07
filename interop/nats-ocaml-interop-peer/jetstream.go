package main

import (
	"fmt"
	"os"

	"github.com/nats-io/nats.go"
)

const (
	goConsumerName    = "GO_PULL"
	ocamlConsumerName = "OCAML_PULL"
)

func expectPublishAck(label string, ack *nats.PubAck, stream string, duplicate bool, sequence uint64) error {
	if ack.Stream != stream {
		return fmt.Errorf("%s named stream %q, expected %q", label, ack.Stream, stream)
	}
	if ack.Duplicate != duplicate {
		return fmt.Errorf("%s duplicate=%t, expected %t", label, ack.Duplicate, duplicate)
	}
	if ack.Sequence != sequence {
		return fmt.Errorf("%s sequence=%d, expected %d", label, ack.Sequence, sequence)
	}
	return nil
}

func runJetStreamPeer(config options) error {
	authOptions, err := connectOptions()
	if err != nil {
		return err
	}
	connectionOptions := append([]nats.Option{nats.Name("ocaml-nats JetStream interop peer")}, authOptions...)
	connection, err := nats.Connect(config.server, connectionOptions...)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer connection.Close()

	jetstream, err := connection.JetStream()
	if err != nil {
		return fmt.Errorf("JetStream context: %w", err)
	}
	streamInfo, err := jetstream.AddStream(&nats.StreamConfig{
		Name:     config.stream,
		Subjects: []string{config.prefix + ".go", config.prefix + ".ocaml"},
		Storage:  nats.MemoryStorage,
	})
	if err != nil {
		return fmt.Errorf("create stream: %w", err)
	}
	if streamInfo.Config.Name != config.stream {
		return fmt.Errorf("created stream %q, expected %q", streamInfo.Config.Name, config.stream)
	}
	if _, err := jetstream.AddConsumer(config.stream, &nats.ConsumerConfig{
		Durable:       goConsumerName,
		DeliverPolicy: nats.DeliverAllPolicy,
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: config.prefix + ".ocaml",
	}); err != nil {
		return fmt.Errorf("create Go consumer: %w", err)
	}
	if _, err := jetstream.AddConsumer(config.stream, &nats.ConsumerConfig{
		Durable:       ocamlConsumerName,
		DeliverPolicy: nats.DeliverAllPolicy,
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: config.prefix + ".go",
	}); err != nil {
		return fmt.Errorf("create OCaml consumer: %w", err)
	}
	goSubscription, err := jetstream.PullSubscribe(
		config.prefix+".ocaml", goConsumerName, nats.Bind(config.stream, goConsumerName))
	if err != nil {
		return fmt.Errorf("bind Go consumer: %w", err)
	}
	defer goSubscription.Unsubscribe()

	startMessages := make(chan *nats.Msg, 1)
	goAcknowledgements := make(chan *nats.Msg, 1)
	ordinaryDeleteChecks := make(chan *nats.Msg, 1)
	secureDeleteChecks := make(chan *nats.Msg, 1)
	_, err = connection.Subscribe(config.prefix+".start", func(message *nats.Msg) {
		startMessages <- message
	})
	if err != nil {
		return fmt.Errorf("subscribe start: %w", err)
	}
	_, err = connection.Subscribe(config.prefix+".go-acked", func(message *nats.Msg) {
		goAcknowledgements <- message
	})
	if err != nil {
		return fmt.Errorf("subscribe Go acknowledgement: %w", err)
	}
	_, err = connection.Subscribe(config.prefix+".delete-check", func(message *nats.Msg) {
		ordinaryDeleteChecks <- message
	})
	if err != nil {
		return fmt.Errorf("subscribe ordinary delete check: %w", err)
	}
	_, err = connection.Subscribe(config.prefix+".secure-delete-check", func(message *nats.Msg) {
		secureDeleteChecks <- message
	})
	if err != nil {
		return fmt.Errorf("subscribe secure delete check: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush setup: %w", err)
	}
	if err := os.WriteFile(config.ready, []byte("ready\n"), 0600); err != nil {
		return fmt.Errorf("write ready file: %w", err)
	}

	startMessage, err := waitMessage("start request", startMessages)
	if err != nil {
		return err
	}
	if err := startMessage.Respond([]byte("started")); err != nil {
		return fmt.Errorf("respond to start: %w", err)
	}

	goMessage := nats.NewMsg(config.prefix + ".go")
	goMessage.Data = []byte("from-go-jetstream")
	goMessage.Header.Set("X-Interop", "go-jetstream")
	goMessage.Header.Add("X-Trace", "go")
	goAck, err := jetstream.PublishMsg(goMessage, nats.MsgId("go-jetstream-message"))
	if err != nil {
		return fmt.Errorf("publish Go JetStream message: %w", err)
	}
	if err := expectPublishAck("Go publish", goAck, config.stream, false, 1); err != nil {
		return err
	}
	goDuplicateAck, err := jetstream.PublishMsg(goMessage, nats.MsgId("go-jetstream-message"))
	if err != nil {
		return fmt.Errorf("duplicate Go JetStream message: %w", err)
	}
	if err := expectPublishAck("duplicate Go publish", goDuplicateAck, config.stream, true, 1); err != nil {
		return err
	}

	acknowledgement, err := waitMessage("OCaml JetStream acknowledgement", goAcknowledgements)
	if err != nil {
		return err
	}
	if string(acknowledgement.Data) != "go-message-acked" {
		return fmt.Errorf("OCaml acknowledgement payload was %q, expected %q", string(acknowledgement.Data), "go-message-acked")
	}
	if err := acknowledgement.Respond([]byte("acknowledged")); err != nil {
		return fmt.Errorf("respond to OCaml acknowledgement: %w", err)
	}

	ordinaryDeleteCheck, err := waitMessage("ordinary delete check", ordinaryDeleteChecks)
	if err != nil {
		return err
	}
	if string(ordinaryDeleteCheck.Data) != "check" {
		return fmt.Errorf("ordinary delete check payload was %q, expected %q", string(ordinaryDeleteCheck.Data), "check")
	}
	if _, err := jetstream.GetMsg(config.stream, 1); err == nil {
		return fmt.Errorf("ordinary delete left stream sequence 1 readable")
	}
	secureAck, err := jetstream.Publish(config.prefix+".go", []byte("secure-delete"))
	if err != nil {
		return fmt.Errorf("publish secure-delete message: %w", err)
	}
	if err := expectPublishAck("secure-delete publish", secureAck, config.stream, false, 2); err != nil {
		return err
	}
	if err := ordinaryDeleteCheck.Respond([]byte("secure-ready")); err != nil {
		return fmt.Errorf("respond to ordinary delete check: %w", err)
	}

	secureDeleteCheck, err := waitMessage("secure delete check", secureDeleteChecks)
	if err != nil {
		return err
	}
	if string(secureDeleteCheck.Data) != "check" {
		return fmt.Errorf("secure delete check payload was %q, expected %q", string(secureDeleteCheck.Data), "check")
	}
	if _, err := jetstream.GetMsg(config.stream, 2); err == nil {
		return fmt.Errorf("secure delete left stream sequence 2 readable")
	}
	if err := secureDeleteCheck.Respond([]byte("secure-deleted")); err != nil {
		return fmt.Errorf("respond to secure delete check: %w", err)
	}

	messages, err := goSubscription.Fetch(1, nats.MaxWait(waitTimeout))
	if err != nil {
		return fmt.Errorf("fetch OCaml JetStream message: %w", err)
	}
	if len(messages) != 1 {
		return fmt.Errorf("Go fetch returned %d messages, expected one", len(messages))
	}
	ocamlMessage := messages[0]
	if string(ocamlMessage.Data) != "from-ocaml-jetstream" {
		return fmt.Errorf("OCaml JetStream payload was %q, expected %q", string(ocamlMessage.Data), "from-ocaml-jetstream")
	}
	if ocamlMessage.Header.Get("X-Interop") != "ocaml-jetstream" {
		return fmt.Errorf("OCaml JetStream X-Interop header was %q", ocamlMessage.Header.Get("X-Interop"))
	}
	if err := checkHeaderValues(ocamlMessage, "X-Trace", []string{"ocaml"}); err != nil {
		return err
	}
	metadata, err := ocamlMessage.Metadata()
	if err != nil {
		return fmt.Errorf("OCaml JetStream metadata: %w", err)
	}
	if metadata.Stream != config.stream || metadata.Consumer != goConsumerName {
		return fmt.Errorf("OCaml metadata identified stream=%q consumer=%q", metadata.Stream, metadata.Consumer)
	}
	if metadata.Sequence.Stream != 3 || metadata.Sequence.Consumer != 1 || metadata.NumPending != 0 {
		return fmt.Errorf("OCaml metadata had stream sequence=%d consumer sequence=%d pending=%d", metadata.Sequence.Stream, metadata.Sequence.Consumer, metadata.NumPending)
	}
	if err := ocamlMessage.AckSync(); err != nil {
		return fmt.Errorf("acknowledge OCaml JetStream message: %w", err)
	}
	if err := goSubscription.Unsubscribe(); err != nil {
		return fmt.Errorf("unsubscribe Go consumer: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush after Go acknowledgement: %w", err)
	}
	done := nats.NewMsg(config.prefix + ".done")
	done.Data = []byte("go-message-acked")
	if err := connection.PublishMsg(done); err != nil {
		return fmt.Errorf("publish completion: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush completion: %w", err)
	}
	if err := connection.Drain(); err != nil {
		return fmt.Errorf("drain: %w", err)
	}
	return nil
}
