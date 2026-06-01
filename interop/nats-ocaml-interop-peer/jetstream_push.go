package main

import (
	"fmt"
	"os"

	"github.com/nats-io/nats.go"
)

const (
	goPushConsumerName    = "GO_PUSH"
	ocamlPushConsumerName = "OCAML_PUSH"
)

func checkPushConsumerInfo(label string, info *nats.ConsumerInfo, stream, name, filter, delivery string) error {
	if info.Stream != stream {
		return fmt.Errorf("%s named stream %q, expected %q", label, info.Stream, stream)
	}
	if info.Name != name {
		return fmt.Errorf("%s named consumer %q, expected %q", label, info.Name, name)
	}
	if info.Config.FilterSubject != filter {
		return fmt.Errorf("%s filtered %q, expected %q", label, info.Config.FilterSubject, filter)
	}
	if info.Config.DeliverSubject != delivery {
		return fmt.Errorf("%s delivered to %q, expected %q", label, info.Config.DeliverSubject, delivery)
	}
	if info.Config.AckPolicy != nats.AckExplicitPolicy {
		return fmt.Errorf("%s did not use explicit acknowledgements", label)
	}
	return nil
}

func expectPushDelivery(label string, message *nats.Msg, stream, consumer, payload, interop, trace string, streamSequence, consumerSequence uint64) error {
	if string(message.Data) != payload {
		return fmt.Errorf("%s payload was %q, expected %q", label, string(message.Data), payload)
	}
	if message.Header.Get("X-Interop") != interop {
		return fmt.Errorf("%s X-Interop header was %q, expected %q", label, message.Header.Get("X-Interop"), interop)
	}
	if err := checkHeaderValues(message, "X-Trace", []string{trace}); err != nil {
		return err
	}
	metadata, err := message.Metadata()
	if err != nil {
		return fmt.Errorf("%s metadata: %w", label, err)
	}
	if metadata.Stream != stream || metadata.Consumer != consumer {
		return fmt.Errorf("%s metadata identified stream=%q consumer=%q", label, metadata.Stream, metadata.Consumer)
	}
	if metadata.Sequence.Stream != streamSequence || metadata.Sequence.Consumer != consumerSequence {
		return fmt.Errorf("%s metadata had stream sequence=%d consumer sequence=%d", label, metadata.Sequence.Stream, metadata.Sequence.Consumer)
	}
	if metadata.NumDelivered != 1 || metadata.NumPending != 0 {
		return fmt.Errorf("%s metadata had delivered=%d pending=%d", label, metadata.NumDelivered, metadata.NumPending)
	}
	return nil
}

func runJetStreamPushPeer(config options) error {
	authOptions, err := connectOptions()
	if err != nil {
		return err
	}
	connectionOptions := append([]nats.Option{nats.Name("ocaml-nats JetStream push interop peer")}, authOptions...)
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
	goDelivery := config.prefix + ".deliver.go"
	ocamlDelivery := config.prefix + ".deliver.ocaml"
	goConsumerInfo, err := jetstream.AddConsumer(config.stream, &nats.ConsumerConfig{
		Durable:        goPushConsumerName,
		DeliverSubject: goDelivery,
		DeliverPolicy:  nats.DeliverAllPolicy,
		AckPolicy:      nats.AckExplicitPolicy,
		FilterSubject:  config.prefix + ".ocaml",
	})
	if err != nil {
		return fmt.Errorf("create Go push consumer: %w", err)
	}
	if err := checkPushConsumerInfo("Go push consumer", goConsumerInfo, config.stream, goPushConsumerName, config.prefix+".ocaml", goDelivery); err != nil {
		return err
	}
	ocamlConsumerInfo, err := jetstream.AddConsumer(config.stream, &nats.ConsumerConfig{
		Durable:        ocamlPushConsumerName,
		DeliverSubject: ocamlDelivery,
		DeliverPolicy:  nats.DeliverAllPolicy,
		AckPolicy:      nats.AckExplicitPolicy,
		FilterSubject:  config.prefix + ".go",
	})
	if err != nil {
		return fmt.Errorf("create OCaml push consumer: %w", err)
	}
	if err := checkPushConsumerInfo("OCaml push consumer", ocamlConsumerInfo, config.stream, ocamlPushConsumerName, config.prefix+".go", ocamlDelivery); err != nil {
		return err
	}

	// Push delivery is interest-driven: bind the Go leg before publishing or
	// signalling readiness so the first OCaml publication cannot race setup.
	goSubscription, err := jetstream.SubscribeSync(config.prefix+".ocaml", nats.Bind(config.stream, goPushConsumerName))
	if err != nil {
		return fmt.Errorf("bind Go push consumer: %w", err)
	}
	defer goSubscription.Unsubscribe()

	startMessages := make(chan *nats.Msg, 1)
	goAcknowledgements := make(chan *nats.Msg, 1)
	_, err = connection.Subscribe(config.prefix+".start", func(message *nats.Msg) {
		startMessages <- message
	})
	if err != nil {
		return fmt.Errorf("subscribe start: %w", err)
	}
	_, err = connection.Subscribe(config.prefix+".go-push-acked", func(message *nats.Msg) {
		goAcknowledgements <- message
	})
	if err != nil {
		return fmt.Errorf("subscribe OCaml push acknowledgement: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush push setup: %w", err)
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
	goMessage.Data = []byte("from-go-jetstream-push")
	goMessage.Header.Set("X-Interop", "go-jetstream-push")
	goMessage.Header.Add("X-Trace", "go-push")
	goAck, err := jetstream.PublishMsg(goMessage)
	if err != nil {
		return fmt.Errorf("publish Go JetStream push message: %w", err)
	}
	if err := expectPublishAck("Go push publish", goAck, config.stream, false, 1); err != nil {
		return err
	}

	acknowledgement, err := waitMessage("OCaml push acknowledgement", goAcknowledgements)
	if err != nil {
		return err
	}
	if string(acknowledgement.Data) != "go-push-message-acked" {
		return fmt.Errorf("OCaml push acknowledgement payload was %q, expected %q", string(acknowledgement.Data), "go-push-message-acked")
	}
	ocamlConsumerInfo, err = jetstream.ConsumerInfo(config.stream, ocamlPushConsumerName)
	if err != nil {
		return fmt.Errorf("read OCaml push consumer after acknowledgement: %w", err)
	}
	if ocamlConsumerInfo.NumAckPending != 0 {
		return fmt.Errorf("OCaml push consumer retained %d pending acknowledgements", ocamlConsumerInfo.NumAckPending)
	}
	if err := acknowledgement.Respond([]byte("acknowledged")); err != nil {
		return fmt.Errorf("respond to OCaml push acknowledgement: %w", err)
	}

	goMessage, err = goSubscription.NextMsg(waitTimeout)
	if err != nil {
		return fmt.Errorf("receive OCaml JetStream push message: %w", err)
	}
	if err := expectPushDelivery("OCaml JetStream push message", goMessage, config.stream, goPushConsumerName, "from-ocaml-jetstream-push", "ocaml-jetstream-push", "ocaml-push", 2, 1); err != nil {
		return err
	}
	if err := goMessage.AckSync(); err != nil {
		return fmt.Errorf("acknowledge OCaml JetStream push message: %w", err)
	}
	goConsumerInfo, err = jetstream.ConsumerInfo(config.stream, goPushConsumerName)
	if err != nil {
		return fmt.Errorf("read Go push consumer after acknowledgement: %w", err)
	}
	if goConsumerInfo.NumAckPending != 0 {
		return fmt.Errorf("Go push consumer retained %d pending acknowledgements (delivered consumer=%d stream=%d pending=%d)", goConsumerInfo.NumAckPending, goConsumerInfo.Delivered.Consumer, goConsumerInfo.Delivered.Stream, goConsumerInfo.NumPending)
	}
	if err := goSubscription.Unsubscribe(); err != nil {
		return fmt.Errorf("unsubscribe Go push consumer: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush after Go push acknowledgement: %w", err)
	}
	done := nats.NewMsg(config.prefix + ".done")
	done.Data = []byte("go-push-message-acked")
	if err := connection.PublishMsg(done); err != nil {
		return fmt.Errorf("publish push completion: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush push completion: %w", err)
	}
	if err := connection.Drain(); err != nil {
		return fmt.Errorf("drain: %w", err)
	}
	return nil
}
