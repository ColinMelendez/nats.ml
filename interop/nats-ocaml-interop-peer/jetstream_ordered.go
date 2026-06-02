package main

import (
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

const orderedWait = 30 * time.Second

func waitOrderedMessage(label string, messages <-chan *nats.Msg) (*nats.Msg, error) {
	timer := time.NewTimer(orderedWait)
	defer timer.Stop()
	select {
	case message := <-messages:
		return message, nil
	case <-timer.C:
		return nil, fmt.Errorf("timed out waiting for %s", label)
	}
}

func checkOrderedConsumerInfo(label string, info *nats.ConsumerInfo, stream, filter string) error {
	if info.Stream != stream {
		return fmt.Errorf("%s named stream %q, expected %q", label, info.Stream, stream)
	}
	if info.Config.FilterSubject != filter {
		return fmt.Errorf("%s filtered %q, expected %q", label, info.Config.FilterSubject, filter)
	}
	if info.Config.AckPolicy != nats.AckNonePolicy {
		return fmt.Errorf("%s used ack policy %q, expected AckNone", label, info.Config.AckPolicy)
	}
	if !info.Config.FlowControl {
		return fmt.Errorf("%s did not enable flow control", label)
	}
	if info.Config.Heartbeat <= 0 {
		return fmt.Errorf("%s did not configure an idle heartbeat", label)
	}
	if info.Config.MaxDeliver != 1 {
		return fmt.Errorf("%s allowed %d deliveries, expected one", label, info.Config.MaxDeliver)
	}
	if !info.Config.MemoryStorage {
		return fmt.Errorf("%s did not use memory storage", label)
	}
	if info.Config.Replicas != 1 {
		return fmt.Errorf("%s used %d replicas, expected one", label, info.Config.Replicas)
	}
	return nil
}

func expectOrderedDelivery(label string, message *nats.Msg, streamName, subject, consumer, payload, interop, trace string, streamSequence, consumerSequence uint64) error {
	if string(message.Data) != payload {
		return fmt.Errorf("%s payload was %q, expected %q", label, string(message.Data), payload)
	}
	if message.Subject != subject {
		return fmt.Errorf("%s subject was %q, expected %q", label, message.Subject, subject)
	}
	if err := checkHeaderValues(message, "X-Interop", []string{interop}); err != nil {
		return fmt.Errorf("%s: %w", label, err)
	}
	if err := checkHeaderValues(message, "X-Trace", []string{trace}); err != nil {
		return fmt.Errorf("%s: %w", label, err)
	}
	metadata, err := message.Metadata()
	if err != nil {
		return fmt.Errorf("%s metadata: %w", label, err)
	}
	if metadata.Stream != streamName || metadata.Consumer != consumer {
		return fmt.Errorf("%s metadata identified stream=%q consumer=%q", label, metadata.Stream, metadata.Consumer)
	}
	if metadata.Sequence.Stream != streamSequence || metadata.Sequence.Consumer != consumerSequence {
		return fmt.Errorf("%s metadata had stream sequence=%d consumer sequence=%d", label, metadata.Sequence.Stream, metadata.Sequence.Consumer)
	}
	if metadata.NumDelivered != 1 {
		return fmt.Errorf("%s metadata had delivery count %d, expected one", label, metadata.NumDelivered)
	}
	return nil
}

func publishOrderedMessage(jetstream nats.JetStreamContext, subject, payload, interop, trace, stream string, sequence uint64) error {
	message := nats.NewMsg(subject)
	message.Data = []byte(payload)
	message.Header.Set("X-Interop", interop)
	message.Header.Add("X-Trace", trace)
	ack, err := jetstream.PublishMsg(message)
	if err != nil {
		return fmt.Errorf("publish %s: %w", payload, err)
	}
	if err := expectPublishAck("publish "+payload, ack, stream, false, sequence); err != nil {
		return err
	}
	return nil
}

func runJetStreamOrderedPeer(config options) error {
	authOptions, err := connectOptions()
	if err != nil {
		return err
	}
	connectionOptions := append([]nats.Option{nats.Name("ocaml-nats JetStream ordered interop peer")}, authOptions...)
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
		Subjects: []string{config.prefix + ".match", config.prefix + ".gap"},
		Storage:  nats.MemoryStorage,
	})
	if err != nil {
		return fmt.Errorf("create stream: %w", err)
	}
	if streamInfo.Config.Name != config.stream {
		return fmt.Errorf("created stream %q, expected %q", streamInfo.Config.Name, config.stream)
	}

	matchSubject := config.prefix + ".match"
	orderedSubscription, err := jetstream.SubscribeSync(
		matchSubject,
		nats.BindStream(config.stream),
		nats.OrderedConsumer(),
	)
	if err != nil {
		return fmt.Errorf("create Go ordered consumer: %w", err)
	}
	orderedClosed := false
	defer func() {
		if !orderedClosed {
			_ = orderedSubscription.Unsubscribe()
		}
	}()
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush Go ordered consumer: %w", err)
	}
	orderedInfo, err := orderedSubscription.ConsumerInfo()
	if err != nil {
		return fmt.Errorf("read Go ordered consumer info: %w", err)
	}
	if err := checkOrderedConsumerInfo("Go ordered consumer", orderedInfo, config.stream, matchSubject); err != nil {
		return err
	}

	startMessages := make(chan *nats.Msg, 1)
	batchTwoMessages := make(chan *nats.Msg, 1)
	orderedClosedMessages := make(chan *nats.Msg, 1)
	cleanupMessages := make(chan *nats.Msg, 1)
	controlSubscriptions := []struct {
		subject  string
		messages chan *nats.Msg
		name     string
	}{
		{config.prefix + ".start", startMessages, "start"},
		{config.prefix + ".batch2", batchTwoMessages, "batch two"},
		{config.prefix + ".ordered-closed", orderedClosedMessages, "ordered close"},
		{config.prefix + ".cleanup", cleanupMessages, "cleanup"},
	}
	for _, control := range controlSubscriptions {
		if _, err := connection.Subscribe(control.subject, func(message *nats.Msg) {
			control.messages <- message
		}); err != nil {
			return fmt.Errorf("subscribe %s: %w", control.name, err)
		}
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush ordered setup: %w", err)
	}
	if err := os.WriteFile(config.ready, []byte("ready\n"), 0600); err != nil {
		return fmt.Errorf("write ready file: %w", err)
	}

	startMessage, err := waitOrderedMessage("start request", startMessages)
	if err != nil {
		return err
	}
	if err := startMessage.Respond([]byte("started")); err != nil {
		return fmt.Errorf("respond to start: %w", err)
	}
	if err := publishOrderedMessage(jetstream, matchSubject, "go-one", "go-ordered", "go-one", config.stream, 1); err != nil {
		return err
	}
	if err := publishOrderedMessage(jetstream, config.prefix+".gap", "go-gap-one", "go-ordered", "go-gap-one", config.stream, 2); err != nil {
		return err
	}
	if err := publishOrderedMessage(jetstream, matchSubject, "go-three", "go-ordered", "go-three", config.stream, 3); err != nil {
		return err
	}
	if err := publishOrderedMessage(jetstream, config.prefix+".gap", "go-gap-two", "go-ordered", "go-gap-two", config.stream, 4); err != nil {
		return err
	}
	if err := publishOrderedMessage(jetstream, matchSubject, "go-five", "go-ordered", "go-five", config.stream, 5); err != nil {
		return err
	}

	for _, expected := range []struct {
		label            string
		payload          string
		trace            string
		streamSequence   uint64
		consumerSequence uint64
	}{{"Go ordered first", "go-one", "go-one", 1, 1}, {"Go ordered second", "go-three", "go-three", 3, 2}, {"Go ordered third", "go-five", "go-five", 5, 3}} {
		message, err := orderedSubscription.NextMsg(orderedWait)
		if err != nil {
			return fmt.Errorf("receive %s: %w", expected.label, err)
		}
		if err := expectOrderedDelivery(expected.label, message, config.stream, matchSubject, orderedInfo.Name, expected.payload, "go-ordered", expected.trace, expected.streamSequence, expected.consumerSequence); err != nil {
			return err
		}
		if expected.consumerSequence == 1 {
			if err := message.Ack(); err != nats.ErrCantAckIfConsumerAckNone {
				return fmt.Errorf("Go ordered Ack returned %v, expected %v", err, nats.ErrCantAckIfConsumerAckNone)
			}
		}
	}

	batchTwoMessage, err := waitOrderedMessage("batch two request", batchTwoMessages)
	if err != nil {
		return err
	}
	if err := expectPayload("batch two request", batchTwoMessage, "batch2"); err != nil {
		return err
	}
	if err := batchTwoMessage.Respond([]byte("go-batch2-ready")); err != nil {
		return fmt.Errorf("respond to batch two request: %w", err)
	}

	for _, expected := range []struct {
		label            string
		payload          string
		trace            string
		streamSequence   uint64
		consumerSequence uint64
	}{{"Go ordered fourth", "ocaml-six", "ocaml-six", 6, 4}, {"Go ordered fifth", "ocaml-eight", "ocaml-eight", 8, 5}} {
		message, err := orderedSubscription.NextMsg(orderedWait)
		if err != nil {
			return fmt.Errorf("receive %s: %w", expected.label, err)
		}
		if err := expectOrderedDelivery(expected.label, message, config.stream, matchSubject, orderedInfo.Name, expected.payload, "ocaml-ordered", expected.trace, expected.streamSequence, expected.consumerSequence); err != nil {
			return err
		}
	}

	closeMessage, err := waitOrderedMessage("OCaml ordered close", orderedClosedMessages)
	if err != nil {
		return err
	}
	if err := expectPayload("OCaml ordered close", closeMessage, "ocaml-ordered-closed"); err != nil {
		return err
	}
	if err := orderedSubscription.Unsubscribe(); err != nil {
		return fmt.Errorf("unsubscribe Go ordered consumer: %w", err)
	}
	orderedClosed = true
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush Go ordered unsubscribe: %w", err)
	}
	if err := closeMessage.Respond([]byte("go-unsubscribe")); err != nil {
		return fmt.Errorf("respond to OCaml ordered close: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush ordered close response: %w", err)
	}
	done := nats.NewMsg(config.prefix + ".go-done")
	done.Data = []byte("go-ordered-unsubscribed")
	if err := connection.PublishMsg(done); err != nil {
		return fmt.Errorf("publish ordered completion: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush ordered completion: %w", err)
	}

	cleanupMessage, err := waitOrderedMessage("cleanup request", cleanupMessages)
	if err != nil {
		return err
	}
	if err := expectPayload("cleanup request", cleanupMessage, "cleanup"); err != nil {
		return err
	}
	if err := cleanupMessage.Respond([]byte("cleaned")); err != nil {
		return fmt.Errorf("respond to cleanup: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush cleanup response: %w", err)
	}
	if err := connection.Drain(); err != nil {
		return fmt.Errorf("drain: %w", err)
	}
	return nil
}
