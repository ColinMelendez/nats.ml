package main

import (
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

const orderedReconnectWait = 90 * time.Second

func expectOrderedReconnectDelivery(label string, message *nats.Msg, stream, subject, payload, interop, trace string, streamSequence, consumerSequence uint64) error {
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
	if metadata.Stream != stream {
		return fmt.Errorf("%s metadata named stream %q, expected %q", label, metadata.Stream, stream)
	}
	if metadata.Sequence.Stream != streamSequence {
		return fmt.Errorf("%s metadata had stream sequence=%d, expected %d", label, metadata.Sequence.Stream, streamSequence)
	}
	if consumerSequence == 0 {
		if metadata.Sequence.Consumer == 0 {
			return fmt.Errorf("%s metadata had no consumer sequence", label)
		}
	} else if metadata.Sequence.Consumer != consumerSequence {
		return fmt.Errorf("%s metadata had consumer sequence=%d, expected %d", label, metadata.Sequence.Consumer, consumerSequence)
	}
	if metadata.NumDelivered != 1 {
		return fmt.Errorf("%s metadata had delivery count %d, expected one", label, metadata.NumDelivered)
	}
	return nil
}

func publishOrderedReconnectMessage(jetstream nats.JetStreamContext, subject, payload, interop, trace, stream string, sequence uint64) error {
	message := nats.NewMsg(subject)
	message.Data = []byte(payload)
	message.Header.Set("X-Interop", interop)
	message.Header.Set("X-Trace", trace)
	ack, err := jetstream.PublishMsg(message)
	if err != nil {
		return fmt.Errorf("publish %s: %w", payload, err)
	}
	return expectPublishAck("publish "+payload, ack, stream, false, sequence)
}

func runJetStreamOrderedReconnectPeer(config options) error {
	authOptions, err := connectOptions()
	if err != nil {
		return err
	}
	reconnected := make(chan struct{}, 1)
	connectionOptions := []nats.Option{
		nats.Name("ocaml-nats JetStream ordered reconnect interop peer"),
		nats.DontRandomize(),
		nats.ReconnectWait(500 * time.Millisecond),
		nats.MaxReconnects(-1),
		nats.ReconnectHandler(func(*nats.Conn) {
			select {
			case reconnected <- struct{}{}:
			default:
			}
		}),
	}
	connectionOptions = append(connectionOptions, authOptions...)
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
		Storage:  nats.FileStorage,
		Replicas: 3,
	})
	if err != nil {
		return fmt.Errorf("create replicated stream: %w", err)
	}
	if streamInfo.Config.Name != config.stream {
		return fmt.Errorf("created stream %q, expected %q", streamInfo.Config.Name, config.stream)
	}
	if streamInfo.Config.Storage != nats.FileStorage || streamInfo.Config.Replicas != 3 {
		return fmt.Errorf("created stream storage=%v replicas=%d, expected file-backed with three replicas", streamInfo.Config.Storage, streamInfo.Config.Replicas)
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
	defer orderedSubscription.Unsubscribe()
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush ordered setup: %w", err)
	}
	orderedInfo, err := orderedSubscription.ConsumerInfo()
	if err != nil {
		return fmt.Errorf("read Go ordered consumer info: %w", err)
	}
	if orderedInfo.Stream != config.stream || orderedInfo.Config.FilterSubject != matchSubject {
		return fmt.Errorf("Go ordered consumer was not bound to the expected stream and filter")
	}
	if orderedInfo.Config.AckPolicy != nats.AckNonePolicy || !orderedInfo.Config.MemoryStorage {
		return fmt.Errorf("Go ordered consumer did not use no-ack memory storage")
	}

	startMessages := make(chan *nats.Msg, 1)
	baselineMessages := make(chan *nats.Msg, 1)
	recoveryMessages := make(chan *nats.Msg, 1)
	closeMessages := make(chan *nats.Msg, 1)
	cleanupMessages := make(chan *nats.Msg, 1)
	controls := []struct {
		subject  string
		messages chan *nats.Msg
		name     string
	}{
		{config.prefix + ".start", startMessages, "start"},
		{config.prefix + ".baseline", baselineMessages, "baseline"},
		{config.prefix + ".recovery-ready", recoveryMessages, "recovery readiness"},
		{config.prefix + ".close", closeMessages, "close"},
		{config.prefix + ".cleanup", cleanupMessages, "cleanup"},
	}
	for _, control := range controls {
		control := control
		if _, err := connection.Subscribe(control.subject, func(message *nats.Msg) {
			control.messages <- message
		}); err != nil {
			return fmt.Errorf("subscribe %s: %w", control.name, err)
		}
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush control setup: %w", err)
	}
	if err := os.WriteFile(config.ready, []byte("ready\n"), 0600); err != nil {
		return fmt.Errorf("write ready file: %w", err)
	}

	startMessage, err := waitJetStreamMessage("start request", startMessages)
	if err != nil {
		return err
	}
	if err := startMessage.Respond([]byte("started")); err != nil {
		return fmt.Errorf("respond to start: %w", err)
	}
	if err := publishOrderedReconnectMessage(jetstream, matchSubject, "go-before-one", "go-ordered-reconnect", "go-before-one", config.stream, 1); err != nil {
		return err
	}
	if err := publishOrderedReconnectMessage(jetstream, config.prefix+".gap", "go-gap", "go-ordered-reconnect", "go-gap", config.stream, 2); err != nil {
		return err
	}
	if err := publishOrderedReconnectMessage(jetstream, matchSubject, "go-before-three", "go-ordered-reconnect", "go-before-three", config.stream, 3); err != nil {
		return err
	}
	for _, expected := range []struct {
		label            string
		payload          string
		trace            string
		streamSequence   uint64
		consumerSequence uint64
	}{{"Go ordered first", "go-before-one", "go-before-one", 1, 1}, {"Go ordered second", "go-before-three", "go-before-three", 3, 2}} {
		message, err := orderedSubscription.NextMsg(orderedReconnectWait)
		if err != nil {
			return fmt.Errorf("receive %s: %w", expected.label, err)
		}
		if err := expectOrderedReconnectDelivery(expected.label, message, config.stream, matchSubject, expected.payload, "go-ordered-reconnect", expected.trace, expected.streamSequence, expected.consumerSequence); err != nil {
			return err
		}
	}

	baselineMessage, err := waitJetStreamMessage("baseline completion", baselineMessages)
	if err != nil {
		return err
	}
	if string(baselineMessage.Data) != "baseline-complete" {
		return fmt.Errorf("baseline completion was %q, expected %q", string(baselineMessage.Data), "baseline-complete")
	}
	if err := baselineMessage.Respond([]byte("go-baseline-ready")); err != nil {
		return fmt.Errorf("respond baseline completion: %w", err)
	}

	if err := waitForJetStreamReconnectSignal(config.signal); err != nil {
		return err
	}
	if err := waitJetStreamReconnect(reconnected); err != nil {
		return err
	}
	recoveryDeadline := time.Now().Add(orderedReconnectWait)
	streamInfo, err = retryJetStreamStreamInfo(jetstream, config.stream, recoveryDeadline)
	if err != nil {
		return err
	}
	if streamInfo.Config.Storage != nats.FileStorage || streamInfo.Config.Replicas != 3 || streamInfo.State.Msgs != 3 || streamInfo.State.LastSeq != 3 {
		return fmt.Errorf("stream after failover storage=%v replicas=%d messages=%d last-sequence=%d", streamInfo.Config.Storage, streamInfo.Config.Replicas, streamInfo.State.Msgs, streamInfo.State.LastSeq)
	}

	recoveryMessage, err := waitJetStreamMessage("OCaml recovery readiness", recoveryMessages)
	if err != nil {
		return err
	}
	if string(recoveryMessage.Data) != "ocaml-reconnected" {
		return fmt.Errorf("recovery readiness was %q, expected %q", string(recoveryMessage.Data), "ocaml-reconnected")
	}
	if err := recoveryMessage.Respond([]byte("go-recovery-ready")); err != nil {
		return fmt.Errorf("respond recovery readiness: %w", err)
	}
	if err := publishOrderedReconnectMessage(jetstream, matchSubject, "go-after-four", "go-ordered-reconnect", "go-after-four", config.stream, 4); err != nil {
		return err
	}
	afterMessage, err := orderedSubscription.NextMsg(orderedReconnectWait)
	if err != nil {
		return fmt.Errorf("receive post-failover ordered message: %w", err)
	}
	if err := expectOrderedReconnectDelivery("Go ordered post-failover", afterMessage, config.stream, matchSubject, "go-after-four", "go-ordered-reconnect", "go-after-four", 4, 0); err != nil {
		return err
	}
	if err := connection.Publish(config.prefix+".go-after-done", []byte("go-after-complete")); err != nil {
		return fmt.Errorf("publish post-failover completion: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush post-failover completion: %w", err)
	}

	closeMessage, err := waitJetStreamMessage("OCaml close request", closeMessages)
	if err != nil {
		return err
	}
	if string(closeMessage.Data) != "ocaml-close" {
		return fmt.Errorf("close request was %q, expected %q", string(closeMessage.Data), "ocaml-close")
	}
	if err := orderedSubscription.Unsubscribe(); err != nil {
		return fmt.Errorf("unsubscribe Go ordered consumer: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush Go ordered unsubscribe: %w", err)
	}
	if err := closeMessage.Respond([]byte("go-closed")); err != nil {
		return fmt.Errorf("respond Go ordered close: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush Go ordered close response: %w", err)
	}

	cleanupMessage, err := waitJetStreamMessage("cleanup request", cleanupMessages)
	if err != nil {
		return err
	}
	if string(cleanupMessage.Data) != "cleanup" {
		return fmt.Errorf("cleanup request was %q, expected %q", string(cleanupMessage.Data), "cleanup")
	}
	if err := cleanupMessage.Respond([]byte("cleaned")); err != nil {
		return fmt.Errorf("respond cleanup: %w", err)
	}
	if err := connection.Drain(); err != nil {
		return fmt.Errorf("drain: %w", err)
	}
	return nil
}
