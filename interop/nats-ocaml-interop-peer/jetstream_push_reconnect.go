package main

import (
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

const jetStreamReconnectWait = 90 * time.Second
const jetStreamReconnectMessageWait = jetStreamReconnectWait

func waitJetStreamMessage(label string, messages <-chan *nats.Msg) (*nats.Msg, error) {
	timer := time.NewTimer(jetStreamReconnectMessageWait)
	defer timer.Stop()
	select {
	case message := <-messages:
		return message, nil
	case <-timer.C:
		return nil, fmt.Errorf("timed out waiting for %s", label)
	}
}

func waitJetStreamReconnect(messages <-chan struct{}) error {
	timer := time.NewTimer(jetStreamReconnectWait)
	defer timer.Stop()
	select {
	case <-messages:
		return nil
	case <-timer.C:
		return fmt.Errorf("timed out waiting for JetStream reconnect")
	}
}

func waitForJetStreamReconnectSignal(signal string) error {
	deadline := time.Now().Add(jetStreamReconnectWait)
	path := signal + ".1"
	failurePath := signal + ".failed"
	for time.Now().Before(deadline) {
		if _, err := os.Stat(failurePath); err == nil {
			return fmt.Errorf("restart watcher failed (see %s)", failurePath)
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("check restart watcher failure: %w", err)
		}
		if _, err := os.Stat(path); err == nil {
			return nil
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("check reconnect signal: %w", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for reconnect signal %s", path)
}

func retryJetStreamConsumerInfo(jetstream nats.JetStreamContext, stream, consumer string, deadline time.Time) (*nats.ConsumerInfo, error) {
	var lastError error
	for time.Now().Before(deadline) {
		info, err := jetstream.ConsumerInfo(stream, consumer)
		if err == nil {
			return info, nil
		}
		lastError = err
		time.Sleep(250 * time.Millisecond)
	}
	return nil, fmt.Errorf("JetStream consumer %s did not become ready: %w", consumer, lastError)
}

func retryJetStreamStreamInfo(jetstream nats.JetStreamContext, stream string, deadline time.Time) (*nats.StreamInfo, error) {
	var lastError error
	for time.Now().Before(deadline) {
		info, err := jetstream.StreamInfo(stream)
		if err == nil {
			return info, nil
		}
		lastError = err
		time.Sleep(250 * time.Millisecond)
	}
	return nil, fmt.Errorf("JetStream stream %s did not become ready: %w", stream, lastError)
}

func checkRecoveredConsumer(label string, info *nats.ConsumerInfo, stream, consumer, filter, delivery string, streamSequence, consumerSequence uint64) error {
	if err := checkPushConsumerInfo(label, info, stream, consumer, filter, delivery); err != nil {
		return err
	}
	if info.NumAckPending != 0 {
		return fmt.Errorf("%s retained %d pending acknowledgements", label, info.NumAckPending)
	}
	if info.AckFloor.Stream != streamSequence || info.AckFloor.Consumer != consumerSequence {
		return fmt.Errorf("%s ack floor stream=%d consumer=%d, expected stream=%d consumer=%d", label, info.AckFloor.Stream, info.AckFloor.Consumer, streamSequence, consumerSequence)
	}
	return nil
}

func runJetStreamPushReconnectPeer(config options) error {
	authOptions, err := connectOptions()
	if err != nil {
		return err
	}
	reconnected := make(chan struct{}, 1)
	connectionOptions := []nats.Option{
		nats.Name("ocaml-nats JetStream push reconnect interop peer"),
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
		Subjects: []string{config.prefix + ".go", config.prefix + ".ocaml"},
		Storage:  nats.FileStorage,
	})
	if err != nil {
		return fmt.Errorf("create stream: %w", err)
	}
	if streamInfo.Config.Name != config.stream {
		return fmt.Errorf("created stream %q, expected %q", streamInfo.Config.Name, config.stream)
	}
	if streamInfo.Config.Storage != nats.FileStorage {
		return fmt.Errorf("created stream was not file-backed")
	}
	goDelivery := config.prefix + ".deliver.go"
	ocamlDelivery := config.prefix + ".deliver.ocaml"
	goFilter := config.prefix + ".ocaml"
	ocamlFilter := config.prefix + ".go"
	goConsumerInfo, err := jetstream.AddConsumer(config.stream, &nats.ConsumerConfig{
		Durable:        goPushConsumerName,
		DeliverSubject: goDelivery,
		DeliverPolicy:  nats.DeliverAllPolicy,
		AckPolicy:      nats.AckExplicitPolicy,
		FilterSubject:  goFilter,
	})
	if err != nil {
		return fmt.Errorf("create Go push consumer: %w", err)
	}
	if err := checkPushConsumerInfo("Go push consumer", goConsumerInfo, config.stream, goPushConsumerName, goFilter, goDelivery); err != nil {
		return err
	}
	ocamlConsumerInfo, err := jetstream.AddConsumer(config.stream, &nats.ConsumerConfig{
		Durable:        ocamlPushConsumerName,
		DeliverSubject: ocamlDelivery,
		DeliverPolicy:  nats.DeliverAllPolicy,
		AckPolicy:      nats.AckExplicitPolicy,
		FilterSubject:  ocamlFilter,
	})
	if err != nil {
		return fmt.Errorf("create OCaml push consumer: %w", err)
	}
	if err := checkPushConsumerInfo("OCaml push consumer", ocamlConsumerInfo, config.stream, ocamlPushConsumerName, ocamlFilter, ocamlDelivery); err != nil {
		return err
	}

	goSubscription, err := jetstream.SubscribeSync(goFilter, nats.Bind(config.stream, goPushConsumerName))
	if err != nil {
		return fmt.Errorf("bind Go push consumer: %w", err)
	}
	defer goSubscription.Unsubscribe()

	startMessages := make(chan *nats.Msg, 1)
	goAcknowledgements := make(chan *nats.Msg, 2)
	recoveryVerificationMessages := make(chan *nats.Msg, 1)
	cleanupMessages := make(chan *nats.Msg, 1)
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
		return fmt.Errorf("subscribe OCaml acknowledgement: %w", err)
	}
	_, err = connection.Subscribe(config.prefix+".recovery-verified", func(message *nats.Msg) {
		recoveryVerificationMessages <- message
	})
	if err != nil {
		return fmt.Errorf("subscribe recovery verification: %w", err)
	}
	_, err = connection.Subscribe(config.prefix+".cleanup", func(message *nats.Msg) {
		cleanupMessages <- message
	})
	if err != nil {
		return fmt.Errorf("subscribe cleanup: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush push setup: %w", err)
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

	beforeGo := nats.NewMsg(config.prefix + ".go")
	beforeGo.Data = []byte("before-go")
	beforeGo.Header.Set("X-Interop", "go-jetstream-push-reconnect")
	beforeGo.Header.Add("X-Trace", "go-before")
	beforeGoAck, err := jetstream.PublishMsg(beforeGo)
	if err != nil {
		return fmt.Errorf("publish baseline Go message: %w", err)
	}
	if err := expectPublishAck("baseline Go publish", beforeGoAck, config.stream, false, 1); err != nil {
		return err
	}
	acknowledgement, err := waitJetStreamMessage("baseline OCaml acknowledgement", goAcknowledgements)
	if err != nil {
		return err
	}
	if string(acknowledgement.Data) != "before-go-acked" {
		return fmt.Errorf("baseline OCaml acknowledgement was %q, expected %q", string(acknowledgement.Data), "before-go-acked")
	}
	ocamlConsumerInfo, err = jetstream.ConsumerInfo(config.stream, ocamlPushConsumerName)
	if err != nil {
		return fmt.Errorf("read baseline OCaml consumer: %w", err)
	}
	if ocamlConsumerInfo.NumAckPending != 0 {
		return fmt.Errorf("baseline OCaml consumer retained %d pending acknowledgements", ocamlConsumerInfo.NumAckPending)
	}
	if err := acknowledgement.Respond([]byte("acknowledged")); err != nil {
		return fmt.Errorf("respond baseline acknowledgement: %w", err)
	}

	beforeOcaml, err := goSubscription.NextMsg(jetStreamReconnectMessageWait)
	if err != nil {
		return fmt.Errorf("receive baseline OCaml message: %w", err)
	}
	if err := expectPushDelivery("baseline OCaml message", beforeOcaml, config.stream, goPushConsumerName, "before-ocaml", "ocaml-jetstream-push-reconnect", "ocaml-before", 2, 1); err != nil {
		return err
	}
	if err := beforeOcaml.AckSync(); err != nil {
		return fmt.Errorf("acknowledge baseline OCaml message: %w", err)
	}
	goConsumerInfo, err = jetstream.ConsumerInfo(config.stream, goPushConsumerName)
	if err != nil {
		return fmt.Errorf("read baseline Go consumer: %w", err)
	}
	if err := checkRecoveredConsumer("baseline Go consumer", goConsumerInfo, config.stream, goPushConsumerName, goFilter, goDelivery, 2, 1); err != nil {
		return err
	}
	if err := connection.Publish(config.prefix+".before-done", []byte("baseline-complete")); err != nil {
		return fmt.Errorf("publish baseline completion: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush baseline completion: %w", err)
	}

	if err := waitForJetStreamReconnectSignal(config.signal); err != nil {
		return err
	}
	if err := waitJetStreamReconnect(reconnected); err != nil {
		return err
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush after reconnect: %w", err)
	}
	recoveryDeadline := time.Now().Add(jetStreamReconnectWait)
	streamInfo, err = retryJetStreamStreamInfo(jetstream, config.stream, recoveryDeadline)
	if err != nil {
		return err
	}
	if streamInfo.Config.Storage != nats.FileStorage || streamInfo.State.Msgs != 2 || streamInfo.State.LastSeq != 2 {
		return fmt.Errorf("stream after reconnect storage=%v messages=%d last-sequence=%d", streamInfo.Config.Storage, streamInfo.State.Msgs, streamInfo.State.LastSeq)
	}
	goConsumerInfo, err = retryJetStreamConsumerInfo(jetstream, config.stream, goPushConsumerName, recoveryDeadline)
	if err != nil {
		return err
	}
	if err := checkRecoveredConsumer("recovered Go consumer", goConsumerInfo, config.stream, goPushConsumerName, goFilter, goDelivery, 2, 1); err != nil {
		return err
	}
	ocamlConsumerInfo, err = retryJetStreamConsumerInfo(jetstream, config.stream, ocamlPushConsumerName, recoveryDeadline)
	if err != nil {
		return err
	}
	if err := checkRecoveredConsumer("recovered OCaml consumer", ocamlConsumerInfo, config.stream, ocamlPushConsumerName, ocamlFilter, ocamlDelivery, 1, 1); err != nil {
		return err
	}
	if err := awaitRecoveryBarrierWithTimeout(connection, config.prefix, 1, jetStreamReconnectWait); err != nil {
		return err
	}
	recoveryVerification, err := waitJetStreamMessage(
		"OCaml JetStream recovery verification", recoveryVerificationMessages)
	if err != nil {
		return err
	}
	if string(recoveryVerification.Data) != "ocaml-recovery-verified" {
		return fmt.Errorf("recovery verification was %q, expected %q", string(recoveryVerification.Data), "ocaml-recovery-verified")
	}
	if err := recoveryVerification.Respond([]byte("go-recovery-verified")); err != nil {
		return fmt.Errorf("respond recovery verification: %w", err)
	}

	afterGo := nats.NewMsg(config.prefix + ".go")
	afterGo.Data = []byte("after-go")
	afterGo.Header.Set("X-Interop", "go-jetstream-push-reconnect")
	afterGo.Header.Add("X-Trace", "go-after")
	afterGoAck, err := jetstream.PublishMsg(afterGo)
	if err != nil {
		return fmt.Errorf("publish post-reconnect Go message: %w", err)
	}
	if err := expectPublishAck("post-reconnect Go publish", afterGoAck, config.stream, false, 3); err != nil {
		return err
	}
	acknowledgement, err = waitJetStreamMessage("post-reconnect OCaml acknowledgement", goAcknowledgements)
	if err != nil {
		return err
	}
	if string(acknowledgement.Data) != "after-go-acked" {
		return fmt.Errorf("post-reconnect OCaml acknowledgement was %q, expected %q", string(acknowledgement.Data), "after-go-acked")
	}
	ocamlConsumerInfo, err = jetstream.ConsumerInfo(config.stream, ocamlPushConsumerName)
	if err != nil {
		return fmt.Errorf("read post-reconnect OCaml consumer: %w", err)
	}
	if ocamlConsumerInfo.NumAckPending != 0 {
		return fmt.Errorf("post-reconnect OCaml consumer retained %d pending acknowledgements", ocamlConsumerInfo.NumAckPending)
	}
	if err := acknowledgement.Respond([]byte("acknowledged")); err != nil {
		return fmt.Errorf("respond post-reconnect acknowledgement: %w", err)
	}

	afterOcaml, err := goSubscription.NextMsg(jetStreamReconnectMessageWait)
	if err != nil {
		return fmt.Errorf("receive post-reconnect OCaml message: %w", err)
	}
	if err := expectPushDelivery("post-reconnect OCaml message", afterOcaml, config.stream, goPushConsumerName, "after-ocaml", "ocaml-jetstream-push-reconnect", "ocaml-after", 4, 2); err != nil {
		return err
	}
	if err := afterOcaml.AckSync(); err != nil {
		return fmt.Errorf("acknowledge post-reconnect OCaml message: %w", err)
	}
	goConsumerInfo, err = jetstream.ConsumerInfo(config.stream, goPushConsumerName)
	if err != nil {
		return fmt.Errorf("read post-reconnect Go consumer: %w", err)
	}
	if err := checkRecoveredConsumer("post-reconnect Go consumer", goConsumerInfo, config.stream, goPushConsumerName, goFilter, goDelivery, 4, 2); err != nil {
		return err
	}
	if err := goSubscription.Unsubscribe(); err != nil {
		return fmt.Errorf("unsubscribe Go push consumer: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush before cleanup: %w", err)
	}
	if err := connection.Publish(config.prefix+".go-done", []byte("post-reconnect-complete")); err != nil {
		return fmt.Errorf("publish post-reconnect completion: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush post-reconnect completion: %w", err)
	}
	cleanup, err := waitJetStreamMessage("cleanup request", cleanupMessages)
	if err != nil {
		return err
	}
	if string(cleanup.Data) != "cleanup" {
		return fmt.Errorf("cleanup request payload was %q, expected %q", string(cleanup.Data), "cleanup")
	}
	if err := cleanup.Respond([]byte("cleaned")); err != nil {
		return fmt.Errorf("respond cleanup: %w", err)
	}
	if err := connection.Drain(); err != nil {
		return fmt.Errorf("drain: %w", err)
	}
	return nil
}
