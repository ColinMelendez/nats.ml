package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

const orderedReconnectWait = 90 * time.Second

func waitForJetStreamLeaderKillSignal(signal string) error {
	deadline := time.Now().Add(orderedReconnectWait)
	path := signal + ".killed"
	failurePath := signal + ".failed"
	for time.Now().Before(deadline) {
		if _, err := os.Stat(failurePath); err == nil {
			return fmt.Errorf("leader kill watcher failed (see %s)", failurePath)
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("check leader kill watcher failure: %w", err)
		}
		if _, err := os.Stat(path); err == nil {
			return nil
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("check leader kill signal: %w", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for leader kill signal %s", path)
}

func waitForJetStreamSurvivorFile(signal, path string) (string, error) {
	deadline := time.Now().Add(orderedReconnectWait)
	failurePath := signal + ".failed"
	for time.Now().Before(deadline) {
		if _, err := os.Stat(failurePath); err == nil {
			return "", fmt.Errorf("leader routing watcher failed (see %s)", failurePath)
		} else if !os.IsNotExist(err) {
			return "", fmt.Errorf("check leader routing watcher failure: %w", err)
		}
		data, err := os.ReadFile(path)
		if err == nil {
			endpoint := strings.TrimSpace(string(data))
			if endpoint != "" {
				return endpoint, nil
			}
		} else if !os.IsNotExist(err) {
			return "", fmt.Errorf("read JetStream survivor endpoints: %w", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	return "", fmt.Errorf("timed out waiting for JetStream survivor endpoints %s", path)
}

func retryJetStreamLeaderChange(jetstream nats.JetStreamContext, stream, previous string, deadline time.Time) (*nats.StreamInfo, error) {
	var lastError error
	for time.Now().Before(deadline) {
		info, err := jetstream.StreamInfo(stream, nats.MaxWait(jetStreamAttemptWait))
		if err == nil && info.Cluster != nil && info.Cluster.Leader != "" && info.Cluster.Leader != previous {
			return info, nil
		}
		if err != nil {
			lastError = err
		} else {
			lastError = fmt.Errorf("JetStream stream %s still had leader %q", stream, previous)
		}
		time.Sleep(250 * time.Millisecond)
	}
	if lastError == nil {
		return nil, fmt.Errorf("JetStream stream %s did not report a leader", stream)
	}
	return nil, fmt.Errorf("JetStream stream %s did not elect a new leader: %w", stream, lastError)
}

func markJetStreamInteropFailure(signal string, err error) error {
	if markerError := os.WriteFile(signal+".failed", []byte("failed\n"), 0600); markerError != nil {
		return fmt.Errorf("%w (write failure marker: %v)", err, markerError)
	}
	return err
}

func retryJetStreamStreamQuorum(jetstream nats.JetStreamContext, stream string, messages, lastSequence uint64, deadline time.Time) (*nats.StreamInfo, error) {
	var lastError error
	for time.Now().Before(deadline) {
		info, err := jetstream.StreamInfo(stream, nats.MaxWait(jetStreamAttemptWait))
		if err != nil {
			lastError = err
		} else if info.State.Msgs != messages || info.State.LastSeq != lastSequence {
			lastError = fmt.Errorf("JetStream stream %s state messages=%d last-sequence=%d, expected messages=%d last-sequence=%d", stream, info.State.Msgs, info.State.LastSeq, messages, lastSequence)
		} else if info.Cluster == nil || info.Cluster.Leader == "" {
			lastError = fmt.Errorf("JetStream stream %s did not identify a leader", stream)
		} else if len(info.Cluster.Replicas) != info.Config.Replicas-1 {
			lastError = fmt.Errorf("JetStream stream %s reported %d followers, expected %d", stream, len(info.Cluster.Replicas), info.Config.Replicas-1)
		} else {
			ready := true
			for _, replica := range info.Cluster.Replicas {
				if replica == nil || !replica.Current || replica.Offline || replica.Lag != 0 {
					ready = false
					break
				}
			}
			if ready {
				return info, nil
			}
			lastError = fmt.Errorf("JetStream stream %s followers are not current", stream)
		}
		time.Sleep(250 * time.Millisecond)
	}
	if lastError == nil {
		return nil, fmt.Errorf("JetStream stream %s did not report replication state", stream)
	}
	return nil, fmt.Errorf("JetStream stream %s did not reach replica quorum: %w", stream, lastError)
}

func retryAddJetStreamStream(jetstream nats.JetStreamContext, config *nats.StreamConfig, deadline time.Time) (*nats.StreamInfo, error) {
	var lastError error
	for time.Now().Before(deadline) {
		info, err := jetstream.AddStream(config, nats.MaxWait(jetStreamAttemptWait))
		if err == nil {
			return info, nil
		}
		lastError = err
		if info, infoError := jetstream.StreamInfo(config.Name, nats.MaxWait(jetStreamAttemptWait)); infoError == nil {
			return info, nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	if lastError == nil {
		return nil, fmt.Errorf("JetStream stream %s did not accept a create request", config.Name)
	}
	return nil, fmt.Errorf("JetStream stream %s did not become placeable: %w", config.Name, lastError)
}

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
	if metadata.Consumer == "" {
		return fmt.Errorf("%s metadata did not identify a consumer", label)
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

func expectOrderedReconnectConsumerProgress(label string, message *nats.Msg, previousConsumer string, stream, subject, payload, interop, trace string, streamSequence, survivingConsumerSequence uint64) error {
	metadata, err := message.Metadata()
	if err != nil {
		return fmt.Errorf("%s metadata: %w", label, err)
	}
	expectedConsumerSequence := uint64(1)
	if metadata.Consumer == previousConsumer {
		expectedConsumerSequence = survivingConsumerSequence
	}
	return expectOrderedReconnectDelivery(label, message, stream, subject, payload, interop, trace, streamSequence, expectedConsumerSequence)
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
	defer func() { connection.Close() }()

	jetstream, err := connection.JetStream(nats.MaxWait(orderedReconnectWait))
	if err != nil {
		return fmt.Errorf("JetStream context: %w", err)
	}
	var leaderProbe nats.JetStreamContext
	streamInfo, err := retryAddJetStreamStream(jetstream, &nats.StreamConfig{
		Name:     config.stream,
		Subjects: []string{config.prefix + ".match", config.prefix + ".gap"},
		Storage:  nats.FileStorage,
		Replicas: 3,
	}, time.Now().Add(orderedReconnectWait))
	if err != nil {
		return fmt.Errorf("create replicated stream: %w", err)
	}
	if streamInfo.Config.Name != config.stream {
		return fmt.Errorf("created stream %q, expected %q", streamInfo.Config.Name, config.stream)
	}
	if streamInfo.Config.Storage != nats.FileStorage || streamInfo.Config.Replicas != 3 {
		return fmt.Errorf("created stream storage=%v replicas=%d, expected file-backed with three replicas", streamInfo.Config.Storage, streamInfo.Config.Replicas)
	}
	leaderName := ""
	if config.leader != "" {
		leaderDeadline := time.Now().Add(orderedReconnectWait)
		streamInfo, err = retryJetStreamStreamInfo(jetstream, config.stream, leaderDeadline)
		if err != nil {
			return err
		}
		if streamInfo.Cluster == nil || streamInfo.Cluster.Leader == "" {
			return fmt.Errorf("created stream did not identify a JetStream leader")
		}
		leaderName = streamInfo.Cluster.Leader
		if err := os.WriteFile(config.leader, []byte(leaderName+"\n"), 0600); err != nil {
			return fmt.Errorf("write JetStream leader: %w", err)
		}
		survivorServer, err := waitForJetStreamSurvivorFile(config.signal, config.survivor)
		if err != nil {
			return err
		}
		connection.Close()
		connection, err = nats.Connect(survivorServer, connectionOptions...)
		if err != nil {
			return fmt.Errorf("connect to JetStream survivor %q: %w", survivorServer, err)
		}
		jetstream, err = connection.JetStream(nats.MaxWait(orderedReconnectWait))
		if err != nil {
			return fmt.Errorf("JetStream survivor context: %w", err)
		}
		leaderProbe, err = connection.JetStream()
		if err != nil {
			return fmt.Errorf("JetStream leader probe context: %w", err)
		}
		streamInfo, err = retryJetStreamStreamInfo(leaderProbe, config.stream, time.Now().Add(orderedReconnectWait))
		if err != nil {
			return err
		}
		if streamInfo.Cluster == nil || streamInfo.Cluster.Leader != leaderName {
			actualLeader := ""
			if streamInfo.Cluster != nil {
				actualLeader = streamInfo.Cluster.Leader
			}
			return markJetStreamInteropFailure(config.signal, fmt.Errorf("JetStream leader changed during setup from %q to %q", leaderName, actualLeader))
		}
	}

	matchSubject := config.prefix + ".match"
	orderedOptions := []nats.SubOpt{
		nats.BindStream(config.stream),
		nats.OrderedConsumer(),
	}
	orderedSubscription, err := jetstream.SubscribeSync(matchSubject, orderedOptions...)
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
	if orderedInfo.Name == "" {
		return fmt.Errorf("Go ordered consumer info did not identify a consumer")
	}
	if orderedInfo.Stream != config.stream || orderedInfo.Config.FilterSubject != matchSubject {
		return fmt.Errorf("Go ordered consumer was not bound to the expected stream and filter")
	}
	if orderedInfo.Config.AckPolicy != nats.AckNonePolicy || !orderedInfo.Config.MemoryStorage || orderedInfo.Config.Replicas != 1 {
		return fmt.Errorf("Go ordered consumer did not use one-replica no-ack memory storage")
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
	if _, err := retryJetStreamStreamQuorum(jetstream, config.stream, 3, 3, time.Now().Add(orderedReconnectWait)); err != nil {
		return err
	}
	// A quorum response can precede the final consumer/meta placement events.
	// Let those events settle before the harness removes the seed server.
	time.Sleep(3 * time.Second)
	if err := baselineMessage.Respond([]byte("go-baseline-ready")); err != nil {
		return fmt.Errorf("respond baseline completion: %w", err)
	}

	if err := waitForJetStreamReconnectSignal(config.signal); err != nil {
		return err
	}
	var recoveryDeadline time.Time
	if config.leader != "" {
		streamInfo, err = retryJetStreamStreamInfo(leaderProbe, config.stream, time.Now().Add(orderedReconnectWait))
		if err != nil {
			return markJetStreamInteropFailure(config.signal, err)
		}
		if streamInfo.Cluster == nil || streamInfo.Cluster.Leader != leaderName {
			actualLeader := ""
			if streamInfo.Cluster != nil {
				actualLeader = streamInfo.Cluster.Leader
			}
			return markJetStreamInteropFailure(config.signal, fmt.Errorf("JetStream leader before kill was %q, expected %q", actualLeader, leaderName))
		}
		if err := os.WriteFile(config.leader, []byte(leaderName+"\n"), 0600); err != nil {
			return markJetStreamInteropFailure(config.signal, fmt.Errorf("write current JetStream leader: %w", err))
		}
		if err := os.WriteFile(config.signal+".kill-ready", []byte("ready\n"), 0600); err != nil {
			return markJetStreamInteropFailure(config.signal, fmt.Errorf("write leader kill barrier: %w", err))
		}
		if err := waitForJetStreamLeaderKillSignal(config.signal); err != nil {
			return err
		}
		recoveryDeadline = time.Now().Add(orderedReconnectWait)
		streamInfo, err = retryJetStreamLeaderChange(leaderProbe, config.stream, leaderName, recoveryDeadline)
	} else if err := waitJetStreamReconnect(reconnected); err != nil {
		return err
	} else {
		recoveryDeadline = time.Now().Add(orderedReconnectWait)
	}
	if err != nil {
		if config.leader != "" {
			return markJetStreamInteropFailure(config.signal, err)
		}
		return err
	}
	if config.leader == "" {
		streamInfo, err = retryJetStreamStreamInfo(jetstream, config.stream, recoveryDeadline)
	}
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
	recoveryPayload := "ocaml-reconnected"
	if config.leader != "" {
		recoveryPayload = "ocaml-leader-failover"
	}
	if string(recoveryMessage.Data) != recoveryPayload {
		return fmt.Errorf("recovery readiness was %q, expected %q", string(recoveryMessage.Data), recoveryPayload)
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
	if err := expectOrderedReconnectConsumerProgress("Go ordered post-failover", afterMessage, orderedInfo.Name, config.stream, matchSubject, "go-after-four", "go-ordered-reconnect", "go-after-four", 4, 3); err != nil {
		return err
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
