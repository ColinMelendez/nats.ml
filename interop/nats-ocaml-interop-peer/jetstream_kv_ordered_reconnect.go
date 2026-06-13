package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	natsjetstream "github.com/nats-io/nats.go/jetstream"
)

const (
	kvOrderedReconnectHistory = 1
)

type keyValueControl struct {
	mu       sync.Mutex
	latest   *nats.Msg
	ready    chan struct{}
	complete bool
	response string
}

func newKeyValueControl() *keyValueControl {
	return &keyValueControl{ready: make(chan struct{}, 1)}
}

func (control *keyValueControl) receive(message *nats.Msg) {
	control.mu.Lock()
	if control.complete {
		response := control.response
		control.mu.Unlock()
		if message.Reply != "" {
			_ = message.Respond([]byte(response))
		}
		return
	}
	// A retry supersedes an older request; completed phases replay the cached
	// response to any later retry.
	control.latest = message
	control.mu.Unlock()
	select {
	case control.ready <- struct{}{}:
	default:
	}
}

func (control *keyValueControl) wait(label string) (*nats.Msg, error) {
	timer := time.NewTimer(waitTimeout)
	defer timer.Stop()
	for {
		select {
		case <-control.ready:
			control.mu.Lock()
			message := control.latest
			control.latest = nil
			control.mu.Unlock()
			if message != nil {
				return message, nil
			}
		case <-timer.C:
			return nil, fmt.Errorf("timed out waiting for %s", label)
		}
	}
}

func (control *keyValueControl) respond(connection *nats.Conn, label string, message *nats.Msg, payload string) error {
	control.mu.Lock()
	control.complete = true
	control.response = payload
	pending := control.latest
	control.latest = nil
	control.mu.Unlock()

	if err := respondKeyValueControl(connection, label, message, payload); err != nil {
		return err
	}
	if pending != nil && pending.Reply != "" {
		if err := pending.Respond([]byte(payload)); err != nil {
			return fmt.Errorf("respond to duplicate %s: %w", label, err)
		}
		if err := connection.Flush(); err != nil {
			return fmt.Errorf("flush duplicate response to %s: %w", label, err)
		}
	}
	return nil
}

func consumerGeneration(name, prefix string) (int, bool) {
	generationText, ok := strings.CutPrefix(name, prefix+"_")
	if !ok {
		return 0, false
	}
	generation, err := strconv.Atoi(generationText)
	if err != nil {
		return 0, false
	}
	return generation, true
}

func retryJetStreamConsumerGeneration(jetstream nats.JetStreamContext, stream, prefix string, minimumGeneration int, deadline time.Time) (*nats.ConsumerInfo, error) {
	var lastError error
	for time.Now().Before(deadline) {
		names := jetstream.ConsumerNames(stream, nats.MaxWait(jetStreamAttemptWait))
		if names == nil {
			lastError = fmt.Errorf("list consumers for stream %s returned no channel", stream)
		} else {
			latestName := ""
			latestGeneration := minimumGeneration
			for name := range names {
				generation, ok := consumerGeneration(name, prefix)
				if !ok || generation <= latestGeneration {
					continue
				}
				latestName = name
				latestGeneration = generation
			}
			if latestName != "" {
				info, err := jetstream.ConsumerInfo(stream, latestName, nats.MaxWait(jetStreamAttemptWait))
				if err == nil {
					return info, nil
				}
				lastError = err
			} else {
				lastError = fmt.Errorf("no consumer with prefix %q after generation %d", prefix, minimumGeneration)
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	if lastError == nil {
		return nil, fmt.Errorf("consumer with prefix %q after generation %d did not become ready", prefix, minimumGeneration)
	}
	return nil, fmt.Errorf("consumer with prefix %q after generation %d did not become ready: %w", prefix, minimumGeneration, lastError)
}

func retryCreateReplicatedKeyValue(jetstream natsjetstream.JetStream, config natsjetstream.KeyValueConfig, deadline time.Time) (natsjetstream.KeyValue, error) {
	var lastError error
	for time.Now().Before(deadline) {
		ctx, cancel := keyValueContext()
		keyValue, err := jetstream.CreateKeyValue(ctx, config)
		cancel()
		if err == nil {
			return keyValue, nil
		}
		lastError = err

		ctx, cancel = keyValueContext()
		keyValue, lookupError := jetstream.KeyValue(ctx, config.Bucket)
		cancel()
		if lookupError == nil {
			return keyValue, nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	if lastError == nil {
		return nil, fmt.Errorf("Key-Value bucket %s did not accept a create request", config.Bucket)
	}
	return nil, fmt.Errorf("Key-Value bucket %s did not become placeable: %w", config.Bucket, lastError)
}

func retryDeleteKeyValue(jetstream natsjetstream.JetStream, bucket string, deadline time.Time) error {
	var lastError error
	for time.Now().Before(deadline) {
		ctx, cancel := keyValueContext()
		err := jetstream.DeleteKeyValue(ctx, bucket)
		cancel()
		if err == nil {
			return nil
		}
		lastError = err
		time.Sleep(250 * time.Millisecond)
	}
	if lastError == nil {
		return fmt.Errorf("Key-Value bucket %s did not accept a delete request", bucket)
	}
	return fmt.Errorf("Key-Value bucket %s did not become deletable: %w", bucket, lastError)
}

func checkReplicatedKeyValueStatus(label string, status natsjetstream.KeyValueStatus, bucket string, values uint64) error {
	if status.Bucket() != bucket {
		return fmt.Errorf("%s bucket was %q, expected %q", label, status.Bucket(), bucket)
	}
	if status.Values() != values {
		return fmt.Errorf("%s values were %d, expected %d", label, status.Values(), values)
	}
	config := status.Config()
	if config.History != kvOrderedReconnectHistory {
		return fmt.Errorf("%s history was %d, expected %d", label, config.History, kvOrderedReconnectHistory)
	}
	if config.Storage != natsjetstream.FileStorage {
		return fmt.Errorf("%s did not use file storage", label)
	}
	if config.Replicas != 3 {
		return fmt.Errorf("%s replicas were %d, expected 3", label, config.Replicas)
	}
	return nil
}

func runJetStreamKeyValueOrderedReconnectPeer(config options) error {
	authOptions, err := connectOptions()
	if err != nil {
		return err
	}
	reconnected := make(chan struct{}, 1)
	connectionOptions := []nats.Option{
		nats.Name("ocaml-nats Key-Value ordered reconnect interop peer"),
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

	jetstream, err := natsjetstream.New(connection)
	if err != nil {
		return fmt.Errorf("JetStream context: %w", err)
	}
	keyValueConfig := natsjetstream.KeyValueConfig{
		Bucket:   config.bucket,
		History:  kvOrderedReconnectHistory,
		Storage:  natsjetstream.FileStorage,
		Replicas: 3,
	}
	keyValue, err := retryCreateReplicatedKeyValue(jetstream, keyValueConfig, time.Now().Add(orderedReconnectWait))
	if err != nil {
		return fmt.Errorf("create replicated Key-Value bucket: %w", err)
	}
	if keyValue.Bucket() != config.bucket {
		return fmt.Errorf("created Key-Value bucket %q, expected %q", keyValue.Bucket(), config.bucket)
	}
	ctx, cancel := keyValueContext()
	status, err := keyValue.Status(ctx)
	cancel()
	if err != nil {
		return fmt.Errorf("initial Key-Value status: %w", err)
	}
	if err := checkReplicatedKeyValueStatus("initial Key-Value status", status, config.bucket, 0); err != nil {
		return err
	}

	streamName := "KV_" + config.bucket
	legacyJetstream, err := connection.JetStream(nats.MaxWait(orderedReconnectWait))
	if err != nil {
		return fmt.Errorf("legacy JetStream context: %w", err)
	}
	streamInfo, err := retryJetStreamStreamInfo(legacyJetstream, streamName, time.Now().Add(orderedReconnectWait))
	if err != nil {
		return fmt.Errorf("open Key-Value stream: %w", err)
	}
	if streamInfo.Config.Storage != nats.FileStorage || streamInfo.Config.Replicas != 3 {
		return fmt.Errorf("Key-Value stream storage=%v replicas=%d, expected file-backed with three replicas", streamInfo.Config.Storage, streamInfo.Config.Replicas)
	}
	ctx, cancel = keyValueContext()
	baselineRevision, err := keyValue.Put(ctx, "watch", []byte("before-failover"))
	cancel()
	if err != nil {
		return fmt.Errorf("put baseline Key-Value entry: %w", err)
	}
	if baselineRevision != 1 {
		return fmt.Errorf("baseline Key-Value revision was %d, expected 1", baselineRevision)
	}
	streamInfo, err = retryJetStreamStreamQuorum(legacyJetstream, streamName, 1, 1, time.Now().Add(orderedReconnectWait))
	if err != nil {
		return err
	}

	leaderName := ""
	var leaderProbe nats.JetStreamContext
	if config.leader != "" {
		streamInfo, err = retryJetStreamStreamInfo(legacyJetstream, streamName, time.Now().Add(orderedReconnectWait))
		if err != nil {
			return err
		}
		if streamInfo.Cluster == nil || streamInfo.Cluster.Leader == "" {
			return fmt.Errorf("created Key-Value stream did not identify a JetStream leader")
		}
		leaderName = streamInfo.Cluster.Leader
		if err := os.WriteFile(config.leader, []byte(leaderName+"\n"), 0600); err != nil {
			return fmt.Errorf("write Key-Value leader: %w", err)
		}
		survivorServer, err := waitForJetStreamSurvivorFile(config.signal, config.survivor)
		if err != nil {
			return err
		}
		connection.Close()
		connection, err = nats.Connect(survivorServer, connectionOptions...)
		if err != nil {
			return fmt.Errorf("connect to Key-Value survivor %q: %w", survivorServer, err)
		}
		jetstream, err = natsjetstream.New(connection)
		if err != nil {
			return fmt.Errorf("Key-Value survivor context: %w", err)
		}
		ctx, cancel = keyValueContext()
		keyValue, err = jetstream.KeyValue(ctx, config.bucket)
		cancel()
		if err != nil {
			return fmt.Errorf("open Key-Value bucket on survivor: %w", err)
		}
		legacyJetstream, err = connection.JetStream(nats.MaxWait(orderedReconnectWait))
		if err != nil {
			return fmt.Errorf("Key-Value survivor legacy context: %w", err)
		}
		leaderProbe = legacyJetstream
		streamInfo, err = retryJetStreamStreamInfo(leaderProbe, streamName, time.Now().Add(orderedReconnectWait))
		if err != nil {
			return err
		}
		if streamInfo.Cluster == nil || streamInfo.Cluster.Leader != leaderName {
			actualLeader := ""
			if streamInfo.Cluster != nil {
				actualLeader = streamInfo.Cluster.Leader
			}
			return markJetStreamInteropFailure(config.signal, fmt.Errorf("Key-Value leader changed during setup from %q to %q", leaderName, actualLeader))
		}
	}

	startMessages := newKeyValueControl()
	baselineMessages := newKeyValueControl()
	recoveryMessages := newKeyValueControl()
	entrySeenMessages := newKeyValueControl()
	closeMessages := newKeyValueControl()
	cleanupMessages := newKeyValueControl()
	controls := []struct {
		subject  string
		messages *keyValueControl
		name     string
	}{
		{config.prefix + ".start", startMessages, "start"},
		{config.prefix + ".baseline", baselineMessages, "baseline"},
		{config.prefix + ".recovery-ready", recoveryMessages, "recovery readiness"},
		{config.prefix + ".entry-seen", entrySeenMessages, "entry acknowledgement"},
		{config.prefix + ".close", closeMessages, "close"},
		{config.prefix + ".cleanup", cleanupMessages, "cleanup"},
	}
	for _, control := range controls {
		control := control
		if _, err := connection.Subscribe(control.subject, func(message *nats.Msg) {
			control.messages.receive(message)
		}); err != nil {
			return fmt.Errorf("subscribe %s: %w", control.name, err)
		}
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush Key-Value setup: %w", err)
	}
	if err := os.WriteFile(config.ready, []byte("ready\n"), 0600); err != nil {
		return fmt.Errorf("write ready file: %w", err)
	}

	startMessage, err := startMessages.wait("Key-Value start request")
	if err != nil {
		return err
	}
	if err := validateKeyValueControl("Key-Value start", "start", startMessage); err != nil {
		return err
	}
	if err := startMessages.respond(connection, "Key-Value start", startMessage, "started"); err != nil {
		return err
	}

	baselineMessage, err := baselineMessages.wait("Key-Value baseline completion")
	if err != nil {
		return err
	}
	if err := validateKeyValueControl("Key-Value baseline", "baseline-complete", baselineMessage); err != nil {
		return err
	}
	baselineDeadline := time.Now().Add(orderedReconnectWait)
	ctx, cancel = keyValueContext()
	status, err = keyValue.Status(ctx)
	cancel()
	if err != nil {
		return fmt.Errorf("baseline Key-Value status: %w", err)
	}
	if err := checkReplicatedKeyValueStatus("baseline Key-Value status", status, config.bucket, 1); err != nil {
		return err
	}
	if _, err := retryJetStreamStreamQuorum(legacyJetstream, streamName, 1, 1, baselineDeadline); err != nil {
		return err
	}
	if err := baselineMessages.respond(connection, "Key-Value baseline", baselineMessage, "go-baseline-ready"); err != nil {
		return err
	}
	initialConsumer, err := retryJetStreamConsumerGeneration(legacyJetstream, streamName, "kv-ordered-reconnect", 0, time.Now().Add(orderedReconnectWait))
	if err != nil {
		return fmt.Errorf("wait for initial OCaml ordered consumer: %w", err)
	}
	initialGeneration, ok := consumerGeneration(initialConsumer.Name, "kv-ordered-reconnect")
	if !ok {
		return fmt.Errorf("initial OCaml ordered consumer name %q did not use the expected prefix", initialConsumer.Name)
	}

	if err := waitForJetStreamReconnectSignal(config.signal); err != nil {
		return err
	}
	var recoveryDeadline time.Time
	if config.leader != "" {
		streamInfo, err = retryJetStreamStreamInfo(leaderProbe, streamName, time.Now().Add(orderedReconnectWait))
		if err != nil {
			return markJetStreamInteropFailure(config.signal, err)
		}
		if streamInfo.Cluster == nil || streamInfo.Cluster.Leader != leaderName {
			actualLeader := ""
			if streamInfo.Cluster != nil {
				actualLeader = streamInfo.Cluster.Leader
			}
			return markJetStreamInteropFailure(config.signal, fmt.Errorf("Key-Value leader before kill was %q, expected %q", actualLeader, leaderName))
		}
		if err := os.WriteFile(config.leader, []byte(leaderName+"\n"), 0600); err != nil {
			return markJetStreamInteropFailure(config.signal, fmt.Errorf("write current Key-Value leader: %w", err))
		}
		if err := os.WriteFile(config.signal+".kill-ready", []byte("ready\n"), 0600); err != nil {
			return markJetStreamInteropFailure(config.signal, fmt.Errorf("write leader kill barrier: %w", err))
		}
		if err := waitForJetStreamLeaderKillSignal(config.signal); err != nil {
			return err
		}
		recoveryDeadline = time.Now().Add(orderedReconnectWait)
		streamInfo, err = retryJetStreamLeaderChange(leaderProbe, streamName, leaderName, recoveryDeadline)
		if err == nil {
			// Stream leadership can move before metadata and ephemeral consumer
			// leadership have converged; let those control-plane updates settle
			// before the functional recovery handshake.
			time.Sleep(3 * time.Second)
		}
	} else if err := waitJetStreamReconnect(reconnected); err != nil {
		return err
	} else {
		recoveryDeadline = time.Now().Add(orderedReconnectWait)
		if config.requireReplicatedStream {
			if err := os.WriteFile(config.signal+".go-reconnected", []byte("ready\n"), 0600); err != nil {
				return fmt.Errorf("write Go reconnect barrier: %w", err)
			}
		}
	}
	if err != nil {
		if config.leader != "" {
			return markJetStreamInteropFailure(config.signal, err)
		}
		return err
	}
	if config.leader == "" {
		if config.requireReplicatedStream {
			streamInfo, err = retryJetStreamStreamQuorum(legacyJetstream, streamName, 1, 1, recoveryDeadline)
		} else {
			streamInfo, err = retryJetStreamStreamInfo(legacyJetstream, streamName, recoveryDeadline)
		}
	}
	if err != nil {
		return err
	}
	if streamInfo.Config.Storage != nats.FileStorage || streamInfo.Config.Replicas != 3 || streamInfo.State.Msgs != 1 || streamInfo.State.LastSeq != 1 {
		return fmt.Errorf("Key-Value stream after failover storage=%v replicas=%d messages=%d last-sequence=%d", streamInfo.Config.Storage, streamInfo.Config.Replicas, streamInfo.State.Msgs, streamInfo.State.LastSeq)
	}

	recoveryMessage, err := recoveryMessages.wait("OCaml Key-Value recovery readiness")
	if err != nil {
		return err
	}
	recoveryPayload := "ocaml-reconnected"
	if config.leader != "" {
		recoveryPayload = "ocaml-leader-failover"
	}
	if err := validateKeyValueControl("OCaml Key-Value recovery readiness", recoveryPayload, recoveryMessage); err != nil {
		return err
	}
	consumerMinimumGeneration := 0
	if config.leader == "" {
		consumerMinimumGeneration = initialGeneration
	}
	consumerRecoveryDeadline := time.Now().Add(orderedReconnectWait)
	if _, err := retryJetStreamConsumerGeneration(legacyJetstream, streamName, "kv-ordered-reconnect", consumerMinimumGeneration, consumerRecoveryDeadline); err != nil {
		return fmt.Errorf("wait for recovered OCaml ordered consumer: %w", err)
	}
	ctx, cancel = keyValueContext()
	recoveryRevision, err := keyValue.Put(ctx, "watch", []byte("after-failover"))
	cancel()
	if err != nil {
		return fmt.Errorf("put post-failover Key-Value entry: %w", err)
	}
	if recoveryRevision != 2 {
		return fmt.Errorf("post-failover Key-Value revision was %d, expected 2", recoveryRevision)
	}
	if err := recoveryMessages.respond(connection, "Key-Value recovery readiness", recoveryMessage, "go-recovery-ready"); err != nil {
		return err
	}

	entrySeenMessage, err := entrySeenMessages.wait("OCaml Key-Value entry acknowledgement")
	if err != nil {
		return err
	}
	if err := validateKeyValueControl("OCaml Key-Value entry acknowledgement", "ocaml-entry-seen", entrySeenMessage); err != nil {
		return err
	}
	if err := entrySeenMessages.respond(connection, "Key-Value entry acknowledgement", entrySeenMessage, "go-entry-validated"); err != nil {
		return err
	}

	closeMessage, err := closeMessages.wait("OCaml Key-Value close request")
	if err != nil {
		return err
	}
	if err := validateKeyValueControl("OCaml Key-Value close", "ocaml-close", closeMessage); err != nil {
		return err
	}
	if err := closeMessages.respond(connection, "Key-Value close", closeMessage, "go-closed"); err != nil {
		return err
	}

	cleanupMessage, err := cleanupMessages.wait("Key-Value cleanup request")
	if err != nil {
		return err
	}
	if err := validateKeyValueControl("Key-Value cleanup", "cleanup", cleanupMessage); err != nil {
		return err
	}
	if err := retryDeleteKeyValue(jetstream, config.bucket, time.Now().Add(orderedReconnectWait)); err != nil {
		return err
	}
	if err := cleanupMessages.respond(connection, "Key-Value cleanup", cleanupMessage, "cleaned"); err != nil {
		return err
	}
	if err := connection.Drain(); err != nil {
		return fmt.Errorf("drain: %w", err)
	}
	return nil
}
