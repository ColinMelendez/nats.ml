package main

import (
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	natsjetstream "github.com/nats-io/nats.go/jetstream"
)

const objectReconnectWait = 90 * time.Second

func retryCreateReplicatedObjectStore(jetstream natsjetstream.JetStream, config natsjetstream.ObjectStoreConfig, deadline time.Time) (natsjetstream.ObjectStore, error) {
	var lastError error
	for time.Now().Before(deadline) {
		ctx, cancel := objectStoreContext()
		objectStore, err := jetstream.CreateObjectStore(ctx, config)
		cancel()
		if err == nil {
			return objectStore, nil
		}
		lastError = err
		ctx, cancel = objectStoreContext()
		objectStore, lookupError := jetstream.ObjectStore(ctx, config.Bucket)
		cancel()
		if lookupError == nil {
			return objectStore, nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	if lastError == nil {
		return nil, fmt.Errorf("Object Store %s did not accept a create request", config.Bucket)
	}
	return nil, fmt.Errorf("Object Store %s did not become placeable: %w", config.Bucket, lastError)
}

func retryOpenObjectStore(jetstream natsjetstream.JetStream, bucket string, deadline time.Time) (natsjetstream.ObjectStore, error) {
	var lastError error
	for time.Now().Before(deadline) {
		ctx, cancel := objectStoreContext()
		objectStore, err := jetstream.ObjectStore(ctx, bucket)
		cancel()
		if err == nil {
			return objectStore, nil
		}
		lastError = err
		time.Sleep(250 * time.Millisecond)
	}
	if lastError == nil {
		return nil, fmt.Errorf("Object Store %s did not become readable", bucket)
	}
	return nil, fmt.Errorf("Object Store %s did not become readable: %w", bucket, lastError)
}

func retryDeleteObjectStore(jetstream natsjetstream.JetStream, bucket string, deadline time.Time) error {
	var lastError error
	for time.Now().Before(deadline) {
		ctx, cancel := objectStoreContext()
		err := jetstream.DeleteObjectStore(ctx, bucket)
		cancel()
		if err == nil {
			return nil
		}
		lastError = err
		time.Sleep(250 * time.Millisecond)
	}
	if lastError == nil {
		return fmt.Errorf("Object Store %s did not accept a delete request", bucket)
	}
	return fmt.Errorf("Object Store %s did not become deletable: %w", bucket, lastError)
}

func checkReplicatedObjectStoreStatus(label string, status natsjetstream.ObjectStoreStatus, bucket string, size uint64) error {
	if status.Bucket() != bucket {
		return fmt.Errorf("%s bucket was %q, expected %q", label, status.Bucket(), bucket)
	}
	if status.Description() != "interop-cluster" {
		return fmt.Errorf("%s description was %q, expected %q", label, status.Description(), "interop-cluster")
	}
	if status.Storage() != natsjetstream.FileStorage {
		return fmt.Errorf("%s did not use file storage", label)
	}
	if status.Replicas() != 3 {
		return fmt.Errorf("%s replicas were %d, expected 3", label, status.Replicas())
	}
	if status.Size() < size {
		return fmt.Errorf("%s size was %d, expected at least %d", label, status.Size(), size)
	}
	if status.Metadata()["owner"] != "interop-cluster" {
		return fmt.Errorf("%s owner metadata was %q, expected %q", label, status.Metadata()["owner"], "interop-cluster")
	}
	if status.Sealed() {
		return fmt.Errorf("%s was unexpectedly sealed", label)
	}
	return nil
}

func retryObjectStoreStreamState(jetstream nats.JetStreamContext, stream string, messages, lastSequence uint64, deadline time.Time) (*nats.StreamInfo, error) {
	var lastError error
	for time.Now().Before(deadline) {
		info, err := jetstream.StreamInfo(stream, nats.MaxWait(jetStreamAttemptWait))
		if err == nil {
			if info.Config.Storage != nats.FileStorage || info.Config.Replicas != 3 {
				lastError = fmt.Errorf("Object Store stream storage=%v replicas=%d, expected file-backed with three replicas", info.Config.Storage, info.Config.Replicas)
			} else if info.Cluster == nil || info.Cluster.Leader == "" {
				lastError = fmt.Errorf("Object Store stream did not report a current leader")
			} else if info.State.Msgs != messages || info.State.LastSeq != lastSequence {
				lastError = fmt.Errorf("Object Store stream state messages=%d last-sequence=%d, expected messages=%d last-sequence=%d", info.State.Msgs, info.State.LastSeq, messages, lastSequence)
			} else {
				return info, nil
			}
		} else {
			lastError = err
		}
		time.Sleep(250 * time.Millisecond)
	}
	if lastError == nil {
		return nil, fmt.Errorf("Object Store stream %s did not report its expected state", stream)
	}
	return nil, fmt.Errorf("Object Store stream %s did not reach its expected state: %w", stream, lastError)
}

func retryObjectStoreGetString(objectStore natsjetstream.ObjectStore, name string, deadline time.Time) (string, error) {
	var lastError error
	for time.Now().Before(deadline) {
		ctx, cancel := objectStoreContext()
		payload, err := objectStore.GetString(ctx, name)
		cancel()
		if err == nil {
			return payload, nil
		}
		lastError = err
		time.Sleep(250 * time.Millisecond)
	}
	if lastError == nil {
		return "", fmt.Errorf("Object Store object %s was not readable", name)
	}
	return "", fmt.Errorf("Object Store object %s did not become readable: %w", name, lastError)
}

func runJetStreamObjectReconnectPeer(config options) error {
	authOptions, err := connectOptions()
	if err != nil {
		return err
	}
	reconnected := make(chan struct{}, 1)
	connectionOptions := []nats.Option{
		nats.Name("ocaml-nats Object Store reconnect interop peer"),
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
	if err := expectInitialConnection(connection, config.server); err != nil {
		connection.Close()
		return err
	}
	defer connection.Close()

	jetstream, err := natsjetstream.New(connection)
	if err != nil {
		return fmt.Errorf("JetStream context: %w", err)
	}
	objectConfig := natsjetstream.ObjectStoreConfig{
		Bucket:      config.bucket,
		Description: "interop-cluster",
		MaxBytes:    1 << 20,
		Storage:     natsjetstream.FileStorage,
		Replicas:    3,
		Metadata:    map[string]string{"owner": "interop-cluster"},
	}
	objectStore, err := retryCreateReplicatedObjectStore(jetstream, objectConfig, time.Now().Add(objectReconnectWait))
	if err != nil {
		return fmt.Errorf("create replicated Object Store: %w", err)
	}
	streamName := "OBJ_" + config.bucket
	legacyJetstream, err := connection.JetStream(nats.MaxWait(objectReconnectWait))
	if err != nil {
		return fmt.Errorf("legacy JetStream context: %w", err)
	}
	streamInfo, err := retryJetStreamStreamInfo(legacyJetstream, streamName, time.Now().Add(objectReconnectWait))
	if err != nil {
		return fmt.Errorf("open Object Store stream: %w", err)
	}
	if streamInfo.Config.Storage != nats.FileStorage || streamInfo.Config.Replicas != 3 {
		return fmt.Errorf("Object Store stream storage=%v replicas=%d, expected file-backed with three replicas", streamInfo.Config.Storage, streamInfo.Config.Replicas)
	}

	leaderName := ""
	var leaderProbe nats.JetStreamContext
	if config.leader != "" {
		if streamInfo.Cluster == nil || streamInfo.Cluster.Leader == "" {
			return fmt.Errorf("created Object Store stream did not identify a JetStream leader")
		}
		leaderName = streamInfo.Cluster.Leader
		if err := os.WriteFile(config.leader, []byte(leaderName+"\n"), 0600); err != nil {
			return fmt.Errorf("write Object Store leader: %w", err)
		}
		survivorServer, err := waitForJetStreamSurvivorFile(config.signal, config.survivor)
		if err != nil {
			return err
		}
		connection.Close()
		connection, err = nats.Connect(survivorServer, connectionOptions...)
		if err != nil {
			return fmt.Errorf("connect to Object Store survivor %q: %w", survivorServer, err)
		}
		jetstream, err = natsjetstream.New(connection)
		if err != nil {
			return fmt.Errorf("Object Store survivor context: %w", err)
		}
		ctx, cancel := objectStoreContext()
		objectStore, err = jetstream.ObjectStore(ctx, config.bucket)
		cancel()
		if err != nil {
			return fmt.Errorf("open Object Store on survivor: %w", err)
		}
		legacyJetstream, err = connection.JetStream(nats.MaxWait(objectReconnectWait))
		if err != nil {
			return fmt.Errorf("Object Store survivor legacy context: %w", err)
		}
		leaderProbe = legacyJetstream
		streamInfo, err = retryJetStreamStreamInfo(leaderProbe, streamName, time.Now().Add(objectReconnectWait))
		if err != nil {
			return err
		}
		if streamInfo.Cluster == nil || streamInfo.Cluster.Leader != leaderName {
			actualLeader := ""
			if streamInfo.Cluster != nil {
				actualLeader = streamInfo.Cluster.Leader
			}
			return markJetStreamInteropFailure(config.signal, fmt.Errorf("Object Store leader changed during setup from %q to %q", leaderName, actualLeader))
		}
	}

	startMessages := newKeyValueControl()
	baselineMessages := newKeyValueControl()
	recoveryMessages := newKeyValueControl()
	afterMessages := newKeyValueControl()
	cleanupMessages := newKeyValueControl()
	controls := []struct {
		subject  string
		messages *keyValueControl
		name     string
	}{
		{config.prefix + ".start", startMessages, "start"},
		{config.prefix + ".baseline", baselineMessages, "baseline"},
		{config.prefix + ".recovery-ready", recoveryMessages, "recovery readiness"},
		{config.prefix + ".after-seen", afterMessages, "post-failover object"},
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
		return fmt.Errorf("flush Object Store setup: %w", err)
	}
	if err := os.WriteFile(config.ready, []byte("ready\n"), 0600); err != nil {
		return fmt.Errorf("write ready file: %w", err)
	}

	startMessage, err := startMessages.wait("Object Store start request")
	if err != nil {
		return err
	}
	if err := validateKeyValueControl("Object Store start", "start", startMessage); err != nil {
		return err
	}
	ctx, cancel := objectStoreContext()
	goInfo, err := objectStore.PutString(ctx, "go-before", "from-go-before")
	cancel()
	if err != nil {
		return fmt.Errorf("put pre-failover Go object: %w", err)
	}
	if err := checkObjectInfo("pre-failover Go object", goInfo, config.bucket, "go-before", 14, 1); err != nil {
		return err
	}
	if err := startMessages.respond(connection, "Object Store start", startMessage, "started"); err != nil {
		return err
	}

	baselineMessage, err := baselineMessages.wait("Object Store baseline completion")
	if err != nil {
		return err
	}
	if err := validateKeyValueControl("Object Store baseline", "baseline-complete", baselineMessage); err != nil {
		return err
	}
	ctx, cancel = objectStoreContext()
	ocamlPayload, err := objectStore.GetString(ctx, "ocaml-before")
	cancel()
	if err != nil {
		return fmt.Errorf("get pre-failover OCaml object: %w", err)
	}
	if ocamlPayload != "from-ocaml-before" {
		return fmt.Errorf("pre-failover OCaml object was %q, expected %q", ocamlPayload, "from-ocaml-before")
	}
	streamInfo, err = retryJetStreamStreamQuorum(legacyJetstream, streamName, 8, 8, time.Now().Add(objectReconnectWait))
	if err != nil {
		return err
	}
	ctx, cancel = objectStoreContext()
	status, err := objectStore.Status(ctx)
	cancel()
	if err != nil {
		return fmt.Errorf("baseline Object Store status: %w", err)
	}
	if err := checkReplicatedObjectStoreStatus("baseline Object Store status", status, config.bucket, 28); err != nil {
		return err
	}
	if err := baselineMessages.respond(connection, "Object Store baseline", baselineMessage, "go-baseline-ready"); err != nil {
		return err
	}

	if err := waitForJetStreamReconnectSignal(config.signal); err != nil {
		return err
	}
	var recoveryDeadline time.Time
	if config.leader != "" {
		streamInfo, err = retryJetStreamStreamInfo(leaderProbe, streamName, time.Now().Add(objectReconnectWait))
		if err != nil {
			return markJetStreamInteropFailure(config.signal, err)
		}
		if streamInfo.Cluster == nil || streamInfo.Cluster.Leader != leaderName {
			actualLeader := ""
			if streamInfo.Cluster != nil {
				actualLeader = streamInfo.Cluster.Leader
			}
			return markJetStreamInteropFailure(config.signal, fmt.Errorf("Object Store leader before kill was %q, expected %q", actualLeader, leaderName))
		}
		if err := os.WriteFile(config.leader, []byte(leaderName+"\n"), 0600); err != nil {
			return markJetStreamInteropFailure(config.signal, fmt.Errorf("write current Object Store leader: %w", err))
		}
		if err := os.WriteFile(config.signal+".kill-ready", []byte("ready\n"), 0600); err != nil {
			return markJetStreamInteropFailure(config.signal, fmt.Errorf("write leader kill barrier: %w", err))
		}
		if err := waitForJetStreamLeaderKillSignal(config.signal); err != nil {
			return err
		}
		recoveryDeadline = time.Now().Add(objectReconnectWait)
		streamInfo, err = retryJetStreamLeaderChange(leaderProbe, streamName, leaderName, recoveryDeadline)
	} else if err := waitJetStreamReconnect(reconnected); err != nil {
		return err
	} else {
		if config.requireReplicatedStream || config.multiNodeLoss {
			if err := os.WriteFile(config.signal+".go-reconnected", []byte("ready\n"), 0600); err != nil {
				return fmt.Errorf("write Go reconnect barrier: %w", err)
			}
		}
		if config.multiNodeLoss {
			if err := waitForJetStreamMultiNodeRecoverySignal(config.signal); err != nil {
				return err
			}
		}
		recoveryDeadline = time.Now().Add(objectReconnectWait)
	}
	if err != nil {
		if config.leader != "" {
			return markJetStreamInteropFailure(config.signal, err)
		}
		return err
	}
	if config.leader == "" && (config.requireReplicatedStream || config.multiNodeLoss) {
		if _, err = retryJetStreamStreamQuorum(legacyJetstream, streamName, 8, 8, recoveryDeadline); err != nil {
			return err
		}
	}
	if streamInfo, err = retryObjectStoreStreamState(legacyJetstream, streamName, 8, 8, recoveryDeadline); err != nil {
		return err
	}
	objectStore, err = retryOpenObjectStore(jetstream, config.bucket, time.Now().Add(objectReconnectWait))
	if err != nil {
		return fmt.Errorf("reopen Object Store after reconnect: %w", err)
	}
	goPayload, err := retryObjectStoreGetString(objectStore, "go-before", time.Now().Add(objectReconnectWait))
	if err != nil {
		return fmt.Errorf("get replicated Go object after failover: %w", err)
	}
	if goPayload != "from-go-before" {
		return fmt.Errorf("replicated Go object after failover was %q, expected %q", goPayload, "from-go-before")
	}

	recoveryMessage, err := recoveryMessages.waitWithTimeout(
		"OCaml Object Store recovery readiness", objectReconnectWait)
	if err != nil {
		return err
	}
	recoveryPayload := "ocaml-reconnected"
	if config.leader != "" {
		recoveryPayload = "ocaml-leader-failover"
	}
	if err := validateKeyValueControl("OCaml Object Store recovery readiness", recoveryPayload, recoveryMessage); err != nil {
		return err
	}
	ctx, cancel = objectStoreContext()
	goInfo, err = objectStore.PutString(ctx, "go-after", "from-go-after")
	cancel()
	if err != nil {
		return fmt.Errorf("put post-failover Go object: %w", err)
	}
	if err := checkObjectInfo("post-failover Go object", goInfo, config.bucket, "go-after", 13, 1); err != nil {
		return err
	}
	goAfterPayload, err := retryObjectStoreGetString(objectStore, "go-after", time.Now().Add(objectReconnectWait))
	if err != nil {
		return fmt.Errorf("read post-failover Go object: %w", err)
	}
	if goAfterPayload != "from-go-after" {
		return fmt.Errorf("post-failover Go object was %q, expected %q", goAfterPayload, "from-go-after")
	}
	if err := recoveryMessages.respond(connection, "Object Store recovery readiness", recoveryMessage, "go-recovery-ready"); err != nil {
		return err
	}

	afterMessage, err := afterMessages.waitWithTimeout(
		"OCaml post-failover Object Store object", objectReconnectWait)
	if err != nil {
		return err
	}
	if err := validateKeyValueControl("OCaml post-failover Object Store object", "ocaml-after-seen", afterMessage); err != nil {
		return err
	}
	ocamlAfter, err := retryObjectStoreGetString(objectStore, "ocaml-after", time.Now().Add(objectReconnectWait))
	if err != nil {
		return fmt.Errorf("get post-failover OCaml object: %w", err)
	}
	if ocamlAfter != "from-ocaml-after" {
		return fmt.Errorf("post-failover OCaml object was %q, expected %q", ocamlAfter, "from-ocaml-after")
	}
	if err := afterMessages.respond(connection, "Object Store post-failover object", afterMessage, "go-after-validated"); err != nil {
		return err
	}

	cleanupMessage, err := cleanupMessages.wait("Object Store cleanup request")
	if err != nil {
		return err
	}
	if err := validateKeyValueControl("Object Store cleanup", "cleanup", cleanupMessage); err != nil {
		return err
	}
	if err := retryDeleteObjectStore(jetstream, config.bucket, time.Now().Add(objectReconnectWait)); err != nil {
		return err
	}
	if err := cleanupMessages.respond(connection, "Object Store cleanup", cleanupMessage, "cleaned"); err != nil {
		return err
	}
	if err := connection.Drain(); err != nil {
		return fmt.Errorf("drain: %w", err)
	}
	return nil
}
