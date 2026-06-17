package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/nats-io/nats.go"
	natsjetstream "github.com/nats-io/nats.go/jetstream"
)

const (
	kvHistory         = 5
	kvGoKey           = "go.key"
	kvWatchOCamlKey   = "watch.ocaml"
	kvWatchGoKey      = "watch.go"
	kvPurgeKey        = "purge.key"
	kvOCamlTTLKey     = "ttl.ocaml"
	kvGoTTLKey        = "ttl.go"
	kvPurgeDeletesKey = "purge.deletes"
)

func keyValueContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), waitTimeout)
}

func keyValueMarkerTTLSupported(version string) (bool, error) {
	var major, minor, patch int
	if _, err := fmt.Sscanf(version, "%d.%d.%d", &major, &minor, &patch); err != nil {
		return false, fmt.Errorf("parse NATS server version %q: %w", version, err)
	}
	return major > 2 || major == 2 && minor >= 11, nil
}

func publishReadyFile(path, content string) error {
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(path)+".*")
	if err != nil {
		return fmt.Errorf("create temporary ready file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if _, err := temporary.WriteString(content); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write temporary ready file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary ready file: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("publish ready file: %w", err)
	}
	return nil
}

func checkKeyValueEntry(label string, entry natsjetstream.KeyValueEntry, bucket, key, value string, revision uint64, operation natsjetstream.KeyValueOp) error {
	if entry.Bucket() != bucket {
		return fmt.Errorf("%s bucket was %q, expected %q", label, entry.Bucket(), bucket)
	}
	if entry.Key() != key {
		return fmt.Errorf("%s key was %q, expected %q", label, entry.Key(), key)
	}
	if string(entry.Value()) != value {
		return fmt.Errorf("%s value was %q, expected %q", label, string(entry.Value()), value)
	}
	if entry.Revision() != revision {
		return fmt.Errorf("%s revision was %d, expected %d", label, entry.Revision(), revision)
	}
	if entry.Operation() != operation {
		return fmt.Errorf("%s operation was %q, expected %q", label, entry.Operation(), operation)
	}
	return nil
}

func checkKeyValueStatus(label string, status natsjetstream.KeyValueStatus, bucket string, values uint64, markerTTL time.Duration) error {
	if status.Bucket() != bucket {
		return fmt.Errorf("%s bucket was %q, expected %q", label, status.Bucket(), bucket)
	}
	if status.Values() != values {
		return fmt.Errorf("%s values were %d, expected %d", label, status.Values(), values)
	}
	if status.History() != kvHistory {
		return fmt.Errorf("%s history was %d, expected %d", label, status.History(), kvHistory)
	}
	if status.TTL() != 0 {
		return fmt.Errorf("%s TTL was %s, expected zero", label, status.TTL())
	}
	if status.LimitMarkerTTL() != markerTTL {
		return fmt.Errorf("%s limit marker TTL was %s, expected %s", label, status.LimitMarkerTTL(), markerTTL)
	}
	if status.BackingStore() != "JetStream" {
		return fmt.Errorf("%s backing store was %q, expected JetStream", label, status.BackingStore())
	}
	if status.Config().Storage != natsjetstream.MemoryStorage {
		return fmt.Errorf("%s did not use memory storage", label)
	}
	return nil
}

func checkKeyValueMessage(label string, stream natsjetstream.Stream, bucket, key string, expectedTTL time.Duration, expectedOperation string) error {
	ctx, cancel := keyValueContext()
	message, err := stream.GetLastMsgForSubject(ctx, "$KV."+bucket+"."+key)
	cancel()
	if err != nil {
		return fmt.Errorf("%s get raw message: %w", label, err)
	}
	ttlText := message.Header.Get(natsjetstream.MsgTTLHeader)
	ttl, err := time.ParseDuration(ttlText)
	if err != nil {
		return fmt.Errorf("%s TTL header %q was not a duration: %w", label, ttlText, err)
	}
	if ttl != expectedTTL {
		return fmt.Errorf("%s TTL header was %q, expected duration %s", label, ttlText, expectedTTL)
	}
	if operation := message.Header.Get("KV-Operation"); operation != expectedOperation {
		return fmt.Errorf("%s operation header was %q, expected %q", label, operation, expectedOperation)
	}
	return nil
}

func waitKeyValueUpdate(label string, watcher natsjetstream.KeyWatcher) (natsjetstream.KeyValueEntry, error) {
	timer := time.NewTimer(waitTimeout)
	defer timer.Stop()
	select {
	case entry, ok := <-watcher.Updates():
		if !ok || entry == nil {
			return nil, fmt.Errorf("%s watcher closed before an update", label)
		}
		return entry, nil
	case <-timer.C:
		return nil, fmt.Errorf("timed out waiting for %s", label)
	}
}

func validateKeyValueControl(label, expected string, message *nats.Msg) error {
	if string(message.Data) != expected {
		return fmt.Errorf("%s payload was %q, expected %q", label, string(message.Data), expected)
	}
	if message.Reply == "" {
		return fmt.Errorf("%s request had no reply subject", label)
	}
	return nil
}

func respondKeyValueControl(connection *nats.Conn, label string, message *nats.Msg, payload string) error {
	if message.Reply == "" {
		return fmt.Errorf("%s request had no reply subject", label)
	}
	if err := message.Respond([]byte(payload)); err != nil {
		return fmt.Errorf("respond to %s: %w", label, err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush response to %s: %w", label, err)
	}
	return nil
}

func runJetStreamKeyValuePeer(config options) error {
	authOptions, err := connectOptions()
	if err != nil {
		return err
	}
	connectionOptions := append([]nats.Option{nats.Name("ocaml-nats Key-Value interop peer")}, authOptions...)
	connection, err := nats.Connect(config.server, connectionOptions...)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer connection.Close()
	markerTTLSupported, err := keyValueMarkerTTLSupported(connection.ConnectedServerVersion())
	if err != nil {
		return err
	}

	jetstream, err := natsjetstream.New(connection)
	if err != nil {
		return fmt.Errorf("JetStream context: %w", err)
	}
	ctx, cancel := keyValueContext()
	keyValueConfig := natsjetstream.KeyValueConfig{
		Bucket:  config.bucket,
		History: kvHistory,
		Storage: natsjetstream.MemoryStorage,
	}
	markerTTL := time.Duration(0)
	if markerTTLSupported {
		markerTTL = time.Minute
		keyValueConfig.LimitMarkerTTL = markerTTL
	}
	keyValue, err := jetstream.CreateKeyValue(ctx, keyValueConfig)
	cancel()
	if err != nil {
		return fmt.Errorf("create Key-Value bucket: %w", err)
	}
	if keyValue.Bucket() != config.bucket {
		return fmt.Errorf("created Key-Value bucket %q, expected %q", keyValue.Bucket(), config.bucket)
	}
	ctx, cancel = keyValueContext()
	status, err := keyValue.Status(ctx)
	cancel()
	if err != nil {
		return fmt.Errorf("initial Key-Value status: %w", err)
	}
	if err := checkKeyValueStatus("initial Key-Value status", status, config.bucket, 0, markerTTL); err != nil {
		return err
	}
	ctx, cancel = keyValueContext()
	stream, err := jetstream.Stream(ctx, "KV_"+config.bucket)
	cancel()
	if err != nil {
		return fmt.Errorf("open Key-Value stream: %w", err)
	}

	startMessages := make(chan *nats.Msg, 1)
	ocamlUpdatedMessages := make(chan *nats.Msg, 1)
	ocamlDeletedMessages := make(chan *nats.Msg, 1)
	goWatchReadyMessages := make(chan *nats.Msg, 1)
	ocamlWatchWrittenMessages := make(chan *nats.Msg, 1)
	goWriteMessages := make(chan *nats.Msg, 1)
	ocamlPurgeReadyMessages := make(chan *nats.Msg, 1)
	ocamlTTLReadyMessages := make(chan *nats.Msg, 1)
	goTTLReadyMessages := make(chan *nats.Msg, 1)
	ocamlPurgeTTLReadyMessages := make(chan *nats.Msg, 1)
	goPurgeTTLMessages := make(chan *nats.Msg, 1)
	goPurgeDeletesReadyMessages := make(chan *nats.Msg, 1)
	ocamlPurgeDeletesDoneMessages := make(chan *nats.Msg, 1)
	doneMessages := make(chan *nats.Msg, 1)
	subscribe := func(suffix string, messages chan<- *nats.Msg) error {
		_, err := connection.Subscribe(config.prefix+suffix, func(message *nats.Msg) {
			messages <- message
		})
		return err
	}
	if err := subscribe(".start", startMessages); err != nil {
		return fmt.Errorf("subscribe Key-Value start: %w", err)
	}
	if err := subscribe(".ocaml-updated", ocamlUpdatedMessages); err != nil {
		return fmt.Errorf("subscribe Key-Value update barrier: %w", err)
	}
	if err := subscribe(".ocaml-deleted", ocamlDeletedMessages); err != nil {
		return fmt.Errorf("subscribe Key-Value delete barrier: %w", err)
	}
	if err := subscribe(".go-watch-ready", goWatchReadyMessages); err != nil {
		return fmt.Errorf("subscribe Key-Value watch barrier: %w", err)
	}
	if err := subscribe(".ocaml-watch-written", ocamlWatchWrittenMessages); err != nil {
		return fmt.Errorf("subscribe Key-Value watch update barrier: %w", err)
	}
	if err := subscribe(".go-write", goWriteMessages); err != nil {
		return fmt.Errorf("subscribe Key-Value Go write: %w", err)
	}
	if err := subscribe(".ocaml-purge-ready", ocamlPurgeReadyMessages); err != nil {
		return fmt.Errorf("subscribe Key-Value purge barrier: %w", err)
	}
	if err := subscribe(".ocaml-ttl-ready", ocamlTTLReadyMessages); err != nil {
		return fmt.Errorf("subscribe Key-Value OCaml TTL barrier: %w", err)
	}
	if err := subscribe(".go-ttl-ready", goTTLReadyMessages); err != nil {
		return fmt.Errorf("subscribe Key-Value Go TTL barrier: %w", err)
	}
	if err := subscribe(".ocaml-purge-ttl-ready", ocamlPurgeTTLReadyMessages); err != nil {
		return fmt.Errorf("subscribe Key-Value OCaml purge TTL barrier: %w", err)
	}
	if err := subscribe(".go-purge-ttl", goPurgeTTLMessages); err != nil {
		return fmt.Errorf("subscribe Key-Value Go purge TTL: %w", err)
	}
	if err := subscribe(".go-purge-deletes-ready", goPurgeDeletesReadyMessages); err != nil {
		return fmt.Errorf("subscribe Key-Value purge deletes setup: %w", err)
	}
	if err := subscribe(".ocaml-purge-deletes-done", ocamlPurgeDeletesDoneMessages); err != nil {
		return fmt.Errorf("subscribe Key-Value purge deletes completion: %w", err)
	}
	if err := subscribe(".done", doneMessages); err != nil {
		return fmt.Errorf("subscribe Key-Value completion: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush Key-Value setup: %w", err)
	}
	readiness := "baseline\n"
	if markerTTLSupported {
		readiness = "marker-ttl\n"
	}
	if err := publishReadyFile(config.ready, readiness); err != nil {
		return fmt.Errorf("write ready file: %w", err)
	}

	startMessage, err := waitMessage("Key-Value start request", startMessages)
	if err != nil {
		return err
	}
	if err := validateKeyValueControl("Key-Value start", "start", startMessage); err != nil {
		return err
	}
	ctx, cancel = keyValueContext()
	goRevision, err := keyValue.Put(ctx, kvGoKey, []byte("from-go"))
	cancel()
	if err != nil {
		return fmt.Errorf("put Go Key-Value entry: %w", err)
	}
	if goRevision != 1 {
		return fmt.Errorf("Go Key-Value entry revision was %d, expected 1", goRevision)
	}
	if err := respondKeyValueControl(connection, "Key-Value start", startMessage, "started"); err != nil {
		return err
	}

	updatedMessage, err := waitMessage("OCaml Key-Value update", ocamlUpdatedMessages)
	if err != nil {
		return err
	}
	if err := validateKeyValueControl("OCaml Key-Value update", "updated", updatedMessage); err != nil {
		return err
	}
	ctx, cancel = keyValueContext()
	entry, err := keyValue.Get(ctx, kvGoKey)
	cancel()
	if err != nil {
		return fmt.Errorf("get OCaml-updated Key-Value entry: %w", err)
	}
	if err := checkKeyValueEntry("OCaml-updated Key-Value entry", entry, config.bucket, kvGoKey, "from-ocaml", 2, natsjetstream.KeyValuePut); err != nil {
		return err
	}
	ctx, cancel = keyValueContext()
	_, staleErr := keyValue.Update(ctx, kvGoKey, []byte("stale"), goRevision)
	cancel()
	if staleErr == nil {
		return errors.New("stale Go Key-Value update unexpectedly succeeded")
	}
	ctx, cancel = keyValueContext()
	history, err := keyValue.History(ctx, kvGoKey)
	cancel()
	if err != nil {
		return fmt.Errorf("read Go Key-Value history: %w", err)
	}
	if len(history) != 2 {
		return fmt.Errorf("Go Key-Value history had %d entries, expected 2", len(history))
	}
	if err := checkKeyValueEntry("Go Key-Value history first", history[0], config.bucket, kvGoKey, "from-go", 1, natsjetstream.KeyValuePut); err != nil {
		return err
	}
	if err := checkKeyValueEntry("Go Key-Value history second", history[1], config.bucket, kvGoKey, "from-ocaml", 2, natsjetstream.KeyValuePut); err != nil {
		return err
	}
	if err := respondKeyValueControl(connection, "OCaml Key-Value update", updatedMessage, "go-validated"); err != nil {
		return err
	}

	deletedMessage, err := waitMessage("OCaml Key-Value delete", ocamlDeletedMessages)
	if err != nil {
		return err
	}
	if err := validateKeyValueControl("OCaml Key-Value delete", "deleted", deletedMessage); err != nil {
		return err
	}
	ctx, cancel = keyValueContext()
	_, err = keyValue.Get(ctx, kvGoKey)
	cancel()
	if !errors.Is(err, natsjetstream.ErrKeyNotFound) {
		if err == nil {
			return errors.New("deleted Go Key-Value entry was still visible")
		}
		return fmt.Errorf("deleted Go Key-Value entry returned %w", err)
	}
	ctx, cancel = keyValueContext()
	history, err = keyValue.History(ctx, kvGoKey)
	cancel()
	if err != nil {
		return fmt.Errorf("read deleted Go Key-Value history: %w", err)
	}
	if len(history) != 3 {
		return fmt.Errorf("deleted Go Key-Value history had %d entries, expected 3", len(history))
	}
	if err := checkKeyValueEntry("Go Key-Value delete history", history[2], config.bucket, kvGoKey, "", 3, natsjetstream.KeyValueDelete); err != nil {
		return err
	}
	if err := respondKeyValueControl(connection, "OCaml Key-Value delete", deletedMessage, "delete-validated"); err != nil {
		return err
	}

	watchContext, watchCancel := context.WithCancel(context.Background())
	watcher, err := keyValue.Watch(watchContext, kvWatchOCamlKey, natsjetstream.UpdatesOnly())
	if err != nil {
		watchCancel()
		return fmt.Errorf("watch OCaml Key-Value entry: %w", err)
	}
	watchStopped := false
	defer func() {
		watchCancel()
		if !watchStopped {
			_ = watcher.Stop()
		}
	}()
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush Go Key-Value watch: %w", err)
	}
	watchReadyMessage, err := waitMessage("OCaml Key-Value watch readiness", goWatchReadyMessages)
	if err != nil {
		return err
	}
	if err := respondKeyValueControl(connection, "OCaml Key-Value watch readiness", watchReadyMessage, "ready"); err != nil {
		return err
	}
	watchWrittenMessage, err := waitMessage("OCaml Key-Value watch update", ocamlWatchWrittenMessages)
	if err != nil {
		return err
	}
	if err := validateKeyValueControl("OCaml Key-Value watch update", "written", watchWrittenMessage); err != nil {
		return err
	}
	watchEntry, err := waitKeyValueUpdate("OCaml Key-Value watch update", watcher)
	if err != nil {
		return err
	}
	if err := checkKeyValueEntry("OCaml Key-Value watch entry", watchEntry, config.bucket, kvWatchOCamlKey, "from-ocaml-watch", 4, natsjetstream.KeyValuePut); err != nil {
		return err
	}
	if err := respondKeyValueControl(connection, "OCaml Key-Value watch update", watchWrittenMessage, "watch-seen"); err != nil {
		return err
	}
	if err := watcher.Stop(); err != nil {
		return fmt.Errorf("stop Go Key-Value watch: %w", err)
	}
	watchStopped = true

	goWriteMessage, err := waitMessage("OCaml request for Go Key-Value write", goWriteMessages)
	if err != nil {
		return err
	}
	if err := validateKeyValueControl("OCaml request for Go Key-Value write", "write", goWriteMessage); err != nil {
		return err
	}
	ctx, cancel = keyValueContext()
	goWatchRevision, err := keyValue.Put(ctx, kvWatchGoKey, []byte("from-go-watch"))
	cancel()
	if err != nil {
		return fmt.Errorf("put Go watch Key-Value entry: %w", err)
	}
	if goWatchRevision != 5 {
		return fmt.Errorf("Go watch Key-Value entry revision was %d, expected 5", goWatchRevision)
	}
	if err := respondKeyValueControl(connection, "OCaml request for Go Key-Value write", goWriteMessage, "written"); err != nil {
		return err
	}

	purgeMessage, err := waitMessage("OCaml Key-Value purge setup", ocamlPurgeReadyMessages)
	if err != nil {
		return err
	}
	if err := validateKeyValueControl("OCaml Key-Value purge setup", "purge", purgeMessage); err != nil {
		return err
	}
	ctx, cancel = keyValueContext()
	if err := keyValue.Purge(ctx, kvPurgeKey, natsjetstream.LastRevision(7)); err != nil {
		cancel()
		return fmt.Errorf("purge OCaml Key-Value entry: %w", err)
	}
	cancel()
	ctx, cancel = keyValueContext()
	history, err = keyValue.History(ctx, kvPurgeKey)
	cancel()
	if err != nil {
		return fmt.Errorf("read purged Key-Value history: %w", err)
	}
	if len(history) != 1 {
		return fmt.Errorf("purged Key-Value history had %d entries, expected 1", len(history))
	}
	if err := checkKeyValueEntry("Go Key-Value purge history", history[0], config.bucket, kvPurgeKey, "", 8, natsjetstream.KeyValuePurge); err != nil {
		return err
	}
	if err := respondKeyValueControl(connection, "OCaml Key-Value purge setup", purgeMessage, "purged"); err != nil {
		return err
	}

	purgeDeletesPutRevision := uint64(9)
	if markerTTLSupported {
		ocamlTTLMessage, err := waitMessage("OCaml Key-Value TTL setup", ocamlTTLReadyMessages)
		if err != nil {
			return err
		}
		if err := validateKeyValueControl("OCaml Key-Value TTL setup", "created", ocamlTTLMessage); err != nil {
			return err
		}
		if err := checkKeyValueMessage("OCaml Key-Value TTL entry", stream, config.bucket, kvOCamlTTLKey, time.Minute, ""); err != nil {
			return err
		}
		if err := respondKeyValueControl(connection, "OCaml Key-Value TTL setup", ocamlTTLMessage, "go-ttl-validated"); err != nil {
			return err
		}

		goTTLMessage, err := waitMessage("Go Key-Value TTL setup", goTTLReadyMessages)
		if err != nil {
			return err
		}
		if err := validateKeyValueControl("Go Key-Value TTL setup", "write", goTTLMessage); err != nil {
			return err
		}
		ctx, cancel = keyValueContext()
		goTTLRevision, err := keyValue.Create(ctx, kvGoTTLKey, []byte("from-go-ttl"), natsjetstream.KeyTTL(time.Minute))
		cancel()
		if err != nil {
			return fmt.Errorf("create Go TTL Key-Value entry: %w", err)
		}
		if goTTLRevision != 10 {
			return fmt.Errorf("Go TTL Key-Value entry revision was %d, expected 10", goTTLRevision)
		}
		if err := respondKeyValueControl(connection, "Go Key-Value TTL setup", goTTLMessage, "written"); err != nil {
			return err
		}

		ocamlPurgeTTLMessage, err := waitMessage("OCaml Key-Value purge TTL", ocamlPurgeTTLReadyMessages)
		if err != nil {
			return err
		}
		if err := validateKeyValueControl("OCaml Key-Value purge TTL", "purged", ocamlPurgeTTLMessage); err != nil {
			return err
		}
		if err := checkKeyValueMessage("OCaml Key-Value purge TTL marker", stream, config.bucket, kvOCamlTTLKey, time.Minute, "PURGE"); err != nil {
			return err
		}
		if err := respondKeyValueControl(connection, "OCaml Key-Value purge TTL", ocamlPurgeTTLMessage, "go-purge-ttl-validated"); err != nil {
			return err
		}

		goPurgeTTLMessage, err := waitMessage("Go Key-Value purge TTL", goPurgeTTLMessages)
		if err != nil {
			return err
		}
		if err := validateKeyValueControl("Go Key-Value purge TTL", "purge", goPurgeTTLMessage); err != nil {
			return err
		}
		ctx, cancel = keyValueContext()
		if err := keyValue.Purge(ctx, kvGoTTLKey, natsjetstream.LastRevision(goTTLRevision), natsjetstream.PurgeTTL(time.Minute)); err != nil {
			cancel()
			return fmt.Errorf("purge Go TTL Key-Value entry: %w", err)
		}
		cancel()
		if err := respondKeyValueControl(connection, "Go Key-Value purge TTL", goPurgeTTLMessage, "purged"); err != nil {
			return err
		}
		purgeDeletesPutRevision = 13
	}

	purgeDeletesMessage, err := waitMessage("Go Key-Value purge-deletes setup", goPurgeDeletesReadyMessages)
	if err != nil {
		return err
	}
	if err := validateKeyValueControl("Go Key-Value purge-deletes setup", "prepare", purgeDeletesMessage); err != nil {
		return err
	}
	ctx, cancel = keyValueContext()
	purgeDeletesRevision, err := keyValue.Put(ctx, kvPurgeDeletesKey, []byte("before-purge-deletes"))
	cancel()
	if err != nil {
		return fmt.Errorf("put purge-deletes Key-Value entry: %w", err)
	}
	if purgeDeletesRevision != purgeDeletesPutRevision {
		return fmt.Errorf("purge-deletes setup revision was %d, expected %d", purgeDeletesRevision, purgeDeletesPutRevision)
	}
	ctx, cancel = keyValueContext()
	err = keyValue.Delete(ctx, kvPurgeDeletesKey, natsjetstream.LastRevision(purgeDeletesRevision))
	cancel()
	if err != nil {
		return fmt.Errorf("delete purge-deletes Key-Value entry: %w", err)
	}
	ctx, cancel = keyValueContext()
	purgeDeletesHistory, err := keyValue.History(ctx, kvPurgeDeletesKey)
	cancel()
	if err != nil {
		return fmt.Errorf("read purge-deletes setup history: %w", err)
	}
	if len(purgeDeletesHistory) != 2 {
		return fmt.Errorf("purge-deletes setup history had %d entries, expected 2", len(purgeDeletesHistory))
	}
	purgeDeletesRevision = purgeDeletesHistory[1].Revision()
	purgeDeletesMarkerRevision := purgeDeletesPutRevision + 1
	if purgeDeletesRevision != purgeDeletesMarkerRevision {
		return fmt.Errorf("purge-deletes marker revision was %d, expected %d", purgeDeletesRevision, purgeDeletesMarkerRevision)
	}
	if err := respondKeyValueControl(connection, "Go Key-Value purge-deletes setup", purgeDeletesMessage, "prepared"); err != nil {
		return err
	}

	purgeDeletesDoneMessage, err := waitMessage("OCaml Key-Value purge-deletes completion", ocamlPurgeDeletesDoneMessages)
	if err != nil {
		return err
	}
	if err := validateKeyValueControl("OCaml Key-Value purge-deletes completion", "done", purgeDeletesDoneMessage); err != nil {
		return err
	}
	ctx, cancel = keyValueContext()
	_, err = keyValue.Get(ctx, kvPurgeDeletesKey)
	cancel()
	if !errors.Is(err, natsjetstream.ErrKeyNotFound) {
		if err == nil {
			return errors.New("purge-deletes marker was still visible")
		}
		return fmt.Errorf("purge-deletes marker lookup returned %w", err)
	}
	if err := respondKeyValueControl(connection, "OCaml Key-Value purge-deletes completion", purgeDeletesDoneMessage, "go-validated"); err != nil {
		return err
	}

	doneMessage, err := waitMessage("Key-Value completion", doneMessages)
	if err != nil {
		return err
	}
	if err := validateKeyValueControl("Key-Value completion", "done", doneMessage); err != nil {
		return err
	}
	ctx, cancel = keyValueContext()
	if err := jetstream.DeleteKeyValue(ctx, config.bucket); err != nil {
		cancel()
		return fmt.Errorf("delete Key-Value bucket: %w", err)
	}
	cancel()
	if err := respondKeyValueControl(connection, "Key-Value completion", doneMessage, "go-validated"); err != nil {
		return err
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush Key-Value completion: %w", err)
	}
	if err := connection.Drain(); err != nil {
		return fmt.Errorf("drain Key-Value peer: %w", err)
	}
	return nil
}
