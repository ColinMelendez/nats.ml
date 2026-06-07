package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	natsjetstream "github.com/nats-io/nats.go/jetstream"
)

const (
	objectGoName         = "go.txt"
	objectOCamlName      = "ocaml.txt"
	objectGoWatchName    = "watch.go"
	objectOCamlWatchName = "watch.ocaml"
	objectGoLinkName     = "go-link"
	objectOCamlLinkName  = "ocaml-link"
)

func objectStoreContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), waitTimeout)
}

func validateObjectControl(label, expected string, message *nats.Msg) error {
	if string(message.Data) != expected {
		return fmt.Errorf("%s payload was %q, expected %q", label, string(message.Data), expected)
	}
	if message.Reply == "" {
		return fmt.Errorf("%s request had no reply subject", label)
	}
	return nil
}

func respondObjectControl(connection *nats.Conn, label string, message *nats.Msg, payload string) error {
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

func checkObjectStoreStatus(label string, status natsjetstream.ObjectStoreStatus, bucket string) error {
	if status.Bucket() != bucket {
		return fmt.Errorf("%s bucket was %q, expected %q", label, status.Bucket(), bucket)
	}
	if status.Description() != "interop" {
		return fmt.Errorf("%s description was %q, expected %q", label, status.Description(), "interop")
	}
	if status.Storage() != natsjetstream.MemoryStorage {
		return fmt.Errorf("%s did not use memory storage", label)
	}
	if status.BackingStore() != "JetStream" {
		return fmt.Errorf("%s backing store was %q, expected JetStream", label, status.BackingStore())
	}
	if status.Metadata()["owner"] != "interop" {
		return fmt.Errorf("%s owner metadata was %q, expected %q", label, status.Metadata()["owner"], "interop")
	}
	if status.Sealed() {
		return fmt.Errorf("%s was unexpectedly sealed", label)
	}
	return nil
}

func checkObjectInfo(label string, info *natsjetstream.ObjectInfo, bucket, name string, size uint64, chunks uint32) error {
	if info == nil {
		return fmt.Errorf("%s returned no object info", label)
	}
	if info.Bucket != bucket {
		return fmt.Errorf("%s bucket was %q, expected %q", label, info.Bucket, bucket)
	}
	if info.Name != name {
		return fmt.Errorf("%s name was %q, expected %q", label, info.Name, name)
	}
	if info.Size != size {
		return fmt.Errorf("%s size was %d, expected %d", label, info.Size, size)
	}
	if info.Chunks != chunks {
		return fmt.Errorf("%s chunks were %d, expected %d", label, info.Chunks, chunks)
	}
	return nil
}

func checkObjectLink(label string, info *natsjetstream.ObjectInfo, bucket, name string) error {
	if info == nil || info.Opts == nil || info.Opts.Link == nil {
		return fmt.Errorf("%s did not contain an object link", label)
	}
	if info.Opts.Link.Bucket != bucket {
		return fmt.Errorf("%s target bucket was %q, expected %q", label, info.Opts.Link.Bucket, bucket)
	}
	if info.Opts.Link.Name != name {
		return fmt.Errorf("%s target name was %q, expected %q", label, info.Opts.Link.Name, name)
	}
	return nil
}

func waitObjectUpdate(label string, watcher natsjetstream.ObjectWatcher, expectedName string) (*natsjetstream.ObjectInfo, error) {
	timer := time.NewTimer(waitTimeout)
	defer timer.Stop()
	for {
		select {
		case info, ok := <-watcher.Updates():
			if !ok || info == nil {
				return nil, fmt.Errorf("%s watcher closed before an update", label)
			}
			if info.Name == expectedName {
				return info, nil
			}
		case <-timer.C:
			return nil, fmt.Errorf("timed out waiting for %s", label)
		}
	}
}

func objectStoreHasName(objects []*natsjetstream.ObjectInfo, name string) bool {
	for _, object := range objects {
		if object != nil && object.Name == name {
			return true
		}
	}
	return false
}

func runJetStreamObjectPeer(config options) error {
	authOptions, err := connectOptions()
	if err != nil {
		return err
	}
	connectionOptions := append([]nats.Option{nats.Name("ocaml-nats Object Store interop peer")}, authOptions...)
	connection, err := nats.Connect(config.server, connectionOptions...)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer connection.Close()

	jetstream, err := natsjetstream.New(connection)
	if err != nil {
		return fmt.Errorf("JetStream context: %w", err)
	}
	ctx, cancel := objectStoreContext()
	objectStore, err := jetstream.CreateObjectStore(ctx, natsjetstream.ObjectStoreConfig{
		Bucket:      config.bucket,
		Description: "interop",
		MaxBytes:    1 << 20,
		Storage:     natsjetstream.MemoryStorage,
		Metadata:    map[string]string{"owner": "interop"},
	})
	cancel()
	if err != nil {
		return fmt.Errorf("create Object Store: %w", err)
	}
	if objectStore == nil {
		return errors.New("create Object Store returned no store")
	}
	storeDeleted := false
	defer func() {
		if !storeDeleted {
			cleanupContext, cleanupCancel := objectStoreContext()
			_ = jetstream.DeleteObjectStore(cleanupContext, config.bucket)
			cleanupCancel()
		}
	}()
	ctx, cancel = objectStoreContext()
	status, err := objectStore.Status(ctx)
	cancel()
	if err != nil {
		return fmt.Errorf("initial Object Store status: %w", err)
	}
	if err := checkObjectStoreStatus("initial Object Store status", status, config.bucket); err != nil {
		return err
	}

	startMessages := make(chan *nats.Msg, 1)
	ocamlWrittenMessages := make(chan *nats.Msg, 1)
	goWatchMessages := make(chan *nats.Msg, 1)
	ocamlWatchMessages := make(chan *nats.Msg, 1)
	goLinkMessages := make(chan *nats.Msg, 1)
	ocamlLinkMessages := make(chan *nats.Msg, 1)
	ocamlDeleteMessages := make(chan *nats.Msg, 1)
	ocamlSealedMessages := make(chan *nats.Msg, 1)
	doneMessages := make(chan *nats.Msg, 1)
	subscribe := func(suffix string, messages chan<- *nats.Msg) error {
		_, err := connection.Subscribe(config.prefix+suffix, func(message *nats.Msg) {
			messages <- message
		})
		return err
	}
	if err := subscribe(".start", startMessages); err != nil {
		return fmt.Errorf("subscribe Object Store start: %w", err)
	}
	if err := subscribe(".ocaml-written", ocamlWrittenMessages); err != nil {
		return fmt.Errorf("subscribe Object Store write barrier: %w", err)
	}
	if err := subscribe(".go-watch", goWatchMessages); err != nil {
		return fmt.Errorf("subscribe Object Store Go watch barrier: %w", err)
	}
	if err := subscribe(".ocaml-watch", ocamlWatchMessages); err != nil {
		return fmt.Errorf("subscribe Object Store OCaml watch barrier: %w", err)
	}
	if err := subscribe(".go-link", goLinkMessages); err != nil {
		return fmt.Errorf("subscribe Object Store Go link barrier: %w", err)
	}
	if err := subscribe(".ocaml-link", ocamlLinkMessages); err != nil {
		return fmt.Errorf("subscribe Object Store OCaml link barrier: %w", err)
	}
	if err := subscribe(".ocaml-delete", ocamlDeleteMessages); err != nil {
		return fmt.Errorf("subscribe Object Store delete barrier: %w", err)
	}
	if err := subscribe(".ocaml-sealed", ocamlSealedMessages); err != nil {
		return fmt.Errorf("subscribe Object Store seal barrier: %w", err)
	}
	if err := subscribe(".done", doneMessages); err != nil {
		return fmt.Errorf("subscribe Object Store completion: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush Object Store setup: %w", err)
	}
	if err := os.WriteFile(config.ready, []byte("ready\n"), 0600); err != nil {
		return fmt.Errorf("write ready file: %w", err)
	}

	startMessage, err := waitMessage("Object Store start request", startMessages)
	if err != nil {
		return err
	}
	if err := validateObjectControl("Object Store start", "start", startMessage); err != nil {
		return err
	}
	ctx, cancel = objectStoreContext()
	goInfo, err := objectStore.PutString(ctx, objectGoName, "from-go")
	cancel()
	if err != nil {
		return fmt.Errorf("put Go object: %w", err)
	}
	if err := checkObjectInfo("Go object", goInfo, config.bucket, objectGoName, 7, 1); err != nil {
		return err
	}
	if err := respondObjectControl(connection, "Object Store start", startMessage, "started"); err != nil {
		return err
	}

	ocamlWrittenMessage, err := waitMessage("OCaml Object Store write", ocamlWrittenMessages)
	if err != nil {
		return err
	}
	if err := validateObjectControl("OCaml Object Store write", "written", ocamlWrittenMessage); err != nil {
		return err
	}
	ctx, cancel = objectStoreContext()
	ocamlPayload, err := objectStore.GetString(ctx, objectOCamlName)
	cancel()
	if err != nil {
		return fmt.Errorf("get OCaml object: %w", err)
	}
	if ocamlPayload != "from-ocaml" {
		return fmt.Errorf("OCaml object payload was %q, expected %q", ocamlPayload, "from-ocaml")
	}
	ctx, cancel = objectStoreContext()
	ocamlInfo, err := objectStore.GetInfo(ctx, objectOCamlName)
	cancel()
	if err != nil {
		return fmt.Errorf("get OCaml object info: %w", err)
	}
	if err := checkObjectInfo("OCaml object", ocamlInfo, config.bucket, objectOCamlName, 10, 5); err != nil {
		return err
	}
	if ocamlInfo.Description != "from-ocaml" {
		return fmt.Errorf("OCaml object description was %q, expected %q", ocamlInfo.Description, "from-ocaml")
	}
	if ocamlInfo.Headers.Get("X-Object") != "ocaml" {
		return fmt.Errorf("OCaml object X-Object header was %q, expected %q", ocamlInfo.Headers.Get("X-Object"), "ocaml")
	}
	if ocamlInfo.Metadata["origin"] != "ocaml" {
		return fmt.Errorf("OCaml object origin metadata was %q, expected %q", ocamlInfo.Metadata["origin"], "ocaml")
	}
	if ocamlInfo.Opts == nil || ocamlInfo.Opts.ChunkSize != 2 {
		return fmt.Errorf("OCaml object chunk size was not 2")
	}
	if err := respondObjectControl(connection, "OCaml Object Store write", ocamlWrittenMessage, "go-validated"); err != nil {
		return err
	}

	goWatchMessage, err := waitMessage("OCaml request for Go Object Store watch write", goWatchMessages)
	if err != nil {
		return err
	}
	if err := validateObjectControl("OCaml request for Go Object Store watch write", "write", goWatchMessage); err != nil {
		return err
	}
	ctx, cancel = objectStoreContext()
	goWatchInfo, err := objectStore.PutString(ctx, objectGoWatchName, "from-go-watch")
	cancel()
	if err != nil {
		return fmt.Errorf("put Go watch object: %w", err)
	}
	if err := checkObjectInfo("Go watch object", goWatchInfo, config.bucket, objectGoWatchName, 13, 1); err != nil {
		return err
	}
	watchContext, watchCancel := context.WithCancel(context.Background())
	watcher, err := objectStore.Watch(watchContext, natsjetstream.UpdatesOnly())
	if err != nil {
		watchCancel()
		return fmt.Errorf("watch OCaml Object Store update: %w", err)
	}
	watchStopped := false
	defer func() {
		watchCancel()
		if !watchStopped {
			_ = watcher.Stop()
		}
	}()
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush Object Store watch: %w", err)
	}
	if err := respondObjectControl(connection, "OCaml request for Go Object Store watch write", goWatchMessage, "written"); err != nil {
		return err
	}

	ocamlWatchMessage, err := waitMessage("OCaml Object Store watch update", ocamlWatchMessages)
	if err != nil {
		return err
	}
	if err := validateObjectControl("OCaml Object Store watch update", "written", ocamlWatchMessage); err != nil {
		return err
	}
	watchInfo, err := waitObjectUpdate("OCaml Object Store watch update", watcher, objectOCamlWatchName)
	if err != nil {
		return err
	}
	if err := checkObjectInfo("OCaml Object Store watch update", watchInfo, config.bucket, objectOCamlWatchName, 16, 1); err != nil {
		return err
	}
	if err := respondObjectControl(connection, "OCaml Object Store watch update", ocamlWatchMessage, "watch-seen"); err != nil {
		return err
	}
	if err := watcher.Stop(); err != nil {
		return fmt.Errorf("stop Go Object Store watch: %w", err)
	}
	watchStopped = true

	goLinkMessage, err := waitMessage("OCaml request for Go Object Store link", goLinkMessages)
	if err != nil {
		return err
	}
	if err := validateObjectControl("OCaml request for Go Object Store link", "link", goLinkMessage); err != nil {
		return err
	}
	ctx, cancel = objectStoreContext()
	goTarget, err := objectStore.GetInfo(ctx, objectGoName)
	cancel()
	if err != nil {
		return fmt.Errorf("get Go link target: %w", err)
	}
	ctx, cancel = objectStoreContext()
	goLinkInfo, err := objectStore.AddLink(ctx, objectGoLinkName, goTarget)
	cancel()
	if err != nil {
		return fmt.Errorf("add Go object link: %w", err)
	}
	if err := checkObjectInfo("Go object link", goLinkInfo, config.bucket, objectGoLinkName, 0, 0); err != nil {
		return err
	}
	if err := checkObjectLink("Go object link", goLinkInfo, config.bucket, objectGoName); err != nil {
		return err
	}
	if err := respondObjectControl(connection, "OCaml request for Go Object Store link", goLinkMessage, "linked"); err != nil {
		return err
	}

	ocamlLinkMessage, err := waitMessage("OCaml Object Store link", ocamlLinkMessages)
	if err != nil {
		return err
	}
	if err := validateObjectControl("OCaml Object Store link", "linked", ocamlLinkMessage); err != nil {
		return err
	}
	ctx, cancel = objectStoreContext()
	ocamlLinkPayload, err := objectStore.GetString(ctx, objectOCamlLinkName)
	cancel()
	if err != nil {
		return fmt.Errorf("get OCaml object link: %w", err)
	}
	if ocamlLinkPayload != "from-ocaml" {
		return fmt.Errorf("OCaml object link payload was %q, expected %q", ocamlLinkPayload, "from-ocaml")
	}
	ctx, cancel = objectStoreContext()
	ocamlLinkInfo, err := objectStore.GetInfo(ctx, objectOCamlLinkName)
	cancel()
	if err != nil {
		return fmt.Errorf("get OCaml object link info: %w", err)
	}
	if err := checkObjectInfo("OCaml object link", ocamlLinkInfo, config.bucket, objectOCamlLinkName, 0, 0); err != nil {
		return err
	}
	if err := checkObjectLink("OCaml object link", ocamlLinkInfo, config.bucket, objectOCamlName); err != nil {
		return err
	}
	if err := respondObjectControl(connection, "OCaml Object Store link", ocamlLinkMessage, "go-validated"); err != nil {
		return err
	}

	ocamlDeleteMessage, err := waitMessage("OCaml Object Store delete", ocamlDeleteMessages)
	if err != nil {
		return err
	}
	if err := validateObjectControl("OCaml Object Store delete", "deleted", ocamlDeleteMessage); err != nil {
		return err
	}
	ctx, cancel = objectStoreContext()
	_, err = objectStore.GetInfo(ctx, objectGoName)
	cancel()
	if !errors.Is(err, natsjetstream.ErrObjectNotFound) {
		if err == nil {
			return errors.New("deleted Go object was still visible")
		}
		return fmt.Errorf("deleted Go object returned %w", err)
	}
	ctx, cancel = objectStoreContext()
	deletedInfo, err := objectStore.GetInfo(ctx, objectGoName, natsjetstream.GetObjectInfoShowDeleted())
	cancel()
	if err != nil {
		return fmt.Errorf("get deleted Go object info: %w", err)
	}
	if !deletedInfo.Deleted {
		return errors.New("deleted Go object info was not marked deleted")
	}
	if deletedInfo.Size != 0 || deletedInfo.Chunks != 0 || deletedInfo.Digest != "" {
		return errors.New("deleted Go object info retained content metadata")
	}
	ctx, cancel = objectStoreContext()
	objects, err := objectStore.List(ctx)
	cancel()
	if err != nil {
		return fmt.Errorf("list Object Store after delete: %w", err)
	}
	if objectStoreHasName(objects, objectGoName) {
		return errors.New("deleted Go object was returned by Object Store list")
	}
	if !objectStoreHasName(objects, objectOCamlName) || !objectStoreHasName(objects, objectGoLinkName) || !objectStoreHasName(objects, objectOCamlLinkName) {
		return errors.New("Object Store list omitted a live object or link")
	}
	if err := respondObjectControl(connection, "OCaml Object Store delete", ocamlDeleteMessage, "go-validated"); err != nil {
		return err
	}

	ocamlSealedMessage, err := waitMessage("OCaml Object Store seal", ocamlSealedMessages)
	if err != nil {
		return err
	}
	if err := validateObjectControl("OCaml Object Store seal", "sealed", ocamlSealedMessage); err != nil {
		return err
	}
	ctx, cancel = objectStoreContext()
	status, err = objectStore.Status(ctx)
	cancel()
	if err != nil {
		return fmt.Errorf("sealed Object Store status: %w", err)
	}
	if !status.Sealed() {
		return errors.New("Object Store status was not sealed")
	}
	ctx, cancel = objectStoreContext()
	_, err = objectStore.PutString(ctx, "after-seal", "rejected")
	cancel()
	if err == nil {
		return errors.New("Object Store accepted a write after sealing")
	}
	if err := respondObjectControl(connection, "OCaml Object Store seal", ocamlSealedMessage, "go-validated"); err != nil {
		return err
	}

	doneMessage, err := waitMessage("Object Store completion", doneMessages)
	if err != nil {
		return err
	}
	if err := validateObjectControl("Object Store completion", "done", doneMessage); err != nil {
		return err
	}
	ctx, cancel = objectStoreContext()
	err = jetstream.DeleteObjectStore(ctx, config.bucket)
	cancel()
	if err != nil {
		return fmt.Errorf("delete Object Store: %w", err)
	}
	storeDeleted = true
	if err := respondObjectControl(connection, "Object Store completion", doneMessage, "go-validated"); err != nil {
		return err
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush Object Store completion: %w", err)
	}
	if err := connection.Drain(); err != nil {
		return fmt.Errorf("drain Object Store peer: %w", err)
	}
	return nil
}
