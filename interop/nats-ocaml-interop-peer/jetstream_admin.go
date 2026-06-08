package main

import (
	"context"
	"fmt"
	"os"

	"github.com/nats-io/nats.go"
	js "github.com/nats-io/nats.go/jetstream"
)

const adminConsumerName = "ADMIN_GO"

func adminContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), waitTimeout)
}

func streamNameListed(lister js.StreamNameLister, expected string) (bool, error) {
	found := false
	for name := range lister.Name() {
		if name == expected {
			found = true
		}
	}
	if err := lister.Err(); err != nil {
		return false, err
	}
	return found, nil
}

func consumerNameListed(lister js.ConsumerNameLister, expected string) (bool, error) {
	found := false
	for name := range lister.Name() {
		if name == expected {
			found = true
		}
	}
	if err := lister.Err(); err != nil {
		return false, err
	}
	return found, nil
}

func verifyJetStreamAdmin(jetStream js.JetStream, config options) error {
	ctx, cancel := adminContext()
	account, err := jetStream.AccountInfo(ctx)
	cancel()
	if err != nil {
		return fmt.Errorf("account info: %w", err)
	}
	if account.Streams < 1 {
		return fmt.Errorf("account info reported %d streams, expected at least one", account.Streams)
	}
	if account.Consumers < 1 {
		return fmt.Errorf("account info reported %d consumers, expected at least one", account.Consumers)
	}

	ctx, cancel = adminContext()
	stream, err := jetStream.Stream(ctx, config.stream)
	cancel()
	if err != nil {
		return fmt.Errorf("stream lookup: %w", err)
	}
	ctx, cancel = adminContext()
	streamInfo, err := stream.Info(ctx)
	cancel()
	if err != nil {
		return fmt.Errorf("stream info: %w", err)
	}
	if streamInfo.Config.Name != config.stream {
		return fmt.Errorf("stream info named %q, expected %q", streamInfo.Config.Name, config.stream)
	}
	if streamInfo.Config.Description != "updated-by-ocaml" {
		return fmt.Errorf("stream description was %q, expected %q", streamInfo.Config.Description, "updated-by-ocaml")
	}

	ctx, cancel = adminContext()
	streamNames := jetStream.StreamNames(ctx)
	listed, listErr := streamNameListed(streamNames, config.stream)
	cancel()
	if listErr != nil {
		return fmt.Errorf("stream names: %w", listErr)
	}
	if !listed {
		return fmt.Errorf("stream names did not include %q", config.stream)
	}

	ctx, cancel = adminContext()
	streamBySubject, err := jetStream.StreamNameBySubject(ctx, config.prefix+".ocaml")
	cancel()
	if err != nil {
		return fmt.Errorf("stream name by subject: %w", err)
	}
	if streamBySubject != config.stream {
		return fmt.Errorf("stream name by subject returned %q, expected %q", streamBySubject, config.stream)
	}

	ctx, cancel = adminContext()
	consumer, err := stream.Consumer(ctx, adminConsumerName)
	cancel()
	if err != nil {
		return fmt.Errorf("consumer lookup: %w", err)
	}
	ctx, cancel = adminContext()
	consumerInfo, err := consumer.Info(ctx)
	cancel()
	if err != nil {
		return fmt.Errorf("consumer info: %w", err)
	}
	if consumerInfo.Name != adminConsumerName {
		return fmt.Errorf("consumer info named %q, expected %q", consumerInfo.Name, adminConsumerName)
	}
	if consumerInfo.Config.MaxDeliver != 5 {
		return fmt.Errorf("consumer max deliver was %d, expected 5", consumerInfo.Config.MaxDeliver)
	}

	ctx, cancel = adminContext()
	consumerNames := stream.ConsumerNames(ctx)
	listed, listErr = consumerNameListed(consumerNames, adminConsumerName)
	cancel()
	if listErr != nil {
		return fmt.Errorf("consumer names: %w", listErr)
	}
	if !listed {
		return fmt.Errorf("consumer names did not include %q", adminConsumerName)
	}

	batch, err := consumer.Fetch(1, js.FetchMaxWait(waitTimeout))
	if err != nil {
		return fmt.Errorf("fetch after reset: %w", err)
	}
	var message js.Msg
	for candidate := range batch.Messages() {
		if message != nil {
			return fmt.Errorf("fetch after reset returned more than one message")
		}
		message = candidate
	}
	if err := batch.Error(); err != nil {
		return fmt.Errorf("fetch after reset: %w", err)
	}
	if message == nil {
		return fmt.Errorf("fetch after reset returned no message")
	}
	metadata, err := message.Metadata()
	if err != nil {
		return fmt.Errorf("metadata after reset: %w", err)
	}
	if metadata.Sequence.Stream != 7 {
		return fmt.Errorf("reset delivered stream sequence %d, expected 7", metadata.Sequence.Stream)
	}
	if err := message.Ack(); err != nil {
		return fmt.Errorf("ack after reset: %w", err)
	}
	return nil
}

func runJetStreamAdminPeer(config options) error {
	authOptions, err := connectOptions()
	if err != nil {
		return err
	}
	connectionOptions := append([]nats.Option{nats.Name("ocaml-nats JetStream administration interop peer")}, authOptions...)
	connection, err := nats.Connect(config.server, connectionOptions...)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer connection.Close()

	jetStream, err := js.New(connection)
	if err != nil {
		return fmt.Errorf("JetStream context: %w", err)
	}
	ctx, cancel := adminContext()
	stream, err := jetStream.CreateStream(ctx, js.StreamConfig{
		Name:     config.stream,
		Subjects: []string{config.prefix + ".go", config.prefix + ".ocaml"},
		Storage:  js.MemoryStorage,
	})
	cancel()
	if err != nil {
		return fmt.Errorf("create stream: %w", err)
	}
	if stream.CachedInfo().Config.Name != config.stream {
		return fmt.Errorf("created stream %q, expected %q", stream.CachedInfo().Config.Name, config.stream)
	}

	ctx, cancel = adminContext()
	_, err = stream.CreateOrUpdateConsumer(ctx, js.ConsumerConfig{
		Name:          adminConsumerName,
		Durable:       adminConsumerName,
		DeliverPolicy: js.DeliverAllPolicy,
		AckPolicy:     js.AckExplicitPolicy,
		FilterSubject: config.prefix + ".ocaml",
		MemoryStorage: true,
	})
	cancel()
	if err != nil {
		return fmt.Errorf("create Go consumer: %w", err)
	}

	ctx, cancel = adminContext()
	for sequence := 1; sequence <= 10; sequence++ {
		if _, err := jetStream.Publish(ctx, config.prefix+".ocaml", []byte(fmt.Sprintf("admin-message-%d", sequence))); err != nil {
			cancel()
			return fmt.Errorf("publish administration message %d: %w", sequence, err)
		}
	}
	cancel()

	startMessages := make(chan *nats.Msg, 1)
	doneMessages := make(chan *nats.Msg, 1)
	_, err = connection.Subscribe(config.prefix+".start", func(message *nats.Msg) {
		startMessages <- message
	})
	if err != nil {
		return fmt.Errorf("subscribe start: %w", err)
	}
	_, err = connection.Subscribe(config.prefix+".admin-done", func(message *nats.Msg) {
		doneMessages <- message
	})
	if err != nil {
		return fmt.Errorf("subscribe administration completion: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush setup: %w", err)
	}
	if err := os.WriteFile(config.ready, []byte("ready\n"), 0600); err != nil {
		return fmt.Errorf("write ready file: %w", err)
	}

	startMessage, err := waitMessage("administration start request", startMessages)
	if err != nil {
		return err
	}
	if err := startMessage.Respond([]byte("started")); err != nil {
		return fmt.Errorf("respond to administration start: %w", err)
	}

	doneMessage, err := waitMessage("administration completion request", doneMessages)
	if err != nil {
		return err
	}
	if string(doneMessage.Data) != "done" {
		return fmt.Errorf("administration completion payload was %q, expected %q", string(doneMessage.Data), "done")
	}
	if err := verifyJetStreamAdmin(jetStream, config); err != nil {
		return err
	}
	if err := doneMessage.Respond([]byte("verified")); err != nil {
		return fmt.Errorf("respond to administration completion: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush administration completion: %w", err)
	}
	if err := connection.Drain(); err != nil {
		return fmt.Errorf("drain: %w", err)
	}
	return nil
}
