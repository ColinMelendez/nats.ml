package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/nats-io/nats.go"
)

const serviceFailureBurstSize = 8

func runServiceFailurePeer(config options) error {
	authOptions, err := connectOptions()
	if err != nil {
		return err
	}
	connectionOptions := append([]nats.Option{nats.Name("ocaml-nats Service failure interop peer")}, authOptions...)
	connection, err := nats.Connect(config.server, connectionOptions...)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer connection.Close()

	startMessages := make(chan *nats.Msg, 1)
	firstStartedMessages := make(chan *nats.Msg, 1)
	if _, err := connection.Subscribe(config.prefix+".start", func(message *nats.Msg) {
		select {
		case startMessages <- message:
		default:
		}
	}); err != nil {
		return fmt.Errorf("subscribe Service failure start: %w", err)
	}
	if _, err := connection.Subscribe(config.prefix+".first-started", func(message *nats.Msg) {
		select {
		case firstStartedMessages <- message:
		default:
		}
	}); err != nil {
		return fmt.Errorf("subscribe Service failure first-handler barrier: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush Service failure subscriptions: %w", err)
	}
	if err := os.WriteFile(config.ready, []byte("ready\n"), 0600); err != nil {
		return fmt.Errorf("write ready file: %w", err)
	}

	startMessage, err := waitMessage("Service failure start request", startMessages)
	if err != nil {
		return err
	}
	if err := startMessage.Respond([]byte("started")); err != nil {
		return fmt.Errorf("respond to Service failure start: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush Service failure start response: %w", err)
	}

	first := nats.NewMsg(config.prefix + ".ocaml.limited")
	first.Data = []byte("first")
	if err := connection.PublishMsg(first); err != nil {
		return fmt.Errorf("publish first slow-consumer request: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush first slow-consumer request: %w", err)
	}

	firstStarted, err := waitMessage("OCaml first Service failure handler", firstStartedMessages)
	if err != nil {
		return err
	}
	if err := expectPayload("OCaml first Service failure handler", firstStarted, "started"); err != nil {
		return err
	}
	if err := firstStarted.Respond([]byte("first-started")); err != nil {
		return fmt.Errorf("respond to first-handler barrier: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush first-handler barrier: %w", err)
	}

	for index := 1; index < serviceFailureBurstSize; index++ {
		message := nats.NewMsg(config.prefix + ".ocaml.limited")
		message.Data = []byte("overflow-" + strconv.Itoa(index))
		if err := connection.PublishMsg(message); err != nil {
			return fmt.Errorf("publish slow-consumer request %d: %w", index, err)
		}
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush slow-consumer burst: %w", err)
	}

	releaseResponse, err := connection.Request(config.prefix+".release", []byte("release"), waitTimeout)
	if err != nil {
		return fmt.Errorf("request Service failure release: %w", err)
	}
	if err := expectPayload("Service failure release", releaseResponse, "released"); err != nil {
		return err
	}

	parentResponse, err := connection.Request(config.prefix+".parent-check", []byte("usable"), waitTimeout)
	if err != nil {
		return fmt.Errorf("request parent-connection check: %w", err)
	}
	if err := expectPayload("parent-connection check", parentResponse, "parent-usable"); err != nil {
		return err
	}

	doneResponse, err := connection.Request(config.prefix+".done", []byte("go-finished"), waitTimeout)
	if err != nil {
		return fmt.Errorf("request Service failure completion: %w", err)
	}
	if err := expectPayload("Service failure completion", doneResponse, "ocaml-validated"); err != nil {
		return err
	}
	if err := connection.Drain(); err != nil {
		return fmt.Errorf("drain Service failure peer: %w", err)
	}
	return nil
}
