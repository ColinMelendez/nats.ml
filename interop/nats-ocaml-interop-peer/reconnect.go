package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

func waitReconnect(messages <-chan struct{}) error {
	timer := time.NewTimer(waitTimeout)
	defer timer.Stop()
	select {
	case <-messages:
		return nil
	case <-timer.C:
		return fmt.Errorf("timed out waiting for reconnect")
	}
}

func expectPayload(label string, message *nats.Msg, expected string) error {
	actual := string(message.Data)
	if actual != expected {
		return fmt.Errorf("%s payload was %q, expected %q", label, actual, expected)
	}
	return nil
}

func runReconnectPeer(config options) error {
	authOptions, err := connectOptions()
	if err != nil {
		return err
	}
	reconnected := make(chan struct{}, 1)
	connectionOptions := []nats.Option{
		nats.Name("ocaml-nats interop reconnect peer"),
		nats.DontRandomize(),
		nats.ReconnectWait(100 * time.Millisecond),
		nats.MaxReconnects(20),
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

	startMessages := make(chan *nats.Msg, 1)
	toGoMessages := make(chan *nats.Msg, 2)
	reconnectReadyMessages := make(chan *nats.Msg, 1)
	_, err = connection.Subscribe(config.prefix+".start", func(message *nats.Msg) {
		startMessages <- message
	})
	if err != nil {
		return fmt.Errorf("subscribe start: %w", err)
	}
	_, err = connection.Subscribe(config.prefix+".to-go", func(message *nats.Msg) {
		toGoMessages <- message
	})
	if err != nil {
		return fmt.Errorf("subscribe to-go: %w", err)
	}
	_, err = connection.Subscribe(config.prefix+".reconnect-ready", func(message *nats.Msg) {
		reconnectReadyMessages <- message
	})
	if err != nil {
		return fmt.Errorf("subscribe reconnect-ready: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush subscriptions: %w", err)
	}
	if err := os.WriteFile(config.ready, []byte("ready\n"), 0600); err != nil {
		return fmt.Errorf("write ready file: %w", err)
	}

	startMessage, err := waitMessage("start request", startMessages)
	if err != nil {
		return err
	}
	if err := startMessage.Respond([]byte("started")); err != nil {
		return fmt.Errorf("respond to start: %w", err)
	}

	fromGo := nats.NewMsg(config.prefix + ".from-go")
	fromGo.Data = []byte("before")
	if err := connection.PublishMsg(fromGo); err != nil {
		return fmt.Errorf("publish before reconnect: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush before reconnect: %w", err)
	}

	toGo, err := waitMessage("OCaml publication before reconnect", toGoMessages)
	if err != nil {
		return err
	}
	if err := expectPayload("OCaml publication before reconnect", toGo, "before"); err != nil {
		return err
	}
	baseline, err := waitMessage("OCaml baseline barrier", toGoMessages)
	if err != nil {
		return err
	}
	if err := expectPayload("OCaml baseline barrier", baseline, "before-flushed"); err != nil {
		return err
	}
	if err := os.WriteFile(config.signal, []byte("kill-primary\n"), 0600); err != nil {
		return fmt.Errorf("write reconnect signal: %w", err)
	}
	if err := waitReconnect(reconnected); err != nil {
		return err
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush after reconnect: %w", err)
	}
	ready, err := waitMessage("OCaml recovery barrier", reconnectReadyMessages)
	if err != nil {
		return err
	}
	if err := expectPayload("OCaml recovery barrier", ready, "ocaml-ready"); err != nil {
		return err
	}
	if err := ready.Respond([]byte("go-ready")); err != nil {
		return fmt.Errorf("respond to OCaml recovery barrier: %w", err)
	}

	fromGo = nats.NewMsg(config.prefix + ".from-go")
	fromGo.Data = []byte("after")
	if err := connection.PublishMsg(fromGo); err != nil {
		return fmt.Errorf("publish after reconnect: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush after reconnect: %w", err)
	}
	toGo, err = waitMessage("OCaml publication after reconnect", toGoMessages)
	if err != nil {
		return err
	}
	if err := expectPayload("OCaml publication after reconnect", toGo, "after"); err != nil {
		return err
	}
	if err := connection.Drain(); err != nil {
		return fmt.Errorf("drain: %w", err)
	}
	return nil
}

func runMode(config options) error {
	switch config.mode {
	case "core":
		return runPeer(config)
	case "reconnect":
		if config.signal == "" {
			return fmt.Errorf("signal-file is required in reconnect mode")
		}
		return runReconnectPeer(config)
	default:
		return fmt.Errorf("unknown mode %q", config.mode)
	}
}

func validateOptions(config options) error {
	if config.server == "" || config.prefix == "" || config.ready == "" ||
		strings.ContainsAny(config.prefix, " \r\n") {
		return fmt.Errorf("server, prefix, and ready-file are required; prefix may not contain whitespace")
	}
	return nil
}
