package main

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

func runServiceParentClosePeer(config options) error {
	closedFile, err := parentCloseFile()
	if err != nil {
		return err
	}
	authOptions, err := connectOptions()
	if err != nil {
		return err
	}
	connectionOptions := append([]nats.Option{nats.Name("ocaml-nats Service parent-close interop peer")}, authOptions...)
	connection, err := nats.Connect(config.server, connectionOptions...)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer connection.Close()

	startMessages := make(chan *nats.Msg, 1)
	if _, err := connection.Subscribe(config.prefix+".start", func(message *nats.Msg) {
		select {
		case startMessages <- message:
		default:
		}
	}); err != nil {
		return fmt.Errorf("subscribe Service parent-close start: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush Service parent-close subscriptions: %w", err)
	}
	if err := writeReadyFile(config.ready); err != nil {
		return err
	}

	startMessage, err := waitMessage("Service parent-close start request", startMessages)
	if err != nil {
		return err
	}
	if err := startMessage.Respond([]byte("started")); err != nil {
		return fmt.Errorf("respond to Service parent-close start: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush Service parent-close start response: %w", err)
	}

	endpoint := config.prefix + ".ocaml.echo"
	response, err := connection.Request(endpoint, []byte("before-close"), waitTimeout)
	if err != nil {
		return fmt.Errorf("request Service before parent close: %w", err)
	}
	if err := expectPayload("Service before parent close", response, "before-close-response"); err != nil {
		return err
	}

	closeResponse, err := connection.Request(config.prefix+".close", []byte("close"), waitTimeout)
	if err != nil {
		return fmt.Errorf("request parent-close control: %w", err)
	}
	if err := expectPayload("parent-close control", closeResponse, "closing"); err != nil {
		return err
	}

	if err := waitForFile(closedFile); err != nil {
		return err
	}
	_, err = connection.Request(endpoint, []byte("after-close"), waitTimeout)
	if !errors.Is(err, nats.ErrNoResponders) && !errors.Is(err, nats.ErrTimeout) {
		if err == nil {
			return errors.New("Service endpoint responded after parent connection close")
		}
		return fmt.Errorf("Service endpoint after parent connection close: %w", err)
	}
	if err := connection.Drain(); err != nil {
		return fmt.Errorf("drain Service parent-close peer: %w", err)
	}
	return nil
}

func parentCloseFile() (string, error) {
	path, ok := os.LookupEnv("NATS_TEST_INTEROP_PARENT_CLOSE_FILE")
	if !ok || path == "" {
		return "", errors.New("NATS_TEST_INTEROP_PARENT_CLOSE_FILE is required")
	}
	return path, nil
}

func waitForFile(path string) error {
	timer := time.NewTimer(waitTimeout)
	defer timer.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, err := os.Stat(path)
		if err == nil {
			return nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("stat %s: %w", path, err)
		}
		select {
		case <-timer.C:
			return fmt.Errorf("timed out waiting for %s", path)
		case <-ticker.C:
		}
	}
}

func writeReadyFile(path string) error {
	if path == "" {
		return errors.New("ready-file is required")
	}
	return os.WriteFile(path, []byte("ready\n"), 0600)
}
