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

func reconnectCycles(server string) (int, error) {
	servers := strings.Split(server, ",")
	if len(servers) < 2 {
		return 0, fmt.Errorf("reconnect mode requires at least two servers")
	}
	for _, server := range servers {
		if strings.TrimSpace(server) == "" {
			return 0, fmt.Errorf("reconnect server list contains an empty endpoint")
		}
	}
	return len(servers) - 1, nil
}

func roundPayload(round int) string {
	return fmt.Sprintf("round-%d", round)
}

func roundMarker(round int) string {
	return fmt.Sprintf("%s-flushed", roundPayload(round))
}

func roundSignal(signal string, round int) string {
	return fmt.Sprintf("%s.%d", signal, round)
}

func awaitRecoveryBarrier(connection *nats.Conn, prefix string, cycle int) error {
	readySubject := fmt.Sprintf("%s.reconnect-ready.%d", prefix, cycle)
	expected := fmt.Sprintf("ocaml-ready-%d", cycle)
	responsePayload := []byte(fmt.Sprintf("go-ready-%d", cycle))
	ready := make(chan struct{}, 1)
	callbackErrors := make(chan error, 1)
	_, err := connection.Subscribe(readySubject, func(message *nats.Msg) {
		if string(message.Data) != expected {
			select {
			case callbackErrors <- fmt.Errorf(
				"recovery barrier payload was %q, expected %q",
				string(message.Data), expected):
			default:
			}
			return
		}
		if message.Reply == "" {
			select {
			case callbackErrors <- fmt.Errorf("recovery barrier has no reply subject"):
			default:
			}
			return
		}
		response := nats.NewMsg(message.Reply)
		response.Data = responsePayload
		if err := connection.PublishMsg(response); err != nil {
			select {
			case callbackErrors <- fmt.Errorf("publish recovery barrier response: %w", err):
			default:
			}
			return
		}
		select {
		case ready <- struct{}{}:
		default:
		}
	})
	if err != nil {
		return fmt.Errorf("subscribe recovery barrier %d: %w", cycle, err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush recovery barrier %d: %w", cycle, err)
	}
	timer := time.NewTimer(waitTimeout)
	defer timer.Stop()
	select {
	case <-ready:
		if err := connection.Flush(); err != nil {
			return fmt.Errorf("flush recovery barrier response %d: %w", cycle, err)
		}
		return nil
	case err := <-callbackErrors:
		return err
	case <-timer.C:
		return fmt.Errorf("timed out waiting for recovery barrier %d", cycle)
	}
}

func runReconnectPeer(config options) error {
	authOptions, err := connectOptions()
	if err != nil {
		return err
	}
	cycles, err := reconnectCycles(config.server)
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

	for round := 0; round <= cycles; round++ {
		payload := roundPayload(round)
		fromGo := nats.NewMsg(config.prefix + ".from-go")
		fromGo.Data = []byte(payload)
		if err := connection.PublishMsg(fromGo); err != nil {
			return fmt.Errorf("publish %s: %w", payload, err)
		}
		if err := connection.Flush(); err != nil {
			return fmt.Errorf("flush %s: %w", payload, err)
		}

		toGo, err := waitMessage("OCaml publication "+payload, toGoMessages)
		if err != nil {
			return err
		}
		if err := expectPayload("OCaml publication "+payload, toGo, payload); err != nil {
			return err
		}
		marker, err := waitMessage("OCaml flush marker "+payload, toGoMessages)
		if err != nil {
			return err
		}
		if err := expectPayload("OCaml flush marker "+payload, marker, roundMarker(round)); err != nil {
			return err
		}

		if round == cycles {
			break
		}
		signal := roundSignal(config.signal, round+1)
		if err := os.WriteFile(signal, []byte("kill\n"), 0600); err != nil {
			return fmt.Errorf("write reconnect signal %d: %w", round+1, err)
		}
		if err := waitReconnect(reconnected); err != nil {
			return fmt.Errorf("reconnect %d: %w", round+1, err)
		}
		if err := connection.Flush(); err != nil {
			return fmt.Errorf("flush after reconnect %d: %w", round+1, err)
		}
		if err := awaitRecoveryBarrier(connection, config.prefix, round+1); err != nil {
			return err
		}
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
	case "jetstream":
		if config.stream == "" {
			return fmt.Errorf("stream is required in jetstream mode")
		}
		return runJetStreamPeer(config)
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
