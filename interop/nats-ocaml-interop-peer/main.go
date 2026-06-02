package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

const waitTimeout = 10 * time.Second

type options struct {
	server string
	prefix string
	stream string
	ready  string
	signal string
	mode   string
}

func waitMessage(label string, messages <-chan *nats.Msg) (*nats.Msg, error) {
	timer := time.NewTimer(waitTimeout)
	defer timer.Stop()
	select {
	case message := <-messages:
		return message, nil
	case <-timer.C:
		return nil, fmt.Errorf("timed out waiting for %s", label)
	}
}

func waitResult(label string, results <-chan error) error {
	timer := time.NewTimer(waitTimeout)
	defer timer.Stop()
	select {
	case err := <-results:
		if err != nil {
			return err
		}
		return nil
	case <-timer.C:
		return fmt.Errorf("timed out waiting for %s", label)
	}
}

func checkHeaderValues(message *nats.Msg, name string, expected []string) error {
	actual := message.Header.Values(name)
	if len(actual) != len(expected) {
		return fmt.Errorf("%s header had %d values, expected %d", name, len(actual), len(expected))
	}
	for index, value := range actual {
		if value != expected[index] {
			return fmt.Errorf("%s header value %d was %q, expected %q", name, index, value, expected[index])
		}
	}
	return nil
}

func connectOptions() ([]nats.Option, error) {
	token, tokenSet := os.LookupEnv("NATS_TEST_TOKEN")
	user, userSet := os.LookupEnv("NATS_TEST_USER")
	password, passwordSet := os.LookupEnv("NATS_TEST_PASS")
	var options []nats.Option
	switch {
	case tokenSet && (userSet || passwordSet):
		return nil, errors.New("NATS_TEST_TOKEN cannot be combined with username/password")
	case tokenSet:
		if token == "" {
			return nil, errors.New("NATS_TEST_TOKEN must be non-empty")
		}
		options = append(options, nats.Token(token))
	case userSet || passwordSet:
		if user == "" || password == "" {
			return nil, errors.New("NATS_TEST_USER and NATS_TEST_PASS must both be non-empty")
		}
		options = append(options, nats.UserInfo(user, password))
	}
	if caFile, caSet := os.LookupEnv("NATS_TEST_TLS_CA"); caSet {
		if caFile == "" {
			return nil, errors.New("NATS_TEST_TLS_CA must be non-empty")
		}
		options = append(options, nats.RootCAs(caFile))
	}
	return options, nil
}

func runPeer(config options) error {
	authOptions, err := connectOptions()
	if err != nil {
		return err
	}
	connectionOptions := append([]nats.Option{nats.Name("ocaml-nats interop peer")}, authOptions...)
	connection, err := nats.Connect(config.server, connectionOptions...)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer connection.Close()

	startMessages := make(chan *nats.Msg, 1)
	toGoMessages := make(chan *nats.Msg, 1)
	goRequestResults := make(chan error, 1)

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
	_, err = connection.Subscribe(config.prefix+".go-request", func(message *nats.Msg) {
		if string(message.Data) != "request-from-ocaml" {
			goRequestResults <- fmt.Errorf("OCaml request payload was %q, expected %q", string(message.Data), "request-from-ocaml")
			return
		}
		if message.Header.Get("X-Interop") != "ocaml-request" {
			goRequestResults <- fmt.Errorf("OCaml request X-Interop header was %q, expected %q", message.Header.Get("X-Interop"), "ocaml-request")
			return
		}
		response := nats.NewMsg(message.Reply)
		response.Data = []byte("response-from-go")
		response.Header.Set("X-Interop", "go-response")
		if publishError := connection.PublishMsg(response); publishError != nil {
			goRequestResults <- fmt.Errorf("respond to OCaml request: %w", publishError)
			return
		}
		goRequestResults <- nil
	})
	if err != nil {
		return fmt.Errorf("subscribe go-request: %w", err)
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
	fromGo.Data = []byte("from-go")
	fromGo.Header.Set("X-Interop", "go")
	fromGo.Header.Add("X-Trace", "one")
	fromGo.Header.Add("X-Trace", "two")
	if err := connection.PublishMsg(fromGo); err != nil {
		return fmt.Errorf("publish from-go: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush from-go: %w", err)
	}

	toGo, err := waitMessage("OCaml publication", toGoMessages)
	if err != nil {
		return err
	}
	if string(toGo.Data) != "to-go" {
		return fmt.Errorf("OCaml payload was %q, expected %q", string(toGo.Data), "to-go")
	}
	if toGo.Header.Get("X-Interop") != "ocaml" {
		return fmt.Errorf("OCaml X-Interop header was %q", toGo.Header.Get("X-Interop"))
	}
	if err := checkHeaderValues(toGo, "X-Trace", []string{"one", "two"}); err != nil {
		return err
	}

	request := nats.NewMsg(config.prefix + ".ocaml-request")
	request.Data = []byte("request-from-go")
	request.Header.Set("X-Interop", "go-request")
	response, err := connection.RequestMsg(request, waitTimeout)
	if err != nil {
		return fmt.Errorf("request OCaml: %w", err)
	}
	if string(response.Data) != "response-from-ocaml" {
		return fmt.Errorf("OCaml response was %q, expected %q", string(response.Data), "response-from-ocaml")
	}
	if response.Header.Get("X-Interop") != "ocaml-response" {
		return fmt.Errorf("OCaml response X-Interop header was %q", response.Header.Get("X-Interop"))
	}

	if err := waitResult("OCaml request response", goRequestResults); err != nil {
		return err
	}
	noResponder := nats.NewMsg(config.prefix + ".no-responder")
	noResponder.Data = []byte("missing")
	_, err = connection.RequestMsg(noResponder, waitTimeout)
	if !errors.Is(err, nats.ErrNoResponders) {
		if err == nil {
			return errors.New("request without a responder unexpectedly succeeded")
		}
		return fmt.Errorf("request without a responder returned %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("final flush: %w", err)
	}
	if err := connection.Drain(); err != nil {
		return fmt.Errorf("drain: %w", err)
	}
	return nil
}

func main() {
	config := options{}
	flag.StringVar(&config.server, "server", "", "NATS server URL")
	flag.StringVar(&config.prefix, "prefix", "", "unique subject prefix")
	flag.StringVar(&config.stream, "stream", "", "JetStream stream name")
	flag.StringVar(&config.ready, "ready-file", "", "file created after subscriptions are ready")
	flag.StringVar(&config.signal, "signal-file", "", "file written to trigger reconnect in reconnect mode")
	flag.StringVar(&config.mode, "mode", "core", "interop mode: core, reconnect, jetstream, jetstream-push, or jetstream-push-reconnect")
	flag.Parse()
	if err := validateOptions(config); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := runMode(config); err != nil {
		fmt.Fprintf(os.Stderr, "interop peer: %s\n", err)
		os.Exit(1)
	}
}
