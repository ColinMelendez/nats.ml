package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

const waitTimeout = 10 * time.Second

type options struct {
	server                  string
	prefix                  string
	stream                  string
	bucket                  string
	ready                   string
	signal                  string
	leader                  string
	survivor                string
	changedAdvertisedURL    string
	mode                    string
	requireReplicatedStream bool
	multiNodeLoss           bool
	managementFailure       bool
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
	nkeySeedFile, nkeySet := os.LookupEnv("NATS_TEST_NKEY_SEED_FILE")
	userJWTFile, userJWTSet := os.LookupEnv("NATS_TEST_USER_JWT_FILE")
	userSeedFile, userSeedSet := os.LookupEnv("NATS_TEST_USER_SEED_FILE")
	var options []nats.Option
	switch {
	case tokenSet && (userSet || passwordSet || nkeySet || userJWTSet || userSeedSet):
		return nil, errors.New("NATS_TEST_TOKEN cannot be combined with another authentication mode")
	case nkeySet && (userSet || passwordSet || userJWTSet || userSeedSet):
		return nil, errors.New("NATS_TEST_NKEY_SEED_FILE cannot be combined with another authentication mode")
	case (userJWTSet || userSeedSet) && (userSet || passwordSet || nkeySet):
		return nil, errors.New("NATS_TEST_USER_JWT_FILE and NATS_TEST_USER_SEED_FILE cannot be combined with another authentication mode")
	case tokenSet:
		if token == "" {
			return nil, errors.New("NATS_TEST_TOKEN must be non-empty")
		}
		options = append(options, nats.Token(token))
	case nkeySet:
		if nkeySeedFile == "" {
			return nil, errors.New("NATS_TEST_NKEY_SEED_FILE must be non-empty")
		}
		seed, err := os.ReadFile(nkeySeedFile)
		if err != nil {
			return nil, fmt.Errorf("read NKey seed: %w", err)
		}
		keyPair, err := nkeys.FromSeed(bytes.TrimSpace(seed))
		if err != nil {
			return nil, fmt.Errorf("parse NKey seed: %w", err)
		}
		publicKey, err := keyPair.PublicKey()
		if err != nil {
			return nil, fmt.Errorf("derive NKey public key: %w", err)
		}
		options = append(options, nats.Nkey(publicKey, func(nonce []byte) ([]byte, error) {
			return keyPair.Sign(nonce)
		}))
	case userJWTSet || userSeedSet:
		if !userJWTSet || !userSeedSet || userJWTFile == "" || userSeedFile == "" {
			return nil, errors.New("NATS_TEST_USER_JWT_FILE and NATS_TEST_USER_SEED_FILE must both be non-empty")
		}
		if _, err := os.ReadFile(userJWTFile); err != nil {
			return nil, fmt.Errorf("read user JWT: %w", err)
		}
		if _, err := os.ReadFile(userSeedFile); err != nil {
			return nil, fmt.Errorf("read user NKey seed: %w", err)
		}
		options = append(options, nats.UserCredentials(userJWTFile, userSeedFile))
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
	clientCert, clientCertSet := os.LookupEnv("NATS_TEST_TLS_CERT")
	clientKey, clientKeySet := os.LookupEnv("NATS_TEST_TLS_KEY")
	if clientCertSet || clientKeySet {
		if clientCert == "" || clientKey == "" {
			return nil, errors.New("NATS_TEST_TLS_CERT and NATS_TEST_TLS_KEY must both be non-empty")
		}
		options = append(options, nats.ClientCert(clientCert, clientKey))
	}
	return options, nil
}

func expectInitialConnection(connection *nats.Conn, servers string) error {
	expected := strings.TrimSpace(strings.SplitN(servers, ",", 2)[0])
	if expected == "" {
		return errors.New("server list did not contain an initial endpoint")
	}
	actual := connection.ConnectedUrl()
	if actual != expected {
		return fmt.Errorf("connected to %q, expected initial endpoint %q", actual, expected)
	}
	return nil
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
	flag.StringVar(&config.bucket, "bucket", "", "JetStream Key-Value bucket name")
	flag.StringVar(&config.ready, "ready-file", "", "file created after subscriptions are ready")
	flag.StringVar(&config.signal, "signal-file", "", "file written to trigger reconnect in reconnect mode")
	flag.StringVar(&config.leader, "leader-file", "", "file written with the JetStream stream leader")
	flag.StringVar(&config.survivor, "survivor-file", "", "file containing the endpoints for the post-failover client")
	flag.StringVar(&config.changedAdvertisedURL, "changed-advertised-url", "", "new advertised URL required by changed-advertisement mode")
	flag.StringVar(&config.mode, "mode", "core", "interop mode: core, service, service-failure, service-parent-close, reconnect, service-reconnect, jetstream, jetstream-admin, jetstream-push, jetstream-ordered, jetstream-kv, jetstream-object, jetstream-push-reconnect, jetstream-ordered-reconnect, jetstream-ordered-management-failure, jetstream-ordered-changed-advertisement, jetstream-ordered-restart, jetstream-ordered-multi-node, jetstream-ordered-leader-failover, jetstream-kv-reconnect, jetstream-kv-restart, jetstream-kv-multi-node, jetstream-kv-leader-failover, jetstream-object-reconnect, jetstream-object-restart, jetstream-object-multi-node, or jetstream-object-leader-failover")
	flag.Parse()
	if err := validateOptions(config); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := runMode(config); err != nil {
		if config.signal != "" {
			if markerError := os.WriteFile(config.signal+".failed", []byte("failed\n"), 0600); markerError != nil {
				fmt.Fprintf(os.Stderr, "interop peer: write failure marker: %s\n", markerError)
			}
		}
		fmt.Fprintf(os.Stderr, "interop peer: %s\n", err)
		os.Exit(1)
	}
}
