package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

const serviceReconnectEndpointSubjectSuffix = ".go.echo"

func serviceReconnectPayload(round int) string {
	return fmt.Sprintf("go-to-ocaml-round-%d", round)
}

func serviceReconnectInfo(label string, info micro.Info, name, subject string) error {
	if info.Name != name {
		return fmt.Errorf("%s service name was %q, expected %q", label, info.Name, name)
	}
	if info.Version != "1.2.3" {
		return fmt.Errorf("%s service version was %q, expected %q", label, info.Version, "1.2.3")
	}
	if err := checkMetadata(label, info.Metadata, "suite", "interop"); err != nil {
		return err
	}
	if len(info.Endpoints) != 1 {
		return fmt.Errorf("%s service advertised %d endpoints, expected 1", label, len(info.Endpoints))
	}
	return checkEndpointInfo(label, info.Endpoints[0], "echo", subject)
}

func serviceReconnectStats(label string, stats micro.Stats, name, id, subject string, expectedRequests int, generator string) error {
	if stats.Name != name {
		return fmt.Errorf("%s stats service name was %q, expected %q", label, stats.Name, name)
	}
	if stats.ID != id {
		return fmt.Errorf("%s stats service id was %q, expected %q", label, stats.ID, id)
	}
	if stats.Version != "1.2.3" {
		return fmt.Errorf("%s stats service version was %q, expected %q", label, stats.Version, "1.2.3")
	}
	if err := checkMetadata(label, stats.Metadata, "suite", "interop"); err != nil {
		return err
	}
	if len(stats.Endpoints) != 1 {
		return fmt.Errorf("%s stats advertised %d endpoints, expected 1", label, len(stats.Endpoints))
	}
	endpoint := stats.Endpoints[0]
	if endpoint.Name != "echo" {
		return fmt.Errorf("%s endpoint name was %q, expected %q", label, endpoint.Name, "echo")
	}
	if endpoint.Subject != subject {
		return fmt.Errorf("%s endpoint subject was %q, expected %q", label, endpoint.Subject, subject)
	}
	if endpoint.QueueGroup != micro.DefaultQueueGroup {
		return fmt.Errorf("%s endpoint queue group was %q, expected %q", label, endpoint.QueueGroup, micro.DefaultQueueGroup)
	}
	if endpoint.NumRequests != expectedRequests {
		return fmt.Errorf("%s endpoint request count was %d, expected %d", label, endpoint.NumRequests, expectedRequests)
	}
	if endpoint.NumErrors != 0 {
		return fmt.Errorf("%s endpoint error count was %d, expected 0", label, endpoint.NumErrors)
	}
	if err := checkStatsData(label, endpoint, generator, "echo"); err != nil {
		return err
	}
	return nil
}

func runServiceReconnectPeer(config options) error {
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
		nats.Name("ocaml-nats Service reconnect interop peer"),
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

	callbackErrors := make(chan error, 4)
	recordCallbackError := func(callbackError error) {
		if callbackError != nil {
			select {
			case callbackErrors <- callbackError:
			default:
			}
		}
	}
	service, err := micro.AddService(connection, micro.Config{
		Name:        goServiceName,
		Version:     "1.2.3",
		Description: "Go Service reconnect interop",
		Metadata:    map[string]string{"language": "go", "suite": "interop"},
		StatsHandler: func(endpoint *micro.Endpoint) any {
			return map[string]string{
				"generator": "go",
				"endpoint":  endpoint.Name,
				"status":    "ready",
			}
		},
	})
	if err != nil {
		return fmt.Errorf("add Go reconnect service: %w", err)
	}
	serviceStopped := false
	defer func() {
		if !serviceStopped {
			_ = service.Stop()
		}
	}()

	echoSubject := config.prefix + serviceReconnectEndpointSubjectSuffix
	echoHandler := micro.HandlerFunc(func(request micro.Request) {
		payload := string(request.Data())
		if !strings.HasPrefix(payload, "ocaml-to-go-round-") {
			recordCallbackError(fmt.Errorf("Go reconnect echo payload was %q", payload))
			if callbackError := request.Error("400", "invalid echo payload", nil); callbackError != nil {
				recordCallbackError(callbackError)
			}
			return
		}
		if request.Headers().Get("X-Interop") != "ocaml-service-reconnect" {
			recordCallbackError(fmt.Errorf("Go reconnect echo X-Interop was %q", request.Headers().Get("X-Interop")))
			if callbackError := request.Error("400", "invalid echo header", nil); callbackError != nil {
				recordCallbackError(callbackError)
			}
			return
		}
		responsePayload := "go-response-for-" + payload
		if callbackError := request.Respond([]byte(responsePayload), micro.WithHeaders(micro.Headers{
			"X-Interop": []string{"go-service-reconnect-response"},
		})); callbackError != nil {
			recordCallbackError(callbackError)
		}
	})
	if err := service.AddEndpoint("echo", echoHandler,
		micro.WithEndpointSubject(echoSubject),
		micro.WithEndpointMetadata(map[string]string{"role": "interop"})); err != nil {
		return fmt.Errorf("add Go reconnect echo endpoint: %w", err)
	}

	startMessages := make(chan *nats.Msg, 1)
	roundReadyMessages := make(chan *nats.Msg, 1)
	if _, err := connection.Subscribe(config.prefix+".start", func(message *nats.Msg) {
		select {
		case startMessages <- message:
		default:
		}
	}); err != nil {
		return fmt.Errorf("subscribe Service reconnect start: %w", err)
	}
	if _, err := connection.Subscribe(config.prefix+".round-ready", func(message *nats.Msg) {
		select {
		case roundReadyMessages <- message:
		default:
		}
	}); err != nil {
		return fmt.Errorf("subscribe Service reconnect round barrier: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush Go Service reconnect subscriptions: %w", err)
	}
	if err := os.WriteFile(config.ready, []byte("ready\n"), 0600); err != nil {
		return fmt.Errorf("write ready file: %w", err)
	}

	startMessage, err := waitMessage("Service reconnect start request", startMessages)
	if err != nil {
		return err
	}
	if err := startMessage.Respond([]byte("started")); err != nil {
		return fmt.Errorf("respond to Service reconnect start: %w", err)
	}

	ocamlInfo, err := requestServiceInfo(connection, ocamlServiceName)
	if err != nil {
		return err
	}
	ocamlSubject := config.prefix + ".ocaml.echo"
	if err := serviceReconnectInfo("OCaml", ocamlInfo, ocamlServiceName, ocamlSubject); err != nil {
		return err
	}

	for round := 0; round <= cycles; round++ {
		payload := serviceReconnectPayload(round)
		request := nats.NewMsg(ocamlSubject)
		request.Data = []byte(payload)
		request.Header.Set("X-Interop", "go-service-reconnect")
		response, err := connection.RequestMsg(request, waitTimeout)
		if err != nil {
			return fmt.Errorf("request OCaml Service round %d: %w", round, err)
		}
		expectedResponse := "ocaml-response-for-" + payload
		if err := expectPayload("OCaml Service round response", response, expectedResponse); err != nil {
			return err
		}
		if response.Header.Get("X-Interop") != "ocaml-service-reconnect-response" {
			return fmt.Errorf("OCaml Service round response X-Interop was %q, expected %q", response.Header.Get("X-Interop"), "ocaml-service-reconnect-response")
		}

		expectedRequests := round + 1
		if _, err := waitForServiceStats("Go reconnect service", func() (micro.Stats, error) {
			return service.Stats(), nil
		}, func(stats micro.Stats) bool {
			return serviceReconnectStats("Go reconnect service", stats, goServiceName, service.Info().ID, echoSubject, expectedRequests, "go") == nil
		}); err != nil {
			return err
		}
		if _, err := waitForServiceStats("OCaml reconnect service", func() (micro.Stats, error) {
			return requestServiceStats(connection, ocamlServiceName, ocamlInfo.ID)
		}, func(stats micro.Stats) bool {
			return serviceReconnectStats("OCaml reconnect service", stats, ocamlServiceName, ocamlInfo.ID, ocamlSubject, expectedRequests, "ocaml") == nil
		}); err != nil {
			return err
		}
		select {
		case callbackError := <-callbackErrors:
			return fmt.Errorf("Go Service reconnect callback: %w", callbackError)
		default:
		}

		roundReady, err := waitMessage("Service reconnect round barrier", roundReadyMessages)
		if err != nil {
			return err
		}
		expectedRound := fmt.Sprintf("round-%d", round)
		if err := expectPayload("Service reconnect round barrier", roundReady, expectedRound); err != nil {
			return err
		}
		if err := roundReady.Respond([]byte("accepted")); err != nil {
			return fmt.Errorf("respond to Service reconnect round barrier: %w", err)
		}
		if err := connection.Flush(); err != nil {
			return fmt.Errorf("flush Service reconnect round barrier: %w", err)
		}

		if round == cycles {
			break
		}
		signal := roundSignal(config.signal, round+1)
		if err := os.WriteFile(signal, []byte("kill\n"), 0600); err != nil {
			return fmt.Errorf("write Service reconnect signal %d: %w", round+1, err)
		}
		if err := waitReconnect(reconnected); err != nil {
			return fmt.Errorf("Service reconnect %d: %w", round+1, err)
		}
		if err := connection.Flush(); err != nil {
			return fmt.Errorf("flush after Service reconnect %d: %w", round+1, err)
		}
		if err := awaitRecoveryBarrier(connection, config.prefix, round+1); err != nil {
			return err
		}
		ocamlInfo, err = requestServiceInfo(connection, ocamlServiceName)
		if err != nil {
			return err
		}
		if err := serviceReconnectInfo("OCaml after reconnect", ocamlInfo, ocamlServiceName, ocamlSubject); err != nil {
			return err
		}
	}

	doneRequest := nats.NewMsg(config.prefix + ".done")
	doneRequest.Data = []byte("go-finished")
	doneResponse, err := connection.RequestMsg(doneRequest, waitTimeout)
	if err != nil {
		return fmt.Errorf("request Service reconnect completion: %w", err)
	}
	if string(doneResponse.Data) != "ocaml-validated" {
		return fmt.Errorf("Service reconnect completion response was %q, expected %q", string(doneResponse.Data), "ocaml-validated")
	}
	if err := service.Stop(); err != nil {
		return fmt.Errorf("stop Go Service reconnect service: %w", err)
	}
	serviceStopped = true
	if err := connection.Drain(); err != nil {
		return fmt.Errorf("drain Go Service reconnect peer: %w", err)
	}
	return nil
}
