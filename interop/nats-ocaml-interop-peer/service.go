package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

const (
	goServiceName    = "go-interop-service"
	ocamlServiceName = "ocaml-interop-service"
)

func endpointStats(stats micro.Stats, name string) (*micro.EndpointStats, error) {
	for _, endpoint := range stats.Endpoints {
		if endpoint.Name == name {
			return endpoint, nil
		}
	}
	return nil, fmt.Errorf("service %q stats omitted endpoint %q", stats.Name, name)
}

func checkMetadata(label string, metadata map[string]string, name, expected string) error {
	if metadata[name] != expected {
		return fmt.Errorf("%s metadata %q was %q, expected %q", label, name, metadata[name], expected)
	}
	return nil
}

func checkEndpointInfo(label string, endpoint micro.EndpointInfo, name, subject string) error {
	if endpoint.Name != name {
		return fmt.Errorf("%s endpoint name was %q, expected %q", label, endpoint.Name, name)
	}
	if endpoint.Subject != subject {
		return fmt.Errorf("%s endpoint subject was %q, expected %q", label, endpoint.Subject, subject)
	}
	if endpoint.QueueGroup != micro.DefaultQueueGroup {
		return fmt.Errorf("%s endpoint queue group was %q, expected %q", label, endpoint.QueueGroup, micro.DefaultQueueGroup)
	}
	return checkMetadata(label, endpoint.Metadata, "role", "interop")
}

func checkStatsData(label string, endpoint *micro.EndpointStats, generator, name string) error {
	if len(endpoint.Data) == 0 {
		return fmt.Errorf("%s omitted custom endpoint data", label)
	}
	var data map[string]string
	if err := json.Unmarshal(endpoint.Data, &data); err != nil {
		return fmt.Errorf("%s custom endpoint data: %w", label, err)
	}
	expected := map[string]string{
		"generator": generator,
		"endpoint":  name,
		"status":    "ready",
	}
	if len(data) != len(expected) {
		return fmt.Errorf("%s custom endpoint data had %d fields, expected %d", label, len(data), len(expected))
	}
	for key, expectedValue := range expected {
		if actualValue := data[key]; actualValue != expectedValue {
			return fmt.Errorf("%s custom endpoint data %q was %q, expected %q", label, key, actualValue, expectedValue)
		}
	}
	return nil
}

func requestServiceInfo(connection *nats.Conn, service string) (micro.Info, error) {
	subject, err := micro.ControlSubject(micro.InfoVerb, service, "")
	if err != nil {
		return micro.Info{}, fmt.Errorf("service info subject: %w", err)
	}
	response, err := connection.Request(subject, nil, waitTimeout)
	if err != nil {
		return micro.Info{}, fmt.Errorf("request service info: %w", err)
	}
	var info micro.Info
	if err := json.Unmarshal(response.Data, &info); err != nil {
		return micro.Info{}, fmt.Errorf("decode service info: %w", err)
	}
	if info.Type != micro.InfoResponseType {
		return micro.Info{}, fmt.Errorf("service info type was %q, expected %q", info.Type, micro.InfoResponseType)
	}
	return info, nil
}

func requestServiceStats(connection *nats.Conn, service, id string) (micro.Stats, error) {
	subject, err := micro.ControlSubject(micro.StatsVerb, service, id)
	if err != nil {
		return micro.Stats{}, fmt.Errorf("service stats subject: %w", err)
	}
	response, err := connection.Request(subject, nil, waitTimeout)
	if err != nil {
		return micro.Stats{}, fmt.Errorf("request service stats: %w", err)
	}
	var stats micro.Stats
	if err := json.Unmarshal(response.Data, &stats); err != nil {
		return micro.Stats{}, fmt.Errorf("decode service stats: %w", err)
	}
	if stats.Type != micro.StatsResponseType {
		return micro.Stats{}, fmt.Errorf("service stats type was %q, expected %q", stats.Type, micro.StatsResponseType)
	}
	return stats, nil
}

func waitForServiceStats(label string, get func() (micro.Stats, error), ready func(micro.Stats) bool) (micro.Stats, error) {
	deadline := time.Now().Add(waitTimeout)
	var lastErr error
	for {
		stats, err := get()
		if err == nil {
			if ready(stats) {
				return stats, nil
			}
			lastErr = fmt.Errorf("%s stats have not reached the expected counters", label)
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			return micro.Stats{}, fmt.Errorf("timed out waiting for %s: %w", label, lastErr)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func runServicePeer(config options) error {
	authOptions, err := connectOptions()
	if err != nil {
		return err
	}
	connectionOptions := append([]nats.Option{nats.Name("ocaml-nats Service interop peer")}, authOptions...)
	connection, err := nats.Connect(config.server, connectionOptions...)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer connection.Close()

	callbackErrors := make(chan error, 4)
	recordCallbackError := func(err error) {
		if err != nil {
			select {
			case callbackErrors <- err:
			default:
			}
		}
	}
	service, err := micro.AddService(connection, micro.Config{
		Name:        goServiceName,
		Version:     "1.2.3",
		Description: "Go Service interop",
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
		return fmt.Errorf("add Go service: %w", err)
	}
	serviceStopped := false
	defer func() {
		if !serviceStopped {
			_ = service.Stop()
		}
	}()

	echoSubject := config.prefix + ".go.echo"
	errorSubject := config.prefix + ".go.error"
	echoHandler := micro.HandlerFunc(func(request micro.Request) {
		if string(request.Data()) != "from-ocaml" {
			recordCallbackError(fmt.Errorf("Go echo payload was %q, expected %q", string(request.Data()), "from-ocaml"))
			if err := request.Error("400", "invalid echo payload", nil); err != nil {
				recordCallbackError(err)
			}
			return
		}
		if request.Headers().Get("X-Interop") != "ocaml-service" {
			recordCallbackError(fmt.Errorf("Go echo X-Interop was %q, expected %q", request.Headers().Get("X-Interop"), "ocaml-service"))
			if err := request.Error("400", "invalid echo header", nil); err != nil {
				recordCallbackError(err)
			}
			return
		}
		recordCallbackError(request.Respond([]byte("go-service-response"), micro.WithHeaders(micro.Headers{"X-Interop": []string{"go-service-response"}})))
	})
	errorHandler := micro.HandlerFunc(func(request micro.Request) {
		if string(request.Data()) != "error-request" {
			recordCallbackError(fmt.Errorf("Go error payload was %q, expected %q", string(request.Data()), "error-request"))
			if err := request.Error("400", "invalid error payload", nil); err != nil {
				recordCallbackError(err)
			}
			return
		}
		if err := request.Error("422", "go-service-error", []byte("go-service-error-payload"), micro.WithHeaders(micro.Headers{"X-Interop": []string{"go-service-error"}})); err != nil {
			recordCallbackError(err)
		}
	})
	if err := service.AddEndpoint("echo", echoHandler,
		micro.WithEndpointSubject(echoSubject),
		micro.WithEndpointMetadata(map[string]string{"role": "interop"})); err != nil {
		return fmt.Errorf("add Go echo endpoint: %w", err)
	}
	if err := service.AddEndpoint("error", errorHandler,
		micro.WithEndpointSubject(errorSubject),
		micro.WithEndpointMetadata(map[string]string{"role": "interop"})); err != nil {
		return fmt.Errorf("add Go error endpoint: %w", err)
	}

	startMessages := make(chan *nats.Msg, 1)
	if _, err := connection.Subscribe(config.prefix+".start", func(message *nats.Msg) {
		select {
		case startMessages <- message:
		default:
		}
	}); err != nil {
		return fmt.Errorf("subscribe service start: %w", err)
	}
	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush Go service: %w", err)
	}
	if err := os.WriteFile(config.ready, []byte("ready\n"), 0600); err != nil {
		return fmt.Errorf("write ready file: %w", err)
	}

	startMessage, err := waitMessage("Service start request", startMessages)
	if err != nil {
		return err
	}
	if err := startMessage.Respond([]byte("started")); err != nil {
		return fmt.Errorf("respond to Service start: %w", err)
	}

	request := nats.NewMsg(config.prefix + ".ocaml.echo")
	request.Data = []byte("from-go")
	request.Header.Set("X-Interop", "go-service-request")
	response, err := connection.RequestMsg(request, waitTimeout)
	if err != nil {
		return fmt.Errorf("request OCaml service: %w", err)
	}
	if string(response.Data) != "ocaml-service-response" {
		return fmt.Errorf("OCaml service response was %q, expected %q", string(response.Data), "ocaml-service-response")
	}
	if response.Header.Get("X-Interop") != "ocaml-service-response" {
		return fmt.Errorf("OCaml service response X-Interop was %q, expected %q", response.Header.Get("X-Interop"), "ocaml-service-response")
	}

	ocamlInfo, err := requestServiceInfo(connection, ocamlServiceName)
	if err != nil {
		return err
	}
	if ocamlInfo.Name != ocamlServiceName || ocamlInfo.Version != "1.2.3" {
		return fmt.Errorf("OCaml service identity was %q/%q, expected %q/1.2.3", ocamlInfo.Name, ocamlInfo.Version, ocamlServiceName)
	}
	if err := checkMetadata("OCaml service", ocamlInfo.Metadata, "language", "ocaml"); err != nil {
		return err
	}
	if err := checkMetadata("OCaml service", ocamlInfo.Metadata, "suite", "interop"); err != nil {
		return err
	}
	if len(ocamlInfo.Endpoints) != 1 {
		return fmt.Errorf("OCaml service advertised %d endpoints, expected 1", len(ocamlInfo.Endpoints))
	}
	if err := checkEndpointInfo("OCaml service", ocamlInfo.Endpoints[0], "echo", config.prefix+".ocaml.echo"); err != nil {
		return err
	}

	ocamlStats, err := waitForServiceStats("OCaml service", func() (micro.Stats, error) {
		return requestServiceStats(connection, ocamlServiceName, ocamlInfo.ID)
	}, func(stats micro.Stats) bool {
		if stats.Name != ocamlServiceName || stats.ID != ocamlInfo.ID || len(stats.Endpoints) != 1 {
			return false
		}
		endpoint, endpointErr := endpointStats(stats, "echo")
		return endpointErr == nil && endpoint.NumRequests >= 1 && endpoint.NumErrors == 0 &&
			checkStatsData("OCaml echo", endpoint, "ocaml", "echo") == nil
	})
	if err != nil {
		return err
	}
	ocamlEcho, err := endpointStats(ocamlStats, "echo")
	if err != nil {
		return err
	}
	if err := checkStatsData("OCaml echo", ocamlEcho, "ocaml", "echo"); err != nil {
		return err
	}

	if err := connection.Flush(); err != nil {
		return fmt.Errorf("flush Go Service interop traffic: %w", err)
	}
	if _, err := waitForServiceStats("Go service", func() (micro.Stats, error) {
		return service.Stats(), nil
	}, func(stats micro.Stats) bool {
		echo, echoErr := endpointStats(stats, "echo")
		errorEndpoint, errorErr := endpointStats(stats, "error")
		return echoErr == nil && errorErr == nil && echo.NumRequests == 1 && echo.NumErrors == 0 && errorEndpoint.NumRequests == 1 && errorEndpoint.NumErrors == 1
	}); err != nil {
		return err
	}
	select {
	case callbackError := <-callbackErrors:
		return fmt.Errorf("Go Service callback: %w", callbackError)
	default:
	}

	doneRequest := nats.NewMsg(config.prefix + ".done")
	doneRequest.Data = []byte("go-finished")
	doneResponse, err := connection.RequestMsg(doneRequest, waitTimeout)
	if err != nil {
		return fmt.Errorf("request final Service barrier: %w", err)
	}
	if string(doneResponse.Data) != "ocaml-validated" {
		return fmt.Errorf("final Service barrier response was %q, expected %q", string(doneResponse.Data), "ocaml-validated")
	}
	if err := service.Stop(); err != nil {
		return fmt.Errorf("stop Go service: %w", err)
	}
	serviceStopped = true
	if err := connection.Drain(); err != nil {
		return fmt.Errorf("drain Go Service peer: %w", err)
	}
	return nil
}
