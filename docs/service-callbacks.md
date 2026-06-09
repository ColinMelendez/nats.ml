# Service callback design

This slice adds the callback and custom statistics behavior exposed by the
official Go [`micro` service configuration](https://github.com/nats-io/nats.go/blob/main/micro/service.go)
without introducing a second lifecycle manager or a general callback registry.

## Chosen surface

The service configuration accepts three optional callbacks:

```ocaml
let config =
  Service.Config.v ~name:"orders" ~version:"1.0.0"
    ~stats_handler:(fun endpoint ->
      if String.equal (Service.Stats.endpoint_name endpoint) "health" then
        Some (Jsont.Json.string "ready")
      else None)
    ~error_handler:(fun error ->
      Format.eprintf "service failed: %a@." Service.Error.pp error)
    ~done_handler:(fun () -> Format.eprintf "service stopped@.")
    ()
```

`stats_handler` receives an immutable `Service.Stats.endpoint` snapshot for
each endpoint whenever `Service.stats` or a `$SRV.STATS` response is built. It
returns `None` to omit the endpoint's `data` member, or `Some json` to include
it. `Some (Jsont.Json.null ())` is therefore distinct from omission. The
callback runs after the statistics snapshot has left the service mutex; the
snapshot passed to it has no custom data yet.

`error_handler` runs once when an owned service subscription causes the service
to transition from `Open` to `Failed`. Request handler failures, service-error
replies, and missing request responses remain endpoint statistics and do not
invoke this callback. `done_handler` runs once after the service reaches
`Stopped`, including a stop that reaches that state while returning a drain
error. It is not invoked for a service that remains `Failed`.

Error and done events are delivered in FIFO order by one dispatcher fiber owned
by the service switch. The dispatcher never holds the service mutex while
calling user code. A normal stop queues the done event and waits for its
callback before resolving `Service.stop`, so a successful stop cannot silently
discard the callback during switch teardown. A failed service queues its error
event and then terminates the dispatcher; it does not synthesize a done event.

Callbacks are ordinary OCaml functions. Their exceptions and cancellation are
not converted into protocol errors or silently discarded: cancellation
propagates, and another exception fails the fiber that owns the callback. This
makes callback bugs observable while keeping service state transitions
independent of callback execution order.

This exception policy is deliberate. `Service.stats` is an established direct
style function whose result is a snapshot rather than an error result, so
turning a failed statistics callback into omitted data would hide a user bug,
while routing it through `error_handler` would make one user callback depend on
another and could recurse. A statistics callback therefore fails its caller
fiber (or the monitoring worker building `$SRV.STATS`), just as a lifecycle
callback failure fails its dispatcher fiber.

## Alternatives considered

- Invoke error and done callbacks directly from the endpoint or monitoring
  worker. Rejected because callback execution would depend on which
  subscription observed the event and could run while a worker owns internal
  state.
- Add a mutable callback registry or service manager. Rejected because the
  service configuration already owns the three fixed extension points; another
  registry would widen the public ownership model without adding protocol
  capability.
- Accept pre-rendered JSON strings for custom data. Rejected because
  `Jsont.json` preserves arbitrary JSON values, including explicit `null`, and
  avoids a second encoding boundary.
- Pass only an endpoint name to `stats_handler`. Rejected because the
  immutable statistics snapshot lets the callback make decisions from the
  endpoint's declared subject, queue, metadata, and counters without exposing
  mutable runtime state.
