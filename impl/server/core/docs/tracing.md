# Tracing

Every span in this server is a [`tracing`](https://docs.rs/tracing) span. The
`resonate` binary turns them into OpenTelemetry spans and exports them over
OTLP when the standard OpenTelemetry environment asks it to. A program that
embeds the server through `resonate_base::build` installs no subscriber of its
own: the spans go to whatever subscriber that program installed, and nothing
is exported unless it exports.

## Turning it on

Export is off unless an OTLP endpoint is set. Everything is read from the
standard `OTEL_*` variables. None of them is a `RESONATE_*` key, because the
server reads every `RESONATE_*` variable as configuration and refuses keys it
does not know.

| Variable | Meaning |
|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Collector base URL. Setting it turns export on. Over HTTP, `/v1/traces` is appended. |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | Traces endpoint, used as given. Setting it also turns export on. |
| `OTEL_EXPORTER_OTLP_PROTOCOL` (or `…_TRACES_PROTOCOL`) | `http/protobuf` (default) or `grpc`. `http/json` is refused at startup. |
| `OTEL_EXPORTER_OTLP_HEADERS`, `…_TIMEOUT`, `…_COMPRESSION` | Read by the exporter, as the OpenTelemetry specification defines them. |
| `OTEL_SERVICE_NAME` | Service name. `resonate` when unset (and when `OTEL_RESOURCE_ATTRIBUTES` names none). |
| `OTEL_RESOURCE_ATTRIBUTES` | Extra resource attributes, `k=v,k=v`. |
| `OTEL_TRACES_SAMPLER`, `OTEL_TRACES_SAMPLER_ARG` | Sampler, for example `parentbased_traceidratio` and `0.1`. The default is `parentbased_always_on`. |
| `OTEL_SDK_DISABLED=true` or `OTEL_TRACES_EXPORTER=none` | Off, whatever else is set. |

Spans are batched. Whatever is still batched when the process stops is flushed
during shutdown, after the server has drained.

The log is unchanged by any of this: the spans are not shown in log lines, and
with no exporter installed they are not created at all.

Only Resonate's own crates (targets starting with `resonate`) are exported, at
`info`. The log level (`RESONATE_LEVEL`, `RUST_LOG`) does not affect it: a
server that logs at `warn` still exports its spans. The HTTP and gRPC clients
the exporter itself uses are never traced into the export.

TLS is rustls for both protocols, with the platform's root certificates. There
is no OpenSSL in the build.

The exporter is the `otlp` feature of `resonate-base`, which is on by default.
An embedder that never calls `resonate_base::run` can turn it off and drop the
exporter's dependencies.

## A local collector

Jaeger all-in-one accepts OTLP on both ports and shows the traces at
<http://localhost:16686>:

```sh
podman run --rm -d --name jaeger \
  -p 16686:16686 -p 4317:4317 -p 4318:4318 \
  docker.io/jaegertracing/all-in-one:1.62.0

OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 resonate dev
# or over gRPC
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 OTEL_EXPORTER_OTLP_PROTOCOL=grpc resonate dev
```

To see the raw spans instead, use an OpenTelemetry Collector with the `debug`
exporter:

```yaml
# otelcol.yaml
receivers:
  otlp:
    protocols:
      grpc: { endpoint: 0.0.0.0:4317 }
      http: { endpoint: 0.0.0.0:4318 }
exporters:
  debug: { verbosity: detailed }
service:
  pipelines:
    traces: { receivers: [otlp], exporters: [debug] }
```

```sh
podman run --rm -d --name otelcol -p 4317:4317 -p 4318:4318 \
  -v "$PWD/otelcol.yaml:/etc/otelcol-contrib/config.yaml:Z" \
  docker.io/otel/opentelemetry-collector-contrib:0.111.0
podman logs -f otelcol
```

## One trace across create, task and settle

A durable function is several requests and a message in between. They form
one trace when the context is carried across each hop:

1. **Create.** The client sends `promise.create` (or `task.create`) with a W3C
   `traceparent` header, or with a `traceparent` tag on the promise (and
   `tracestate` beside it if it has one). If both are present, the header wins
   for the request. A tag the client set is stored like any other tag.
2. **Dispatch.** The execute message for the promise's task carries the trace
   in its `head`, as `traceparent` (and `tracestate`):
   - HTTP push sends it as the `traceparent`/`tracestate` headers of the
     delivery as well as in the body.
   - Poll (SSE) carries it in the frame's JSON, at `head.traceparent`.
   - An unblock message carries the trace of the promise it reports, in the
     same place.
   The parent named is the server's delivery span, so the worker's work hangs
   off the hand-off.
3. **Task and settle.** The worker sends the message's `traceparent` back as
   the header on its `task.acquire`, `task.fulfill`, `task.suspend` and
   `promise.settle` requests. Those requests then continue the trace.

A task dispatched later than its create still continues the create's trace:
for example, one redispatched by its retry deadline, or one resumed because a
promise it awaited settled. The server remembers each promise's trace when it
is created. This memory is bounded (100,000 promises) and lives in the
process: after a restart, or for a promise created through another instance,
a redispatch starts from whatever emitted it instead.

The server never adds a tag the client did not ask for. Adding one would change
every later read of that promise. A deployment that wants a header-only client
to get a durable `traceparent` tag can opt in on the HTTP gateway:

```toml
[gateways.gateway_http]
trace_tags = true   # default false
```

An embedder serving the endpoint through `protocol_router` sets the same
switch as `ProtocolOptions::trace_tags`.

With this on, a `promise.create` or `task.create` that has a `traceparent`
header but no `traceparent` tag gets the header written into its tags.

`tests/tracing.rs` covers this end to end on SQLite with an in-memory
exporter. It sends one create with a header, takes the execute message the
worker received, then acquires and fulfills the task with the context from
that message. It then checks that all of their spans share one trace id.

## Spans

Span names below are the OpenTelemetry names (`otel.name`). The `tracing`
name, which a log line shows, is in brackets.

| Span | Where | Attributes |
|---|---|---|
| `<kind>`, e.g. `promise.create` [`resonate.request`] | HTTP gateway, one per request (also for an embedder serving `protocol_router` or calling `answer_envelope`); kind `server`; parented on the incoming `traceparent` | `rpc.system=resonate`, `resonate.kind`, `resonate.corr_id`, `resonate.status` |
| `process <kind>` [`resonate.process`] | the server port, whatever is in front of it | `resonate.kind`, `resonate.corr_id`, `resonate.status` |
| `<store>.<op>` [`resonate.store`] | one per storage operation | `db.system`, `db.operation`, `outcome` (`ok`/`error`) |
| `resonate.blob.batch` | blob: one origin's decision batch; parented on its first request and linked to the rest | `resonate.origin`, `resonate.batch_size`, `resonate.attempts`; a `CAS retry` event per lost compare-and-swap, with `attempt` and `outcome` |
| `route execute` / `route unblock` [`resonate.route`] | the router, one per message; kind `producer` | `resonate.message.kind`, `resonate.promise.id`, `resonate.address.scheme` |
| `deliver http` [`resonate.deliver`] | HTTP push, one per delivery; kind `client` | `resonate.message.kind`, `url.full`, `http.response.status_code` |
| `deliver poll` [`resonate.deliver`] | poll/SSE, one per message; kind `producer` | `resonate.message.kind`, `resonate.poll.group`, `resonate.delivered` |
| `resonate.timer.fire` | the timer loop firing a batch of deadlines | `resonate.timeouts` |
| `timeout <kind>` [`resonate.timer.timeout`] | one per deadline fired | `resonate.timeout.kind`, `resonate.timeout.id` |
| `resonate.timer.sweep` | SQL servers' backstop scan | `resonate.schedules_fired`, `resonate.messages` |

The `<store>.<op>` operations are:

- `sqlite.transact`, `sqlite.query`
- `postgres.transact`, `mysql.transact` (a serialization retry stays inside the
  one span, logged as an event)
- `blob.get`, `blob.put_if_match`, `blob.put_if_none_match`, `blob.put`,
  `blob.delete`, `blob.list`, `blob.delete_prefix`

`blob` is the store's own name (`Store::system`). Any `Store` handed to
`resonate-server-blob` is wrapped, so a store over another backend gets the
same spans under its own name.

Log events inside a span are attached to it as span events.

## Metrics

The traces have Prometheus counterparts on `/metrics` (the `gateway_metrics`
listener). None of them has a tenant, promise or key label.

| Metric | Labels |
|---|---|
| `resonate_request_duration_seconds` (histogram) | `kind` |
| `resonate_request_total` | `kind`, `status` |
| `resonate_store_op_duration_seconds` (histogram) | `store`, `op`, `outcome` |
| `resonate_deliveries_total` | `status` |
| `resonate_messages_total`, `resonate_blob_messages_total` | `kind` |
