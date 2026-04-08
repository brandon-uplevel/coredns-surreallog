# coredns-surreallog

CoreDNS plugin that logs every DNS query and response to SurrealDB with async batched writes.

## Features

- Full query + response capture (like dnstap, but without protobuf overhead)
- Async batched writes — never blocks DNS resolution
- Parsed answer records stored as structured data
- Optional raw wire-format packet storage for forensic analysis
- Prometheus metrics: logged, dropped, flushed, errors
- Configurable batch size, flush interval, and buffer size

## Corefile Syntax

```
surreallog {
    url http://surrealdb:8000
    namespace hellojade_dns
    database logs
    username root
    password secret
    server_id ns1
    batch_size 100
    flush_interval 1s
    buffer_size 10000
    log_answers true
    log_raw false
}
```

## Building

Add to your CoreDNS `plugin.cfg`:

```
surreallog:github.com/brandon-uplevel/coredns-surreallog
```

Then:

```bash
go get github.com/brandon-uplevel/coredns-surreallog@latest
go generate && go build
```

## Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `coredns_surreallog_logged_total` | Counter | Queries sent to writer |
| `coredns_surreallog_dropped_total` | Counter | Queries dropped (buffer full) |
| `coredns_surreallog_flushed_total` | Counter | Queries written to SurrealDB |
| `coredns_surreallog_errors_total` | Counter | SurrealDB write errors |

## SurrealDB Schema

See `db/surreal/migrations/002_create_dns_query_table.surql` for the full schema.
