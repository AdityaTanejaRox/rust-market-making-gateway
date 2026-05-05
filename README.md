# Rust Market-Making Gateway

A Rust trading-systems gateway that connects to live crypto exchange feeds, maintains normalized market data across multiple venues, exposes real-time operational APIs, and routes simulated buy/sell requests to the best fresh venue.

This project was built as a production-shaped Rust systems project focused on:

- real WebSocket market data
- local order book maintenance
- cross-venue normalization
- latency-aware processing
- health/readiness/status endpoints
- rate-limited quote generation
- smart routing across venues
- Dockerized deployment
- unit-tested core components

---

## Venues Supported
-----------------------------------------------------------------------------
| Venue     |              Feed Type            |         Symbols           |
|-----------|-----------------------------------|---------------------------|
| Binance   | Full depth stream + REST snapshot | BTCUSDT, ETHUSDT, SOLUSDT |
| Coinbase  | Real ticker top-of-book WebSocket | BTCUSDT, ETHUSDT, SOLUSDT |
| Kraken    | Real ticker top-of-book WebSocket | BTCUSDT, ETHUSDT, SOLUSDT |
-----------------------------------------------------------------------------
---

## Architecture

```text
                 +--------------------+
                 | Binance Depth Feed |
                 | WS + REST Snapshot |
                 +---------+----------+
                           |
                           v
                    Local Order Book
                           |
                           v

+----------------+   +----------------+   +----------------+
| Coinbase TOB   |   | Shared Book    |   | Kraken TOB     |
| WebSocket      |-->| Store          |<--| WebSocket      |
+----------------+   +-------+--------+   +----------------+
                             |
              +--------------+--------------+
              |                             |
              v                             v
       Aggregation API                Routing API
       /aggregate/:symbol             /route/:symbol
```

---

## Core Features

### Real Market Data
The gateway connects to live public exchange feeds:
    Binance depth WebSocket
    Binance REST depth snapshot
    Coinbase ticker WebSocket
    Kraken ticker WebSocket

Binance maintains a real local order book using:
```text
REST snapshot
+ diff depth stream
+ sequence validation
+ resync on gap
```

Coinbase and Kraken are normalized into the same internal top-of-book model.

---

## Normalized Book Model

Every venue is normalized into:
```bash
venue
symbol
venue_symbol
bid_price
bid_qty
ask_price
ask_qty
sequence
receive_ts_us
last_update_latency_us
```

Prices are stored as integer cent ticks:
```bash
81633.47 -> 8163347
2382.18  -> 238218
86.62    -> 8662
```

This avoids floating-point routing decisions.

---

## HTTP API

### Health
```bash
curl localhost:8080/healthz
```

Returns process liveness:
```json
{
  "status": "ok"
}
```
---

### Status
```bash
curl localhost:8080/status
```

Shows all configured venues/symbols, freshness, active books, and metrics.
----

### Readiness
```bash
curl localhost:8080/readyz
```

Returns whether all configured market books are fresh.
---

### All Books
```bash
curl localhost:8080/books
```

Returns all live normalized books across all venues.
---

### Single Book
```bash
curl localhost:8080/books/BINANCE/BTCUSDT
curl localhost:8080/books/COINBASE/ETHUSDT
curl localhost:8080/books/KRAKEN/SOLUSDT
```
---

### Quote Generation
```bash
curl -X POST localhost:8080/quote/BINANCE/BTCUSDT
```

Generates a simple quote around mid price:
```bash
mid = (bid + ask) / 2
quote_bid = mid - 1 tick
quote_ask = mid + 1 tick
```

Quotes requests are rate-limited through a token bucket.
---

### Aggregation
```bash
curl localhost:8080/aggregate/BTCUSDT
curl localhost:8080/aggregate/ETHUSDT
curl localhost:8080/aggregate/SOLUSDT
```

Returns:
    fresh venues
    best bid
    best ask
    venue comparison
    simple arbitrage check
---

### Routing
Buy routes to the lowest fresh ask:
```bash
curl "localhost:8080/route/BTCUSDT?side=BUY&qty=1"
```

Sell routes to the highest fresh bid:
```bash
curl "localhost:8080/route/BTCUSDT?side=SELL&qty=1"
```

Example routing logic:
```bash
BUY  -> lowest fresh ask
SELL -> highest fresh bid
```
---

### Metrics
Available from:
```bash
curl localhost:8080/metrics-lite
```

Tracks:
```bash
messages_received
messages_parsed
book_updates_applied
quotes_generated
rate_limit_blocks
parse_errors
sequence_gaps_detected
reconnects_total
heartbeat_timeouts
stale_books
```
---

### Latency Tracking
The system tracks microsecond-level timing:
```bash
receive_ts_us
book_age_us
book_age_ms
last_update_latency_us
quote_latency_us
parse_latency_us
book_apply_latency_us
```

Structured logs include fields for feed processing and quote generation.
---

### Reconnect and Resync
Each venue supervisor runs independently.

On failure:
```bash
connection failure
heartbeat timeout
websocket close
sequence gap
```
the supervisor reconnects with exponential backoff.

For Binance depth streams, a sequence gap triggers a full resync:
```bash
discard current book
reconnect
reload REST snapshot
resume depth stream
```
---

### Running Locally
```bash
cargo run
```

In another terminal:
```bash
curl localhost:8080/status
curl localhost:8080/books
curl localhost:8080/aggregate/BTCUSDT
curl "localhost:8080/route/BTCUSDT?side=BUY&qty=1"
```
---

### Running Tests
```bash
cargo test
```

Current test coverage includes:
    local order book updates
    sequence gap detection
    stale/fresh book detection
    shared top-of-book store
    quote generation
    token bucket rate limiter
    Binance parser
    Coinbase parser
    Kraken parser

Expected:
```bash
22 passed
0 failed
```
---

### Docker
Build and run:
```bash
docker compose up --build
```

Test:
```bash
curl localhost:8080/healthz
curl localhost:8080/status
curl localhost:8080/books
curl localhost:8080/aggregate/BTCUSDT
curl "localhost:8080/route/BTCUSDT?side=BUY&qty=1"
```

Stop:
```bash
docker compose down
```
---

## Project Layout
```bash
src/
  api/
    http.rs

  book/
    order_book.rs
    top_of_book.rs

  config.rs
  error.rs
  logging.rs
  main.rs
  types.rs

  metrics/
    counters.rs
    latency.rs

  strategy/
    market_maker.rs

  venue/
    binance.rs
    coinbase.rs
    kraken.rs
    rate_limit.rs
    sequence.rs
    supervisor.rs

configs/
  binance.yaml

Dockerfile
docker-compose.yaml
```
---

## Design Choices

### Rust + Tokio
Tokio is used for async WebSocket processing, concurrent venue supervisors, HTTP service handling, and graceful shutdown.

### Per-Venue Supervisors
Each venue/symbol pair runs independently:
```bash
BINANCE BTCUSDT
BINANCE ETHUSDT
BINANCE SOLUSDT
COINBASE BTCUSDT
...
```
A failure in one stream does not stop the whole gateway.

### Shared Book Store
The system stores normalized books by:
```bash
VENUE:SYMBOL
```

Example:
```bash
BINANCE:  BTCUSDT
KRAKEN:   BTCUSDT
COINBASE: BTCUSDT
...
```

### Freshness Filtering
Routing and aggregation only use fresh books.

Binance uses a tighter threshold:
```bash
3000 ms
```

Coinbase and Kraken use a wider threshold:
```bash
10000 ms
```

because top-of-book feeds may update less frequently during quiet periods.
---


## What This Is Not
```text
This is not a live trading bot.

It does not submit real orders, manage private API keys, or execute trades.

It is a market-data, aggregation, routing, and infrastructure project designed to model the core systems around a trading engine.
```
---

## Future Extensions
Possible next steps:
```bash
private order gateway
real order lifecycle
risk checks
inventory-aware quoting
fee-adjusted routing
Prometheus metrics
trader dashboard
persistent event log
multi-process service decomposition
more venues
full-depth Coinbase/Kraken books
```
---

## Summary
This project demonstrates a production-shaped Rust trading gateway with live market data, local order book handling, normalized multi-venue books, latency-aware processing, operational endpoints, rate-limited quote generation, aggregation, and smart routing.