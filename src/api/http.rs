use crate::book::top_of_book::SharedTopOfBook;
use crate::config::AppConfig;
use crate::metrics::counters::Metrics;
use crate::strategy::market_maker::generate_quote;
use crate::types::TopOfBook;
use crate::venue::rate_limit::TokenBucket;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use parking_lot::Mutex;
use serde::Deserialize;
use serde_json::json;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::broadcast;

#[derive(Clone)]
pub struct ApiState {
    pub config: AppConfig,
    pub metrics: Arc<Metrics>,
    pub top_of_book: SharedTopOfBook,
    pub quote_rate_limiter: Arc<Mutex<TokenBucket>>,
}

#[derive(Debug, Deserialize)]
pub struct RouteQuery {
    pub side: String,
    pub qty: Option<f64>,
}

pub async fn run_http_server(
    config: AppConfig,
    metrics: Arc<Metrics>,
    top_of_book: SharedTopOfBook,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> anyhow::Result<()> {
    let quote_rate_limiter = Arc::new(Mutex::new(TokenBucket::new(
        config.rate_limits.outbound_messages_per_second,
        config.rate_limits.outbound_messages_per_second,
    )));

    let state = ApiState {
        config: config.clone(),
        metrics,
        top_of_book,
        quote_rate_limiter,
    };

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/status", get(status))
        .route("/metrics-lite", get(metrics_lite))
        .route("/books", get(books))
        .route("/books/:venue/:symbol", get(book_by_venue_symbol))
        .route("/quote/:venue/:symbol", post(quote_by_venue_symbol))
        .route("/aggregate/:symbol", get(aggregate_by_symbol))
        .route("/route/:symbol", get(route_by_symbol))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&config.service.http_addr).await?;

    tracing::info!(
        addr = %config.service.http_addr,
        "http server listening"
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown_rx.recv().await;
            tracing::info!("http server received shutdown");
        })
        .await?;

    Ok(())
}

async fn healthz() -> Json<serde_json::Value> {
    Json(json!({
        "status": "ok"
    }))
}

async fn readyz(State(state): State<ApiState>) -> (StatusCode, Json<serde_json::Value>) {
    let mut markets = Vec::new();
    let mut all_ready = true;

    for (venue, symbol) in state.config.all_configured_markets() {
        let stale_after_ms = state.config.stale_after_ms_for(&venue).unwrap_or(3000);
        let age_us = state.top_of_book.age_us(&venue, &symbol);
        let age_ms = state.top_of_book.age_ms(&venue, &symbol);
        let ready = state.top_of_book.is_fresh(&venue, &symbol, stale_after_ms);

        if !ready {
            all_ready = false;
        }

        markets.push(json!({
            "venue": venue,
            "symbol": symbol,
            "ready": ready,
            "book_age_us": age_us,
            "book_age_ms": age_ms,
            "stale_after_ms": stale_after_ms
        }));
    }

    let status = if all_ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (
        status,
        Json(json!({
            "ready": all_ready,
            "markets": markets
        })),
    )
}

async fn status(State(state): State<ApiState>) -> Json<serde_json::Value> {
    let metrics = state.metrics.snapshot();

    let mut markets = Vec::new();

    for (venue, symbol) in state.config.all_configured_markets() {
        let stale_after_ms = state.config.stale_after_ms_for(&venue).unwrap_or(3000);
        let book_age_us = state.top_of_book.age_us(&venue, &symbol);
        let book_age_ms = state.top_of_book.age_ms(&venue, &symbol);
        let book_fresh = state.top_of_book.is_fresh(&venue, &symbol, stale_after_ms);

        markets.push(json!({
            "venue": venue,
            "symbol": symbol,
            "connected": book_fresh,
            "book_fresh": book_fresh,
            "book_age_us": book_age_us,
            "book_age_ms": book_age_ms,
            "stale_after_ms": stale_after_ms
        }));
    }

    Json(json!({
        "active_books": state.top_of_book.active_keys(),
        "markets": markets,
        "metrics": {
            "messages_received": metrics.messages_received,
            "messages_parsed": metrics.messages_parsed,
            "book_updates_applied": metrics.book_updates_applied,
            "quotes_generated": metrics.quotes_generated,
            "rate_limit_blocks": metrics.rate_limit_blocks,
            "parse_errors": metrics.parse_errors,
            "sequence_gaps_detected": metrics.sequence_gaps_detected,
            "reconnects_total": metrics.reconnects_total,
            "heartbeat_timeouts": metrics.heartbeat_timeouts,
            "stale_books": metrics.stale_books
        }
    }))
}

async fn metrics_lite(State(state): State<ApiState>) -> Json<serde_json::Value> {
    Json(json!(state.metrics.snapshot()))
}

async fn books(State(state): State<ApiState>) -> Json<serde_json::Value> {
    let books: Vec<serde_json::Value> = state
        .top_of_book
        .all()
        .into_iter()
        .map(|book| {
            let stale_after_ms = state.config.stale_after_ms_for(&book.venue).unwrap_or(3000);
            let age_us = state.top_of_book.age_us(&book.venue, &book.symbol);
            let age_ms = state.top_of_book.age_ms(&book.venue, &book.symbol);
            let fresh = state
                .top_of_book
                .is_fresh(&book.venue, &book.symbol, stale_after_ms);

            json!({
                "venue": book.venue,
                "symbol": book.symbol,
                "venue_symbol": book.venue_symbol,
                "fresh": fresh,
                "book_age_us": age_us,
                "book_age_ms": age_ms,
                "stale_after_ms": stale_after_ms,
                "book": book
            })
        })
        .collect();

    Json(json!({
        "status": "ok",
        "books": books
    }))
}

async fn book_by_venue_symbol(
    State(state): State<ApiState>,
    Path((venue, symbol)): Path<(String, String)>,
) -> (StatusCode, Json<serde_json::Value>) {
    build_book_response(state, venue, symbol)
}

fn build_book_response(
    state: ApiState,
    venue: String,
    symbol: String,
) -> (StatusCode, Json<serde_json::Value>) {
    let venue_upper = venue.to_uppercase();
    let symbol_upper = symbol.to_uppercase();
    let stale_after_ms = state.config.stale_after_ms_for(&venue_upper).unwrap_or(3000);

    let age_us = state.top_of_book.age_us(&venue_upper, &symbol_upper);
    let age_ms = state.top_of_book.age_ms(&venue_upper, &symbol_upper);
    let fresh = state
        .top_of_book
        .is_fresh(&venue_upper, &symbol_upper, stale_after_ms);

    match state.top_of_book.get(&venue_upper, &symbol_upper) {
        Some(book) => (
            StatusCode::OK,
            Json(json!({
                "status": "ok",
                "fresh": fresh,
                "book_age_us": age_us,
                "book_age_ms": age_ms,
                "stale_after_ms": stale_after_ms,
                "book": book
            })),
        ),
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "warming_up",
                "venue": venue_upper,
                "symbol": symbol_upper,
                "fresh": false,
                "book_age_us": null,
                "book_age_ms": null,
                "stale_after_ms": stale_after_ms,
                "book": null
            })),
        ),
    }
}

async fn quote_by_venue_symbol(
    State(state): State<ApiState>,
    Path((venue, symbol)): Path<(String, String)>,
) -> (StatusCode, Json<serde_json::Value>) {
    build_quote_response(state, venue, symbol)
}

fn build_quote_response(
    state: ApiState,
    venue: String,
    symbol: String,
) -> (StatusCode, Json<serde_json::Value>) {
    {
        let mut limiter = state.quote_rate_limiter.lock();

        if !limiter.try_acquire() {
            state
                .metrics
                .rate_limit_blocks
                .fetch_add(1, Ordering::Relaxed);

            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(json!({
                    "status": "rate_limited",
                    "reason": "quote token bucket exhausted"
                })),
            );
        }
    }

    let venue_upper = venue.to_uppercase();
    let symbol_upper = symbol.to_uppercase();
    let stale_after_ms = state.config.stale_after_ms_for(&venue_upper).unwrap_or(3000);

    let age_us = state.top_of_book.age_us(&venue_upper, &symbol_upper);
    let age_ms = state.top_of_book.age_ms(&venue_upper, &symbol_upper);
    let fresh = state
        .top_of_book
        .is_fresh(&venue_upper, &symbol_upper, stale_after_ms);

    if !fresh {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "stale_book",
                "venue": venue_upper,
                "symbol": symbol_upper,
                "book_age_us": age_us,
                "book_age_ms": age_ms,
                "stale_after_ms": stale_after_ms
            })),
        );
    }

    let Some(book) = state.top_of_book.get(&venue_upper, &symbol_upper) else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "no_book",
                "venue": venue_upper,
                "symbol": symbol_upper
            })),
        );
    };

    let quote_start_us = crate::metrics::latency::now_us();
    let generated_quote = generate_quote(&book);
    let quote_latency_us = crate::metrics::latency::now_us() - quote_start_us;

    state
        .metrics
        .quotes_generated
        .fetch_add(1, Ordering::Relaxed);

    (
        StatusCode::OK,
        Json(json!({
            "status": "ok",
            "venue": book.venue,
            "symbol": book.symbol,
            "venue_symbol": book.venue_symbol,
            "book_age_us": age_us,
            "book_age_ms": age_ms,
            "quote_latency_us": quote_latency_us,
            "market": {
                "bid_price": book.bid_price,
                "bid_qty": book.bid_qty,
                "ask_price": book.ask_price,
                "ask_qty": book.ask_qty,
                "sequence": book.sequence
            },
            "quote": generated_quote
        })),
    )
}

async fn aggregate_by_symbol(
    State(state): State<ApiState>,
    Path(symbol): Path<String>,
) -> Json<serde_json::Value> {
    let symbol_upper = symbol.to_uppercase();
    let books = fresh_books_for_symbol(&state, &symbol_upper);

    let best_bid = books
        .iter()
        .max_by_key(|book| book.bid_price)
        .map(book_side_json_bid);

    let best_ask = books
        .iter()
        .min_by_key(|book| book.ask_price)
        .map(book_side_json_ask);

    let venue_books: Vec<serde_json::Value> = books
        .iter()
        .map(|book| {
            let stale_after_ms = state.config.stale_after_ms_for(&book.venue).unwrap_or(3000);
            json!({
                "venue": book.venue,
                "symbol": book.symbol,
                "venue_symbol": book.venue_symbol,
                "book_age_us": state.top_of_book.age_us(&book.venue, &book.symbol),
                "book_age_ms": state.top_of_book.age_ms(&book.venue, &book.symbol),
                "stale_after_ms": stale_after_ms,
                "bid_price": book.bid_price,
                "bid_qty": book.bid_qty,
                "ask_price": book.ask_price,
                "ask_qty": book.ask_qty,
                "sequence": book.sequence
            })
        })
        .collect();

    let arbitrage = match (&best_bid, &best_ask) {
        (Some(bid), Some(ask)) => {
            let bid_price = bid["price"].as_i64().unwrap_or_default();
            let ask_price = ask["price"].as_i64().unwrap_or_default();
            let bid_venue = bid["venue"].as_str().unwrap_or_default();
            let ask_venue = ask["venue"].as_str().unwrap_or_default();

            json!({
                "exists": bid_price > ask_price && bid_venue != ask_venue,
                "edge_ticks": bid_price - ask_price,
                "buy_venue": ask_venue,
                "sell_venue": bid_venue
            })
        }
        _ => json!({
            "exists": false,
            "edge_ticks": null
        }),
    };

    Json(json!({
        "status": "ok",
        "symbol": symbol_upper,
        "fresh_venue_count": books.len(),
        "best_bid": best_bid,
        "best_ask": best_ask,
        "arbitrage": arbitrage,
        "venues": venue_books
    }))
}

async fn route_by_symbol(
    State(state): State<ApiState>,
    Path(symbol): Path<String>,
    Query(query): Query<RouteQuery>,
) -> (StatusCode, Json<serde_json::Value>) {
    let symbol_upper = symbol.to_uppercase();
    let side = query.side.to_uppercase();
    let qty = query.qty.unwrap_or(1.0);

    let books = fresh_books_for_symbol(&state, &symbol_upper);

    if books.is_empty() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "no_fresh_books",
                "symbol": symbol_upper
            })),
        );
    }

    let selected = match side.as_str() {
        "BUY" | "BUY_BASE" | "BUY_YES" => books.iter().min_by_key(|book| book.ask_price),
        "SELL" | "SELL_BASE" | "SELL_YES" => books.iter().max_by_key(|book| book.bid_price),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "status": "bad_side",
                    "side": query.side,
                    "expected": "BUY or SELL"
                })),
            );
        }
    };

    let Some(book) = selected else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "no_route",
                "symbol": symbol_upper,
                "side": side
            })),
        );
    };

    let price = if side.starts_with("BUY") {
        book.ask_price
    } else {
        book.bid_price
    };

    let available_qty = if side.starts_with("BUY") {
        book.ask_qty
    } else {
        book.bid_qty
    };

    let candidates: Vec<serde_json::Value> = books
        .iter()
        .map(|book| {
            json!({
                "venue": book.venue,
                "venue_symbol": book.venue_symbol,
                "bid_price": book.bid_price,
                "bid_qty": book.bid_qty,
                "ask_price": book.ask_price,
                "ask_qty": book.ask_qty,
                "book_age_us": state.top_of_book.age_us(&book.venue, &book.symbol)
            })
        })
        .collect();

    (
        StatusCode::OK,
        Json(json!({
            "status": "ok",
            "symbol": symbol_upper,
            "side": side,
            "requested_qty": qty,
            "route": {
                "venue": book.venue,
                "venue_symbol": book.venue_symbol,
                "price": price,
                "available_qty": available_qty,
                "routable_qty": qty.min(available_qty),
                "reason": if side.starts_with("BUY") {
                    "lowest fresh ask"
                } else {
                    "highest fresh bid"
                }
            },
            "candidates": candidates
        })),
    )
}

fn fresh_books_for_symbol(state: &ApiState, symbol: &str) -> Vec<TopOfBook> {
    state
        .top_of_book
        .all_for_symbol(symbol)
        .into_iter()
        .filter(|book| {
            let stale_after_ms = state.config.stale_after_ms_for(&book.venue).unwrap_or(3000);
            state
                .top_of_book
                .is_fresh(&book.venue, &book.symbol, stale_after_ms)
        })
        .collect()
}

fn book_side_json_bid(book: &TopOfBook) -> serde_json::Value {
    json!({
        "venue": book.venue,
        "venue_symbol": book.venue_symbol,
        "price": book.bid_price,
        "qty": book.bid_qty,
        "sequence": book.sequence
    })
}

fn book_side_json_ask(book: &TopOfBook) -> serde_json::Value {
    json!({
        "venue": book.venue,
        "venue_symbol": book.venue_symbol,
        "price": book.ask_price,
        "qty": book.ask_qty,
        "sequence": book.sequence
    })
}