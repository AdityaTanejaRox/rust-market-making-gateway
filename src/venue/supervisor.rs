use crate::book::order_book::{top_update_to_book, BookApplyResult, LocalOrderBook};
use crate::book::top_of_book::SharedTopOfBook;
use crate::config::{VenueKind, VenueRuntimeConfig};
use crate::metrics::counters::Metrics;
use crate::metrics::latency::now_us;
use crate::venue::binance::{fetch_depth_snapshot, parse_depth_update};
use crate::venue::{coinbase, kraken};

use futures_util::{SinkExt, StreamExt};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::time::{sleep, Duration, Instant};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

pub struct VenueSupervisor {
    config: VenueRuntimeConfig,
    metrics: Arc<Metrics>,
    top_of_book: SharedTopOfBook,
}

impl VenueSupervisor {
    pub fn new(
        config: VenueRuntimeConfig,
        metrics: Arc<Metrics>,
        top_of_book: SharedTopOfBook,
    ) -> Self {
        Self {
            config,
            metrics,
            top_of_book,
        }
    }

    pub async fn run(
        &self,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> anyhow::Result<()> {
        let mut reconnect_delay_ms = self.config.reconnect_base_delay_ms;

        loop {
            let stream_shutdown_rx = shutdown_rx.resubscribe();

            tokio::select! {
                _ = shutdown_rx.recv() => {
                    tracing::info!(
                        venue = %self.config.name,
                        symbol = %self.config.symbol,
                        "venue supervisor received shutdown"
                    );

                    return Ok(());
                }

                result = self.connect_and_stream(stream_shutdown_rx) => {
                    match result {
                        Ok(()) => {
                            reconnect_delay_ms = self.config.reconnect_base_delay_ms;
                        }

                        Err(err) => {
                            self.metrics
                                .reconnects_total
                                .fetch_add(1, Ordering::Relaxed);

                            tracing::error!(
                                venue = %self.config.name,
                                symbol = %self.config.symbol,
                                error = ?err,
                                reconnect_delay_ms,
                                "venue stream failed; reconnecting"
                            );

                            sleep(Duration::from_millis(reconnect_delay_ms)).await;

                            reconnect_delay_ms = (reconnect_delay_ms * 2)
                                .min(self.config.reconnect_max_delay_ms);
                        }
                    }
                }
            }
        }
    }

    async fn connect_and_stream(
        &self,
        shutdown_rx: broadcast::Receiver<()>,
    ) -> anyhow::Result<()> {
        match self.config.kind {
            VenueKind::Binance => self.connect_binance_depth(shutdown_rx).await,
            VenueKind::Coinbase => self.connect_top_of_book_feed(shutdown_rx).await,
            VenueKind::Kraken => self.connect_top_of_book_feed(shutdown_rx).await,
        }
    }

    async fn connect_binance_depth(
        &self,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> anyhow::Result<()> {
        tracing::info!(
            venue = %self.config.name,
            symbol = %self.config.symbol,
            venue_symbol = %self.config.venue_symbol,
            stream_url = %self.config.stream_url,
            "connecting Binance depth websocket"
        );

        let (ws_stream, _) = connect_async(&self.config.stream_url).await?;

        tracing::info!(
            venue = %self.config.name,
            symbol = %self.config.symbol,
            "websocket connected"
        );

        let snapshot_url = self
            .config
            .snapshot_url
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Binance missing snapshot_url"))?;

        let snapshot = fetch_depth_snapshot(
            &self.config.name,
            &self.config.symbol,
            &self.config.venue_symbol,
            snapshot_url,
        )
        .await?;

        tracing::info!(
            venue = %snapshot.venue,
            symbol = %snapshot.symbol,
            venue_symbol = %snapshot.venue_symbol,
            last_update_id = snapshot.last_update_id,
            bid_levels = snapshot.bids.len(),
            ask_levels = snapshot.asks.len(),
            "depth snapshot loaded"
        );

        let mut local_book = LocalOrderBook::from_snapshot(snapshot);

        if let Some(top) = local_book.top_of_book() {
            self.top_of_book.update(top);
        }

        let (mut write, mut read) = ws_stream.split();
        let mut last_message_time = Instant::now();

        loop {
            let heartbeat_timeout = Duration::from_millis(self.config.heartbeat_timeout_ms);

            tokio::select! {
                _ = shutdown_rx.recv() => {
                    tracing::info!(
                        venue = %self.config.name,
                        symbol = %self.config.symbol,
                        "closing websocket stream on shutdown"
                    );

                    let _ = write.send(Message::Close(None)).await;
                    return Ok(());
                }

                maybe_msg = read.next() => {
                    let Some(msg) = maybe_msg else {
                        anyhow::bail!("websocket stream ended");
                    };

                    let msg = msg?;

                    match msg {
                        Message::Text(raw) => {
                            last_message_time = Instant::now();

                            match self.handle_binance_depth_message(&raw, &mut local_book).await? {
                                StreamAction::Continue => {}

                                StreamAction::ResyncRequired => {
                                    anyhow::bail!("depth sequence gap requires resync");
                                }
                            }
                        }

                        Message::Ping(payload) => {
                            last_message_time = Instant::now();
                            write.send(Message::Pong(payload)).await?;
                        }

                        Message::Pong(_) => {
                            last_message_time = Instant::now();
                        }

                        Message::Close(frame) => {
                            anyhow::bail!("websocket closed: {:?}", frame);
                        }

                        Message::Binary(_) => {}
                        Message::Frame(_) => {}
                    }
                }

                _ = sleep(Duration::from_secs(5)) => {
                    if last_message_time.elapsed() > heartbeat_timeout {
                        self.metrics
                            .heartbeat_timeouts
                            .fetch_add(1, Ordering::Relaxed);

                        anyhow::bail!("heartbeat timeout");
                    }
                }
            }
        }
    }

    async fn connect_top_of_book_feed(
        &self,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> anyhow::Result<()> {
        tracing::info!(
            venue = %self.config.name,
            symbol = %self.config.symbol,
            venue_symbol = %self.config.venue_symbol,
            stream_url = %self.config.stream_url,
            "connecting top-of-book websocket"
        );

        let (ws_stream, _) = connect_async(&self.config.stream_url).await?;

        tracing::info!(
            venue = %self.config.name,
            symbol = %self.config.symbol,
            "websocket connected"
        );

        let (mut write, mut read) = ws_stream.split();

        let subscribe = match self.config.kind {
            VenueKind::Coinbase => coinbase::subscribe_message(&self.config.venue_symbol),
            VenueKind::Kraken => kraken::subscribe_message(&self.config.venue_symbol),
            VenueKind::Binance => unreachable!(),
        };

        write.send(Message::Text(subscribe)).await?;

        tracing::info!(
            venue = %self.config.name,
            symbol = %self.config.symbol,
            venue_symbol = %self.config.venue_symbol,
            "subscription sent"
        );

        let mut last_message_time = Instant::now();

        loop {
            let heartbeat_timeout = Duration::from_millis(self.config.heartbeat_timeout_ms);

            tokio::select! {
                _ = shutdown_rx.recv() => {
                    tracing::info!(
                        venue = %self.config.name,
                        symbol = %self.config.symbol,
                        "closing websocket stream on shutdown"
                    );

                    let _ = write.send(Message::Close(None)).await;
                    return Ok(());
                }

                maybe_msg = read.next() => {
                    let Some(msg) = maybe_msg else {
                        anyhow::bail!("websocket stream ended");
                    };

                    let msg = msg?;

                    match msg {
                        Message::Text(raw) => {
                            last_message_time = Instant::now();
                            self.handle_top_of_book_message(&raw).await?;
                        }

                        Message::Ping(payload) => {
                            last_message_time = Instant::now();
                            write.send(Message::Pong(payload)).await?;
                        }

                        Message::Pong(_) => {
                            last_message_time = Instant::now();
                        }

                        Message::Close(frame) => {
                            anyhow::bail!("websocket closed: {:?}", frame);
                        }

                        Message::Binary(_) => {}
                        Message::Frame(_) => {}
                    }
                }

                _ = sleep(Duration::from_secs(5)) => {
                    if last_message_time.elapsed() > heartbeat_timeout {
                        self.metrics
                            .heartbeat_timeouts
                            .fetch_add(1, Ordering::Relaxed);

                        anyhow::bail!("heartbeat timeout");
                    }
                }
            }
        }
    }

    async fn handle_binance_depth_message(
        &self,
        raw: &str,
        local_book: &mut LocalOrderBook,
    ) -> anyhow::Result<StreamAction> {
        let receive_ts_us = now_us();

        self.metrics
            .messages_received
            .fetch_add(1, Ordering::Relaxed);

        let parse_start_us = now_us();

        let update = match parse_depth_update(
            &self.config.name,
            &self.config.symbol,
            &self.config.venue_symbol,
            raw,
            receive_ts_us,
        ) {
            Ok(update) => update,
            Err(err) => {
                self.metrics.parse_errors.fetch_add(1, Ordering::Relaxed);

                tracing::warn!(
                    venue = %self.config.name,
                    symbol = %self.config.symbol,
                    error = ?err,
                    raw = %raw,
                    "failed to parse depth websocket message"
                );

                return Ok(StreamAction::Continue);
            }
        };

        let parse_latency_us = now_us() - parse_start_us;

        self.metrics
            .messages_parsed
            .fetch_add(1, Ordering::Relaxed);

        match local_book.apply_depth_update(update) {
            BookApplyResult::IgnoredOldUpdate => {
                return Ok(StreamAction::Continue);
            }

            BookApplyResult::SequenceGap {
                expected_next,
                received_first,
                received_final,
            } => {
                self.metrics
                    .sequence_gaps_detected
                    .fetch_add(1, Ordering::Relaxed);

                tracing::warn!(
                    venue = %self.config.name,
                    symbol = %self.config.symbol,
                    expected_next,
                    received_first,
                    received_final,
                    current_last_update_id = local_book.last_update_id(),
                    "depth sequence gap detected"
                );

                return Ok(StreamAction::ResyncRequired);
            }

            BookApplyResult::Applied => {}
        }

        let Some(top) = local_book.top_of_book() else {
            return Ok(StreamAction::Continue);
        };

        self.top_of_book.update(top.clone());

        let applied_count = self
            .metrics
            .book_updates_applied
            .fetch_add(1, Ordering::Relaxed)
            + 1;

        if applied_count == 1 || applied_count % 1000 == 0 {
            tracing::info!(
                event_type = "depth_update_applied",
                venue = %top.venue,
                symbol = %top.symbol,
                venue_symbol = %top.venue_symbol,
                bid_price = top.bid_price,
                bid_qty = top.bid_qty,
                ask_price = top.ask_price,
                ask_qty = top.ask_qty,
                sequence = top.sequence,
                bid_levels = local_book.bid_depth_len(),
                ask_levels = local_book.ask_depth_len(),
                parse_latency_us,
                book_apply_latency_us = top.last_update_latency_us,
                applied_count,
                "local depth book updated"
            );
        }

        Ok(StreamAction::Continue)
    }

    async fn handle_top_of_book_message(&self, raw: &str) -> anyhow::Result<()> {
        let receive_ts_us = now_us();

        self.metrics
            .messages_received
            .fetch_add(1, Ordering::Relaxed);

        let parsed = match self.config.kind {
            VenueKind::Coinbase => coinbase::parse_ticker(
                &self.config.name,
                &self.config.symbol,
                &self.config.venue_symbol,
                raw,
                receive_ts_us,
            )?,

            VenueKind::Kraken => kraken::parse_ticker(
                &self.config.name,
                &self.config.symbol,
                &self.config.venue_symbol,
                raw,
                receive_ts_us,
            )?,

            VenueKind::Binance => unreachable!(),
        };

        let Some(update) = parsed else {
            return Ok(());
        };

        self.metrics
            .messages_parsed
            .fetch_add(1, Ordering::Relaxed);

        let top = top_update_to_book(update);
        self.top_of_book.update(top.clone());

        let applied_count = self
            .metrics
            .book_updates_applied
            .fetch_add(1, Ordering::Relaxed)
            + 1;

        if applied_count == 1 || applied_count % 1000 == 0 {
            tracing::info!(
                event_type = "top_of_book_update_applied",
                venue = %top.venue,
                symbol = %top.symbol,
                venue_symbol = %top.venue_symbol,
                bid_price = top.bid_price,
                bid_qty = top.bid_qty,
                ask_price = top.ask_price,
                ask_qty = top.ask_qty,
                sequence = top.sequence,
                latency_us = top.last_update_latency_us,
                applied_count,
                "top-of-book feed updated"
            );
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum StreamAction {
    Continue,
    ResyncRequired,
}