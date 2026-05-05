use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct Metrics {
    pub messages_received: AtomicU64,
    pub messages_parsed: AtomicU64,
    pub book_updates_applied: AtomicU64,
    pub sequence_gaps_detected: AtomicU64,
    pub reconnects_total: AtomicU64,
    pub heartbeat_timeouts: AtomicU64,
    pub rate_limit_blocks: AtomicU64,
    pub parse_errors: AtomicU64,
    pub stale_books: AtomicU64,
    pub quotes_generated: AtomicU64,
}

impl Metrics {
    pub fn inc_messages_received(&self) {
        self.messages_received.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            messages_received: self.messages_received.load(Ordering::Relaxed),
            messages_parsed: self.messages_parsed.load(Ordering::Relaxed),
            book_updates_applied: self.book_updates_applied.load(Ordering::Relaxed),
            sequence_gaps_detected: self.sequence_gaps_detected.load(Ordering::Relaxed),
            reconnects_total: self.reconnects_total.load(Ordering::Relaxed),
            heartbeat_timeouts: self.heartbeat_timeouts.load(Ordering::Relaxed),
            rate_limit_blocks: self.rate_limit_blocks.load(Ordering::Relaxed),
            parse_errors: self.parse_errors.load(Ordering::Relaxed),
            stale_books: self.stale_books.load(Ordering::Relaxed),
            quotes_generated: self.quotes_generated.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct MetricsSnapshot {
    pub messages_received: u64,
    pub messages_parsed: u64,
    pub book_updates_applied: u64,
    pub sequence_gaps_detected: u64,
    pub reconnects_total: u64,
    pub heartbeat_timeouts: u64,
    pub rate_limit_blocks: u64,
    pub parse_errors: u64,
    pub stale_books: u64,
    pub quotes_generated: u64,
}