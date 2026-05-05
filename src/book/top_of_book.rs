use crate::metrics::latency::now_us;
use crate::types::TopOfBook;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Default, Clone)]
pub struct SharedTopOfBook {
    inner: Arc<RwLock<HashMap<String, TopOfBook>>>,
}

impl SharedTopOfBook {
    pub fn update(&self, tob: TopOfBook) {
        self.inner.write().insert(make_key(&tob.venue, &tob.symbol), tob);
    }

    pub fn get(&self, venue: &str, symbol: &str) -> Option<TopOfBook> {
        self.inner.read().get(&make_key(venue, symbol)).cloned()
    }

    pub fn all(&self) -> Vec<TopOfBook> {
        let mut books: Vec<TopOfBook> = self.inner.read().values().cloned().collect();
        books.sort_by(|a, b| {
            let venue_cmp = a.venue.cmp(&b.venue);
            if venue_cmp == std::cmp::Ordering::Equal {
                a.symbol.cmp(&b.symbol)
            } else {
                venue_cmp
            }
        });
        books
    }

    pub fn all_for_symbol(&self, symbol: &str) -> Vec<TopOfBook> {
        let symbol_upper = symbol.to_uppercase();

        let mut books: Vec<TopOfBook> = self
            .inner
            .read()
            .values()
            .filter(|book| book.symbol.eq_ignore_ascii_case(&symbol_upper))
            .cloned()
            .collect();

        books.sort_by(|a, b| a.venue.cmp(&b.venue));
        books
    }

    pub fn age_us(&self, venue: &str, symbol: &str) -> Option<i64> {
        self.get(venue, symbol).map(|book| now_us() - book.receive_ts_us)
    }

    pub fn age_ms(&self, venue: &str, symbol: &str) -> Option<i64> {
        self.age_us(venue, symbol).map(|age_us| age_us / 1_000)
    }

    pub fn is_fresh(&self, venue: &str, symbol: &str, stale_after_ms: u64) -> bool {
        match self.age_ms(venue, symbol) {
            Some(age_ms) => age_ms <= stale_after_ms as i64,
            None => false,
        }
    }

    pub fn active_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.inner.read().keys().cloned().collect();
        keys.sort();
        keys
    }
}

fn make_key(venue: &str, symbol: &str) -> String {
    format!("{}:{}", venue.to_uppercase(), symbol.to_uppercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn book(venue: &str, symbol: &str, receive_ts_us: i64) -> TopOfBook {
        TopOfBook {
            venue: venue.to_string(),
            symbol: symbol.to_string(),
            venue_symbol: symbol.to_string(),
            bid_price: 100,
            bid_qty: 1.0,
            ask_price: 101,
            ask_qty: 2.0,
            sequence: 1,
            receive_ts_us,
            last_update_latency_us: 5,
        }
    }

    #[test]
    fn stores_and_retrieves_by_venue_and_symbol_case_insensitive() {
        let shared = SharedTopOfBook::default();
        shared.update(book("BINANCE", "BTCUSDT", now_us()));

        let found = shared.get("binance", "btcusdt").unwrap();

        assert_eq!(found.venue, "BINANCE");
        assert_eq!(found.symbol, "BTCUSDT");
    }

    #[test]
    fn returns_all_books_for_symbol_across_venues() {
        let shared = SharedTopOfBook::default();
        let ts = now_us();

        shared.update(book("BINANCE", "BTCUSDT", ts));
        shared.update(book("COINBASE", "BTCUSDT", ts));
        shared.update(book("KRAKEN", "ETHUSDT", ts));

        let btc_books = shared.all_for_symbol("BTCUSDT");

        assert_eq!(btc_books.len(), 2);
    }

    #[test]
    fn detects_fresh_book() {
        let shared = SharedTopOfBook::default();
        shared.update(book("BINANCE", "BTCUSDT", now_us()));

        assert!(shared.is_fresh("BINANCE", "BTCUSDT", 3000));
    }

    #[test]
    fn detects_stale_book() {
        let shared = SharedTopOfBook::default();
        let old_ts = now_us() - 10_000_000;

        shared.update(book("BINANCE", "BTCUSDT", old_ts));

        assert!(!shared.is_fresh("BINANCE", "BTCUSDT", 3000));
    }
}