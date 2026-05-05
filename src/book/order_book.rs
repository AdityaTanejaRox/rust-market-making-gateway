use crate::metrics::latency::now_us;
use crate::types::{
    DepthSnapshot, NormalizedDepthUpdate, NormalizedTopOfBookUpdate, Price, PriceLevel, Quantity,
    TopOfBook,
};
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct LocalOrderBook {
    venue: String,
    symbol: String,
    venue_symbol: String,

    bids: BTreeMap<Price, Quantity>,
    asks: BTreeMap<Price, Quantity>,

    last_update_id: u64,
    last_receive_ts_us: i64,
    last_update_latency_us: i64,
}

#[derive(Debug)]
pub enum BookApplyResult {
    Applied,
    IgnoredOldUpdate,
    SequenceGap {
        expected_next: u64,
        received_first: u64,
        received_final: u64,
    },
}

impl LocalOrderBook {
    pub fn from_snapshot(snapshot: DepthSnapshot) -> Self {
        let mut book = Self {
            venue: snapshot.venue,
            symbol: snapshot.symbol,
            venue_symbol: snapshot.venue_symbol,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_update_id: snapshot.last_update_id,
            last_receive_ts_us: snapshot.receive_ts_us,
            last_update_latency_us: 0,
        };

        for level in snapshot.bids {
            if level.qty > 0.0 {
                book.bids.insert(level.price, level.qty);
            }
        }

        for level in snapshot.asks {
            if level.qty > 0.0 {
                book.asks.insert(level.price, level.qty);
            }
        }

        book
    }

    pub fn apply_depth_update(&mut self, update: NormalizedDepthUpdate) -> BookApplyResult {
        if update.final_update_id <= self.last_update_id {
            return BookApplyResult::IgnoredOldUpdate;
        }

        let expected_next = self.last_update_id + 1;

        if update.first_update_id > expected_next {
            return BookApplyResult::SequenceGap {
                expected_next,
                received_first: update.first_update_id,
                received_final: update.final_update_id,
            };
        }

        let apply_start_us = now_us();

        for level in update.bids {
            if level.qty <= 0.0 {
                self.bids.remove(&level.price);
            } else {
                self.bids.insert(level.price, level.qty);
            }
        }

        for level in update.asks {
            if level.qty <= 0.0 {
                self.asks.remove(&level.price);
            } else {
                self.asks.insert(level.price, level.qty);
            }
        }

        self.last_update_id = update.final_update_id;
        self.last_receive_ts_us = update.receive_ts_us;
        self.last_update_latency_us = now_us() - apply_start_us;

        BookApplyResult::Applied
    }

    pub fn top_of_book(&self) -> Option<TopOfBook> {
        let (&bid_price, &bid_qty) = self.bids.iter().next_back()?;
        let (&ask_price, &ask_qty) = self.asks.iter().next()?;

        Some(TopOfBook {
            venue: self.venue.clone(),
            symbol: self.symbol.clone(),
            venue_symbol: self.venue_symbol.clone(),

            bid_price,
            bid_qty,

            ask_price,
            ask_qty,

            sequence: self.last_update_id,

            receive_ts_us: self.last_receive_ts_us,
            last_update_latency_us: self.last_update_latency_us,
        })
    }

    pub fn last_update_id(&self) -> u64 {
        self.last_update_id
    }

    pub fn bid_depth_len(&self) -> usize {
        self.bids.len()
    }

    pub fn ask_depth_len(&self) -> usize {
        self.asks.len()
    }
}

pub fn top_update_to_book(update: NormalizedTopOfBookUpdate) -> TopOfBook {
    let done_us = now_us();

    TopOfBook {
        venue: update.venue,
        symbol: update.symbol,
        venue_symbol: update.venue_symbol,

        bid_price: update.bid_price,
        bid_qty: update.bid_qty,

        ask_price: update.ask_price,
        ask_qty: update.ask_qty,

        sequence: update.sequence,

        receive_ts_us: update.receive_ts_us,
        last_update_latency_us: done_us - update.receive_ts_us,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> DepthSnapshot {
        DepthSnapshot {
            venue: "BINANCE".to_string(),
            symbol: "BTCUSDT".to_string(),
            venue_symbol: "BTCUSDT".to_string(),
            last_update_id: 100,
            bids: vec![
                PriceLevel { price: 99, qty: 1.0 },
                PriceLevel { price: 98, qty: 2.0 },
            ],
            asks: vec![
                PriceLevel { price: 101, qty: 1.0 },
                PriceLevel { price: 102, qty: 2.0 },
            ],
            receive_ts_us: now_us(),
        }
    }

    #[test]
    fn builds_top_of_book_from_snapshot() {
        let book = LocalOrderBook::from_snapshot(snapshot());
        let top = book.top_of_book().unwrap();

        assert_eq!(top.bid_price, 99);
        assert_eq!(top.ask_price, 101);
        assert_eq!(top.sequence, 100);
    }

    #[test]
    fn applies_in_order_depth_update() {
        let mut book = LocalOrderBook::from_snapshot(snapshot());

        let result = book.apply_depth_update(NormalizedDepthUpdate {
            venue: "BINANCE".to_string(),
            symbol: "BTCUSDT".to_string(),
            venue_symbol: "BTCUSDT".to_string(),
            first_update_id: 101,
            final_update_id: 101,
            bids: vec![PriceLevel { price: 100, qty: 3.0 }],
            asks: vec![PriceLevel { price: 101, qty: 0.0 }],
            receive_ts_us: now_us(),
        });

        assert!(matches!(result, BookApplyResult::Applied));

        let top = book.top_of_book().unwrap();
        assert_eq!(top.bid_price, 100);
        assert_eq!(top.ask_price, 102);
        assert_eq!(top.sequence, 101);
    }

    #[test]
    fn ignores_old_update() {
        let mut book = LocalOrderBook::from_snapshot(snapshot());

        let result = book.apply_depth_update(NormalizedDepthUpdate {
            venue: "BINANCE".to_string(),
            symbol: "BTCUSDT".to_string(),
            venue_symbol: "BTCUSDT".to_string(),
            first_update_id: 99,
            final_update_id: 100,
            bids: vec![PriceLevel { price: 100, qty: 3.0 }],
            asks: vec![],
            receive_ts_us: now_us(),
        });

        assert!(matches!(result, BookApplyResult::IgnoredOldUpdate));
    }

    #[test]
    fn detects_sequence_gap() {
        let mut book = LocalOrderBook::from_snapshot(snapshot());

        let result = book.apply_depth_update(NormalizedDepthUpdate {
            venue: "BINANCE".to_string(),
            symbol: "BTCUSDT".to_string(),
            venue_symbol: "BTCUSDT".to_string(),
            first_update_id: 105,
            final_update_id: 106,
            bids: vec![],
            asks: vec![],
            receive_ts_us: now_us(),
        });

        assert!(matches!(result, BookApplyResult::SequenceGap { .. }));
    }

    #[test]
    fn converts_top_update_to_book() {
        let update = NormalizedTopOfBookUpdate {
            venue: "COINBASE".to_string(),
            symbol: "BTCUSDT".to_string(),
            venue_symbol: "BTC-USD".to_string(),
            bid_price: 100,
            bid_qty: 1.0,
            ask_price: 101,
            ask_qty: 2.0,
            sequence: 7,
            receive_ts_us: now_us(),
        };

        let book = top_update_to_book(update);

        assert_eq!(book.venue, "COINBASE");
        assert_eq!(book.bid_price, 100);
        assert_eq!(book.ask_price, 101);
    }
}