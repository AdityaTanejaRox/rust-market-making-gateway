use crate::types::TopOfBook;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct GeneratedQuote {
    pub bid_price: i64,
    pub ask_price: i64,
    pub mid_price: i64,
    pub half_spread_ticks: i64,
}

pub fn generate_quote(book: &TopOfBook) -> GeneratedQuote {
    let mid_price = (book.bid_price + book.ask_price) / 2;

    let half_spread_ticks = 1;

    GeneratedQuote {
        bid_price: mid_price - half_spread_ticks,
        ask_price: mid_price + half_spread_ticks,
        mid_price,
        half_spread_ticks,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_book() -> TopOfBook {
        TopOfBook {
            venue: "BINANCE".to_string(),
            symbol: "BTCUSDT".to_string(),
            venue_symbol: "BTCUSDT".to_string(),
            bid_price: 100,
            bid_qty: 2.0,
            ask_price: 102,
            ask_qty: 3.0,
            sequence: 1,
            receive_ts_us: 1000,
            last_update_latency_us: 10,
        }
    }

    #[test]
    fn generates_quote_around_mid_price() {
        let book = sample_book();
        let quote = generate_quote(&book);

        assert_eq!(quote.mid_price, 101);
        assert_eq!(quote.bid_price, 100);
        assert_eq!(quote.ask_price, 102);
        assert_eq!(quote.half_spread_ticks, 1);
    }

    #[test]
    fn handles_one_tick_market_spread() {
        let mut book = sample_book();
        book.bid_price = 100;
        book.ask_price = 101;

        let quote = generate_quote(&book);

        assert_eq!(quote.mid_price, 100);
        assert_eq!(quote.bid_price, 99);
        assert_eq!(quote.ask_price, 101);
    }
}