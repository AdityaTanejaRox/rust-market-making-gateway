use crate::metrics::latency::now_us;
use crate::types::NormalizedTopOfBookUpdate;
use crate::venue::binance::price_to_ticks;
use serde_json::json;

pub fn subscribe_message(product_id: &str) -> String {
    json!({
        "type": "subscribe",
        "product_ids": [product_id],
        "channels": ["ticker"]
    })
    .to_string()
}

pub fn parse_ticker(
    venue: &str,
    symbol: &str,
    venue_symbol: &str,
    raw: &str,
    receive_ts_us: i64,
) -> anyhow::Result<Option<NormalizedTopOfBookUpdate>> {
    let value: serde_json::Value = serde_json::from_str(raw)?;

    if value.get("type").and_then(|v| v.as_str()) != Some("ticker") {
        return Ok(None);
    }

    let product_id = value
        .get("product_id")
        .and_then(|v| v.as_str())
        .unwrap_or_default();

    if product_id != venue_symbol {
        return Ok(None);
    }

    let bid_price = required_price(&value, "best_bid")?;
    let ask_price = required_price(&value, "best_ask")?;

    let bid_qty = optional_qty(&value, "best_bid_size").unwrap_or(0.0);
    let ask_qty = optional_qty(&value, "best_ask_size").unwrap_or(0.0);

    let sequence = value
        .get("sequence")
        .and_then(|v| {
            if v.is_string() {
                v.as_str()?.parse::<u64>().ok()
            } else {
                v.as_u64()
            }
        })
        .unwrap_or_else(|| now_us() as u64);

    Ok(Some(NormalizedTopOfBookUpdate {
        venue: venue.to_string(),
        symbol: symbol.to_string(),
        venue_symbol: venue_symbol.to_string(),

        bid_price,
        bid_qty,

        ask_price,
        ask_qty,

        sequence,

        receive_ts_us,
    }))
}

fn required_price(value: &serde_json::Value, field: &str) -> anyhow::Result<i64> {
    let raw = value
        .get(field)
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing Coinbase field {}", field))?;

    price_to_ticks(raw)
}

fn optional_qty(value: &serde_json::Value, field: &str) -> Option<f64> {
    value.get(field)?.as_str()?.parse::<f64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_coinbase_subscribe_message() {
        let msg = subscribe_message("BTC-USD");

        assert!(msg.contains("subscribe"));
        assert!(msg.contains("BTC-USD"));
        assert!(msg.contains("ticker"));
    }

    #[test]
    fn parses_coinbase_ticker() {
        let raw = r#"{
            "type": "ticker",
            "product_id": "BTC-USD",
            "best_bid": "81628.20",
            "best_bid_size": "0.5",
            "best_ask": "81628.21",
            "best_ask_size": "0.7",
            "sequence": 123
        }"#;

        let update = parse_ticker(
            "COINBASE",
            "BTCUSDT",
            "BTC-USD",
            raw,
            now_us(),
        )
        .unwrap()
        .unwrap();

        assert_eq!(update.venue, "COINBASE");
        assert_eq!(update.symbol, "BTCUSDT");
        assert_eq!(update.bid_price, 8_162_820);
        assert_eq!(update.ask_price, 8_162_821);
        assert_eq!(update.bid_qty, 0.5);
        assert_eq!(update.ask_qty, 0.7);
        assert_eq!(update.sequence, 123);
    }

    #[test]
    fn ignores_non_ticker_message() {
        let raw = r#"{"type":"subscriptions"}"#;

        let update = parse_ticker("COINBASE", "BTCUSDT", "BTC-USD", raw, now_us()).unwrap();

        assert!(update.is_none());
    }
}