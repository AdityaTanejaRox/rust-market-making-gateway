use crate::metrics::latency::now_us;
use crate::types::NormalizedTopOfBookUpdate;
use crate::venue::binance::price_to_ticks;
use serde_json::json;

pub fn subscribe_message(symbol: &str) -> String {
    json!({
        "method": "subscribe",
        "params": {
            "channel": "ticker",
            "symbol": [symbol],
            "event_trigger": "bbo",
            "snapshot": true
        }
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

    if value.get("channel").and_then(|v| v.as_str()) != Some("ticker") {
        return Ok(None);
    }

    let Some(first) = value
        .get("data")
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.first())
    else {
        return Ok(None);
    };

    let msg_symbol = first
        .get("symbol")
        .and_then(|v| v.as_str())
        .unwrap_or_default();

    if msg_symbol != venue_symbol {
        return Ok(None);
    }

    let bid_price = required_price(first, "bid")?;
    let ask_price = required_price(first, "ask")?;

    let bid_qty = optional_qty(first, "bid_qty").unwrap_or(0.0);
    let ask_qty = optional_qty(first, "ask_qty").unwrap_or(0.0);

    let sequence = now_us() as u64;

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
    let raw_value = value
        .get(field)
        .ok_or_else(|| anyhow::anyhow!("missing Kraken field {}", field))?;

    if let Some(raw) = raw_value.as_str() {
        return price_to_ticks(raw);
    }

    if let Some(num) = raw_value.as_f64() {
        return Ok((num * 100.0).round() as i64);
    }

    anyhow::bail!("invalid Kraken price field {}", field)
}

fn optional_qty(value: &serde_json::Value, field: &str) -> Option<f64> {
    let raw_value = value.get(field)?;

    if let Some(raw) = raw_value.as_str() {
        return raw.parse::<f64>().ok();
    }

    raw_value.as_f64()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_kraken_subscribe_message() {
        let msg = subscribe_message("BTC/USD");

        assert!(msg.contains("subscribe"));
        assert!(msg.contains("BTC/USD"));
        assert!(msg.contains("ticker"));
    }

    #[test]
    fn parses_kraken_ticker() {
        let raw = r#"{
            "channel": "ticker",
            "type": "snapshot",
            "data": [
                {
                    "symbol": "BTC/USD",
                    "bid": 81630.00,
                    "bid_qty": 0.25,
                    "ask": 81630.10,
                    "ask_qty": 0.40
                }
            ]
        }"#;

        let update = parse_ticker(
            "KRAKEN",
            "BTCUSDT",
            "BTC/USD",
            raw,
            now_us(),
        )
        .unwrap()
        .unwrap();

        assert_eq!(update.venue, "KRAKEN");
        assert_eq!(update.symbol, "BTCUSDT");
        assert_eq!(update.bid_price, 8_163_000);
        assert_eq!(update.ask_price, 8_163_010);
        assert_eq!(update.bid_qty, 0.25);
        assert_eq!(update.ask_qty, 0.40);
    }

    #[test]
    fn ignores_non_ticker_message() {
        let raw = r#"{"method":"pong"}"#;

        let update = parse_ticker("KRAKEN", "BTCUSDT", "BTC/USD", raw, now_us()).unwrap();

        assert!(update.is_none());
    }
}