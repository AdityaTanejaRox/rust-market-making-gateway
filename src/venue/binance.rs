use crate::metrics::latency::now_us;
use crate::types::{
    BinanceDepthSnapshot, BinanceDepthUpdate, DepthSnapshot, NormalizedDepthUpdate, PriceLevel,
};
use anyhow::Context;

pub async fn fetch_depth_snapshot(
    venue: &str,
    symbol: &str,
    venue_symbol: &str,
    snapshot_url: &str,
) -> anyhow::Result<DepthSnapshot> {
    let receive_ts_us = now_us();

    let snapshot: BinanceDepthSnapshot = reqwest::get(snapshot_url)
        .await
        .context("failed to request Binance depth snapshot")?
        .error_for_status()
        .context("Binance snapshot returned non-success HTTP status")?
        .json()
        .await
        .context("failed to decode Binance depth snapshot JSON")?;

    Ok(DepthSnapshot {
        venue: venue.to_string(),
        symbol: symbol.to_string(),
        venue_symbol: venue_symbol.to_string(),
        last_update_id: snapshot.last_update_id,
        bids: parse_levels(snapshot.bids)?,
        asks: parse_levels(snapshot.asks)?,
        receive_ts_us,
    })
}

pub fn parse_depth_update(
    venue: &str,
    symbol: &str,
    venue_symbol: &str,
    raw: &str,
    receive_ts_us: i64,
) -> anyhow::Result<NormalizedDepthUpdate> {
    let msg: BinanceDepthUpdate =
        serde_json::from_str(raw).context("failed to parse Binance depth update")?;

    Ok(NormalizedDepthUpdate {
        venue: venue.to_string(),
        symbol: symbol.to_string(),
        venue_symbol: venue_symbol.to_string(),

        first_update_id: msg.first_update_id,
        final_update_id: msg.final_update_id,

        bids: parse_levels(msg.bids)?,
        asks: parse_levels(msg.asks)?,

        receive_ts_us,
    })
}

fn parse_levels(raw_levels: Vec<[String; 2]>) -> anyhow::Result<Vec<PriceLevel>> {
    let mut out = Vec::with_capacity(raw_levels.len());

    for [raw_price, raw_qty] in raw_levels {
        out.push(PriceLevel {
            price: price_to_ticks(&raw_price)?,
            qty: raw_qty.parse::<f64>()?,
        });
    }

    Ok(out)
}

pub fn price_to_ticks(raw: &str) -> anyhow::Result<i64> {
    let price = raw.parse::<f64>()?;
    Ok((price * 100.0).round() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_decimal_price_to_cent_ticks() {
        assert_eq!(price_to_ticks("81633.47").unwrap(), 8_163_347);
        assert_eq!(price_to_ticks("86.62").unwrap(), 8_662);
        assert_eq!(price_to_ticks("2382.18").unwrap(), 238_218);
    }

    #[test]
    fn parses_binance_depth_update() {
        let raw = r#"{
            "e": "depthUpdate",
            "E": 1778014907000,
            "s": "BTCUSDT",
            "U": 101,
            "u": 102,
            "b": [["81633.47", "1.25"]],
            "a": [["81633.48", "2.50"]]
        }"#;

        let update = parse_depth_update(
            "BINANCE",
            "BTCUSDT",
            "BTCUSDT",
            raw,
            now_us(),
        )
        .unwrap();

        assert_eq!(update.venue, "BINANCE");
        assert_eq!(update.symbol, "BTCUSDT");
        assert_eq!(update.first_update_id, 101);
        assert_eq!(update.final_update_id, 102);
        assert_eq!(update.bids[0].price, 8_163_347);
        assert_eq!(update.asks[0].price, 8_163_348);
    }
}