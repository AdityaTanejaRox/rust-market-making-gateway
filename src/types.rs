use serde::{Deserialize, Serialize};

pub type Price = i64;
pub type Quantity = f64;
pub type Sequence = u64;

#[derive(Debug, Clone, Serialize)]
pub struct TopOfBook {
    pub venue: String,
    pub symbol: String,
    pub venue_symbol: String,

    pub bid_price: Price,
    pub bid_qty: Quantity,

    pub ask_price: Price,
    pub ask_qty: Quantity,

    pub sequence: Sequence,

    pub receive_ts_us: i64,
    pub last_update_latency_us: i64,
}

#[derive(Debug, Clone)]
pub struct PriceLevel {
    pub price: Price,
    pub qty: Quantity,
}

#[derive(Debug, Clone)]
pub struct NormalizedDepthUpdate {
    pub venue: String,
    pub symbol: String,
    pub venue_symbol: String,

    pub first_update_id: Sequence,
    pub final_update_id: Sequence,

    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,

    pub receive_ts_us: i64,
}

#[derive(Debug, Clone)]
pub struct DepthSnapshot {
    pub venue: String,
    pub symbol: String,
    pub venue_symbol: String,

    pub last_update_id: Sequence,

    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,

    pub receive_ts_us: i64,
}

#[derive(Debug, Clone)]
pub struct NormalizedTopOfBookUpdate {
    pub venue: String,
    pub symbol: String,
    pub venue_symbol: String,

    pub bid_price: Price,
    pub bid_qty: Quantity,

    pub ask_price: Price,
    pub ask_qty: Quantity,

    pub sequence: Sequence,

    pub receive_ts_us: i64,
}

#[derive(Debug, Deserialize)]
pub struct BinanceDepthUpdate {
    #[serde(rename = "e")]
    pub event_type: String,

    #[serde(rename = "E")]
    pub event_time_ms: u64,

    #[serde(rename = "s")]
    pub symbol: String,

    #[serde(rename = "U")]
    pub first_update_id: Sequence,

    #[serde(rename = "u")]
    pub final_update_id: Sequence,

    #[serde(rename = "b")]
    pub bids: Vec<[String; 2]>,

    #[serde(rename = "a")]
    pub asks: Vec<[String; 2]>,
}

#[derive(Debug, Deserialize)]
pub struct BinanceDepthSnapshot {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: Sequence,

    pub bids: Vec<[String; 2]>,
    pub asks: Vec<[String; 2]>,
}