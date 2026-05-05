use serde::Deserialize;
use std::fs;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub service: ServiceConfig,
    pub venues: Vec<VenueConfig>,
    pub rate_limits: RateLimitConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServiceConfig {
    pub name: String,
    pub http_addr: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VenueConfig {
    pub name: String,
    pub kind: String,

    pub ws_base_url: Option<String>,
    pub rest_base_url: Option<String>,
    pub ws_url: Option<String>,

    pub heartbeat_timeout_ms: u64,
    pub reconnect_base_delay_ms: u64,
    pub reconnect_max_delay_ms: u64,
    pub stale_after_ms: u64,

    pub markets: Vec<MarketConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MarketConfig {
    pub symbol: String,
    pub venue_symbol: String,
}

#[derive(Debug, Clone)]
pub struct VenueRuntimeConfig {
    pub name: String,
    pub kind: VenueKind,

    pub symbol: String,
    pub venue_symbol: String,

    pub stream_url: String,
    pub snapshot_url: Option<String>,

    pub heartbeat_timeout_ms: u64,
    pub reconnect_base_delay_ms: u64,
    pub reconnect_max_delay_ms: u64,
    pub stale_after_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VenueKind {
    Binance,
    Coinbase,
    Kraken,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RateLimitConfig {
    pub outbound_messages_per_second: u32,
}

impl AppConfig {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let raw = fs::read_to_string(path)?;
        Ok(serde_yaml::from_str(&raw)?)
    }

    pub fn venue_runtime_configs(&self) -> anyhow::Result<Vec<VenueRuntimeConfig>> {
        let mut out = Vec::new();

        for venue in &self.venues {
            let kind = parse_venue_kind(&venue.kind)?;

            for market in &venue.markets {
                let symbol_upper = market.symbol.to_uppercase();

                let runtime = match kind {
                    VenueKind::Binance => {
                        let ws_base = venue
                            .ws_base_url
                            .as_ref()
                            .ok_or_else(|| anyhow::anyhow!("BINANCE missing ws_base_url"))?;

                        let rest_base = venue
                            .rest_base_url
                            .as_ref()
                            .ok_or_else(|| anyhow::anyhow!("BINANCE missing rest_base_url"))?;

                        let venue_symbol_upper = market.venue_symbol.to_uppercase();
                        let venue_symbol_lower = market.venue_symbol.to_lowercase();

                        VenueRuntimeConfig {
                            name: venue.name.clone(),
                            kind,
                            symbol: symbol_upper,
                            venue_symbol: venue_symbol_upper.clone(),
                            stream_url: format!("{}/{}@depth@100ms", ws_base, venue_symbol_lower),
                            snapshot_url: Some(format!(
                                "{}/api/v3/depth?symbol={}&limit=1000",
                                rest_base, venue_symbol_upper
                            )),
                            heartbeat_timeout_ms: venue.heartbeat_timeout_ms,
                            reconnect_base_delay_ms: venue.reconnect_base_delay_ms,
                            reconnect_max_delay_ms: venue.reconnect_max_delay_ms,
                            stale_after_ms: venue.stale_after_ms,
                        }
                    }

                    VenueKind::Coinbase | VenueKind::Kraken => {
                        let ws_url = venue
                            .ws_url
                            .as_ref()
                            .ok_or_else(|| anyhow::anyhow!("{} missing ws_url", venue.name))?;

                        VenueRuntimeConfig {
                            name: venue.name.clone(),
                            kind,
                            symbol: symbol_upper,
                            venue_symbol: market.venue_symbol.clone(),
                            stream_url: ws_url.clone(),
                            snapshot_url: None,
                            heartbeat_timeout_ms: venue.heartbeat_timeout_ms,
                            reconnect_base_delay_ms: venue.reconnect_base_delay_ms,
                            reconnect_max_delay_ms: venue.reconnect_max_delay_ms,
                            stale_after_ms: venue.stale_after_ms,
                        }
                    }
                };

                out.push(runtime);
            }
        }

        Ok(out)
    }

    pub fn default_market(&self) -> Option<(String, String)> {
        let venue = self.venues.first()?;
        let market = venue.markets.first()?;

        Some((venue.name.to_uppercase(), market.symbol.to_uppercase()))
    }

    pub fn stale_after_ms_for(&self, venue_name: &str) -> Option<u64> {
        self.venues
            .iter()
            .find(|venue| venue.name.eq_ignore_ascii_case(venue_name))
            .map(|venue| venue.stale_after_ms)
    }

    pub fn all_configured_markets(&self) -> Vec<(String, String)> {
        let mut out = Vec::new();

        for venue in &self.venues {
            for market in &venue.markets {
                out.push((venue.name.to_uppercase(), market.symbol.to_uppercase()));
            }
        }

        out
    }
}

fn parse_venue_kind(raw: &str) -> anyhow::Result<VenueKind> {
    match raw.to_lowercase().as_str() {
        "binance" => Ok(VenueKind::Binance),
        "coinbase" => Ok(VenueKind::Coinbase),
        "kraken" => Ok(VenueKind::Kraken),
        other => anyhow::bail!("unsupported venue kind: {}", other),
    }
}