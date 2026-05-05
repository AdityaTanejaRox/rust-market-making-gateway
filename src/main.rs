mod api;
mod book;
mod config;
mod error;
mod logging;
mod metrics;
mod strategy;
mod types;
mod venue;

use crate::book::top_of_book::SharedTopOfBook;
use crate::config::AppConfig;
use crate::metrics::counters::Metrics;
use crate::venue::supervisor::VenueSupervisor;

use std::sync::Arc;
use tokio::sync::broadcast;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logging::init();

    let config = AppConfig::load("configs/binance.yaml")?;
    let runtime_configs = config.venue_runtime_configs()?;

    let metrics = Arc::new(Metrics::default());
    let top_of_book = SharedTopOfBook::default();

    let (shutdown_tx, _) = broadcast::channel::<()>(64);

    tracing::info!(
        service = "rust-market-making-gateway",
        venues = ?config.venues.iter().map(|v| &v.name).collect::<Vec<_>>(),
        runtime_count = runtime_configs.len(),
        "starting gateway"
    );

    let http_config = config.clone();
    let http_metrics = metrics.clone();
    let http_top_of_book = top_of_book.clone();
    let http_shutdown_rx = shutdown_tx.subscribe();

    let http_handle = tokio::spawn(async move {
        if let Err(err) = api::http::run_http_server(
            http_config,
            http_metrics,
            http_top_of_book,
            http_shutdown_rx,
        )
        .await
        {
            tracing::error!(error = ?err, "http server failed");
        }
    });

    let mut venue_handles = Vec::new();

    for runtime_config in runtime_configs {
        let supervisor = VenueSupervisor::new(
            runtime_config.clone(),
            metrics.clone(),
            top_of_book.clone(),
        );

        let venue_shutdown_rx = shutdown_tx.subscribe();

        let handle = tokio::spawn(async move {
            if let Err(err) = supervisor.run(venue_shutdown_rx).await {
                tracing::error!(
                    venue = %runtime_config.name,
                    symbol = %runtime_config.symbol,
                    venue_symbol = %runtime_config.venue_symbol,
                    error = ?err,
                    "venue supervisor failed"
                );
            }
        });

        venue_handles.push(handle);
    }

    tokio::signal::ctrl_c().await?;

    tracing::info!("shutdown signal received");

    let _ = shutdown_tx.send(());

    let _ = http_handle.await;

    for handle in venue_handles {
        let _ = handle.await;
    }

    tracing::info!("gateway shutdown complete");

    Ok(())
}