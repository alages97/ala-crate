use std::net::SocketAddr;

use anyhow::{Context, Result};
use metrics_exporter_prometheus::PrometheusBuilder;

/// Shared bounds for every latency metric, all of which record seconds.
/// Explicit bounds make the exporter produce aggregatable Prometheus
/// histograms (`*_bucket`) rather than its default process-local summaries.
const LATENCY_BUCKETS_SECONDS: &[f64] = &[
    0.000_01, 0.000_025, 0.000_05, 0.000_1, 0.000_25, 0.000_5, 0.001, 0.002_5, 0.005, 0.01, 0.025,
    0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 90.0,
];

fn configured_prometheus_builder() -> Result<PrometheusBuilder> {
    PrometheusBuilder::new()
        .set_buckets(LATENCY_BUCKETS_SECONDS)
        .context("configure Prometheus latency histogram buckets")
}

pub fn install_metrics_listener(address: SocketAddr) -> Result<()> {
    configured_prometheus_builder()?
        .with_http_listener(address)
        .install()
        .context("install Prometheus metrics recorder")?;
    Ok(())
}

/// Select a process-wide Rustls crypto backend before any TLS connection is
/// attempted. The dependency graph contains clients from multiple generations;
/// making the choice explicit avoids an ambiguous-provider startup panic.
pub fn install_tls_crypto_provider() -> Result<()> {
    if rustls::crypto::CryptoProvider::get_default().is_some() {
        return Ok(());
    }
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("install the Rustls ring crypto provider"))?;
    Ok(())
}

pub fn record_elapsed(metric_name: &'static str, started: std::time::Instant, venue: &str) {
    metrics::histogram!(metric_name, "venue" => venue.to_owned())
        .record(started.elapsed().as_secs_f64());
}

pub fn record_event_lag(metric_name: &'static str, exchange_timestamp_ms: u64, venue: &str) {
    let now = crate::types::unix_timestamp_ms();
    if exchange_timestamp_ms <= now {
        metrics::histogram!(metric_name, "venue" => venue.to_owned())
            .record((now - exchange_timestamp_ms) as f64 / 1_000.0);
    }
}

#[cfg(test)]
mod tests {
    use super::configured_prometheus_builder;

    #[test]
    fn latency_metrics_render_as_aggregatable_histogram_buckets() {
        let recorder = configured_prometheus_builder()
            .expect("valid histogram bounds")
            .build_recorder();
        let handle = recorder.handle();

        metrics::with_local_recorder(&recorder, || {
            metrics::histogram!("telemetry_render_test_latency_seconds").record(0.004);
        });

        let rendered = handle.render();
        assert!(rendered.contains("# TYPE telemetry_render_test_latency_seconds histogram"));
        assert!(rendered.contains("telemetry_render_test_latency_seconds_bucket{le=\"0.005\"} 1"));
        assert!(rendered.contains("telemetry_render_test_latency_seconds_bucket{le=\"+Inf\"} 1"));
    }
}
