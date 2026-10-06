use std::fs::{self, OpenOptions};
use std::future::pending;
use std::io::{self, BufWriter, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use tracing_subscriber::EnvFilter;

use wide_quoter::market_data::{
    Bbo, MarketDataClient, MarketDataEnvironment, MarketDataMessage, MarketDataSourceConfig,
};
use wide_quoter::types::unix_timestamp_ms;

const EVENT_BUFFER_CAPACITY: usize = 1_024;
const CAPTURE_FLUSH_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Parser)]
#[command(
    name = "md-tail",
    about = "Read, validate, and print normalized market data from live-md"
)]
struct Args {
    /// TCP address exposed by live-md.
    #[arg(long, default_value = "127.0.0.1:9101")]
    address: SocketAddr,

    /// Print protocol-v1 NDJSON instead of compact human-readable lines.
    #[arg(long)]
    json: bool,

    /// Also write validated protocol-v1 NDJSON to this create-new file.
    #[arg(long)]
    output: Option<PathBuf>,

    /// Suppress stdout; requires --output.
    #[arg(long)]
    quiet: bool,

    /// Stop cleanly after this many seconds; omit to run until a signal.
    #[arg(long)]
    duration_seconds: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.quiet && args.output.is_none() {
        return Err(anyhow!("--quiet requires --output"));
    }
    if args.duration_seconds == Some(0) {
        return Err(anyhow!("--duration-seconds must be positive"));
    }
    let log_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(log_filter)
        .with_target(false)
        .with_writer(io::stderr)
        .try_init()
        .map_err(|error| anyhow!("install tracing subscriber: {error}"))?;

    let source = MarketDataSourceConfig {
        name: "md-tail".to_owned(),
        address: args.address.to_string(),
        reconnect_delay_ms: 500,
        max_publisher_lag_ms: 500,
        max_exchange_lag_ms: 2_500,
        max_future_skew_ms: 1_000,
    };
    let (mut events, reader_task) = MarketDataClient::spawn(source, EVENT_BUFFER_CAPACITY);
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    let duration_shutdown = async {
        match args.duration_seconds {
            Some(seconds) => tokio::time::sleep(Duration::from_secs(seconds)).await,
            None => pending::<()>().await,
        }
    };
    tokio::pin!(duration_shutdown);
    let stdout = io::stdout();
    let mut output = stdout.lock();
    let mut capture = args
        .output
        .as_deref()
        .map(|path| -> Result<_> {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("create {}", parent.display()))?;
            }
            let file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(path)
                .with_context(|| format!("create {}", path.display()))?;
            Ok(BufWriter::with_capacity(1024 * 1024, file))
        })
        .transpose()?;
    let mut last_capture_flush = Instant::now();
    let mut frames = 0_u64;

    eprintln!(
        "tailing validated market data from {} (Ctrl-C to stop)",
        args.address
    );

    let outcome = loop {
        let bbo = tokio::select! {
            signal = &mut shutdown => {
                break signal;
            }
            _ = &mut duration_shutdown => {
                break Ok(());
            }
            event = events.recv() => match event {
                Some(event) => event.into_bbo(),
                None => break Ok(()),
            }
        };

        let ndjson = serde_json::to_string(&MarketDataMessage::Bbo(bbo.clone()))
            .context("serialize market-data frame")?;
        if let Some(capture) = capture.as_mut() {
            writeln!(capture, "{ndjson}").context("write market-data capture")?;
            if last_capture_flush.elapsed() >= CAPTURE_FLUSH_INTERVAL {
                capture.flush().context("flush market-data capture")?;
                last_capture_flush = Instant::now();
            }
        }
        frames = frames.saturating_add(1);

        if args.quiet {
            continue;
        }
        let line = if args.json {
            ndjson
        } else {
            format_human(&bbo)
        };
        if let Err(error) = writeln!(output, "{line}").and_then(|()| output.flush()) {
            if error.kind() == io::ErrorKind::BrokenPipe {
                break Ok(());
            }
            break Err(error).context("write market data to stdout");
        }
    };

    drop(events);
    reader_task.await.context("join market-data reader")?;
    if let Some(mut capture) = capture {
        capture.flush().context("flush market-data capture")?;
        capture
            .get_ref()
            .sync_all()
            .context("sync market-data capture")?;
    }
    eprintln!("validated_frames={frames}");
    outcome
}

fn format_human(bbo: &Bbo) -> String {
    let now_ms = unix_timestamp_ms();
    let publisher_age_ms = i128::from(now_ms) - i128::from(bbo.publisher_timestamp_ms);
    let exchange_age_ms = i128::from(now_ms) - i128::from(bbo.exchange_timestamp_ms);
    format!(
        "env={} venue={} symbol={} bid={} bid_qty={} ask={} ask_qty={} seq={} publisher_age_ms={} exchange_age_ms={} stream={}",
        environment_label(bbo.environment),
        bbo.venue,
        bbo.symbol,
        bbo.bid_price,
        bbo.bid_qty,
        bbo.ask_price,
        bbo.ask_qty,
        bbo.sequence,
        publisher_age_ms,
        exchange_age_ms,
        bbo.stream_id,
    )
}

#[cfg(unix)]
async fn shutdown_signal() -> Result<()> {
    use tokio::signal::unix::{signal, SignalKind};

    let mut terminate = signal(SignalKind::terminate()).context("listen for SIGTERM")?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result.context("listen for Ctrl-C"),
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() -> Result<()> {
    tokio::signal::ctrl_c().await.context("listen for Ctrl-C")
}

const fn environment_label(environment: MarketDataEnvironment) -> &'static str {
    match environment {
        MarketDataEnvironment::Live => "live",
        MarketDataEnvironment::Synthetic => "synthetic",
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use rust_decimal::Decimal;

    use super::*;
    use wide_quoter::market_data::{decode_market_data_frame, MARKET_DATA_SCHEMA_VERSION};
    use wide_quoter::types::Venue;

    fn sample_bbo() -> Bbo {
        let now = unix_timestamp_ms();
        Bbo {
            schema_version: MARKET_DATA_SCHEMA_VERSION,
            environment: MarketDataEnvironment::Live,
            venue: Venue::Binance,
            symbol: "BTCUSDT".to_owned(),
            bid_price: Decimal::from_str("100.1").expect("decimal"),
            bid_qty: Decimal::from_str("2.5").expect("decimal"),
            ask_price: Decimal::from_str("100.2").expect("decimal"),
            ask_qty: Decimal::from_str("3.5").expect("decimal"),
            exchange_timestamp_ms: now,
            publisher_timestamp_ms: now,
            sequence: 42,
            stream_id: "live-md-test".to_owned(),
        }
    }

    #[test]
    fn human_output_contains_the_complete_bbo_identity_and_prices() {
        let line = format_human(&sample_bbo());
        assert!(line.contains("env=live venue=binance symbol=BTCUSDT"));
        assert!(line.contains("bid=100.1 bid_qty=2.5 ask=100.2 ask_qty=3.5"));
        assert!(line.contains("seq=42"));
        assert!(line.contains("stream=live-md-test"));
    }

    #[test]
    fn json_output_round_trips_through_the_wire_decoder() {
        let expected = sample_bbo();
        let line = serde_json::to_string(&MarketDataMessage::Bbo(expected.clone()))
            .expect("serialize BBO");
        assert_eq!(
            decode_market_data_frame(&line).expect("decode BBO"),
            expected
        );
    }
}
