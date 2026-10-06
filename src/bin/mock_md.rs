use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use rust_decimal::Decimal;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

use wide_quoter::market_data::{
    Bbo, MarketDataEnvironment, MarketDataMessage, MARKET_DATA_SCHEMA_VERSION,
};
use wide_quoter::types::{unix_timestamp_ms, Venue};

#[derive(Debug, Parser)]
#[command(
    name = "mock-md",
    about = "Development-only normalized BBO publisher; never connects to an exchange"
)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:9101")]
    listen: String,
    #[arg(long, default_value = "100000")]
    mid: Decimal,
    #[arg(long, default_value_t = 100)]
    interval_ms: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let listener = TcpListener::bind(&args.listen)
        .await
        .with_context(|| format!("bind {}", args.listen))?;
    let args = Arc::new(args);
    eprintln!(
        "DEVELOPMENT MARKET DATA ONLY: publishing synthetic BBOs on {}",
        args.listen
    );

    loop {
        let (stream, peer) = listener.accept().await.context("accept consumer")?;
        let args = Arc::clone(&args);
        tokio::spawn(async move {
            if let Err(error) = serve(stream, args).await {
                eprintln!("mock market-data consumer {peer} disconnected: {error:#}");
            }
        });
    }
}

async fn serve(mut stream: TcpStream, args: Arc<Args>) -> Result<()> {
    let sources = [
        (Venue::Binance, "BTCUSDT", Decimal::ZERO),
        (Venue::Bybit, "BTCUSDT", Decimal::from_str("0.2")?),
        (Venue::Hyperliquid, "BTC", Decimal::from_str("-0.1")?),
        (Venue::Lighter, "BTC", Decimal::from_str("0.1")?),
    ];
    let half_width = Decimal::from_str("0.5")?;
    let quantity = Decimal::from_str("1.00000")?;
    let stream_id = format!("mock-{}", unix_timestamp_ms());
    let mut sequence = 0_u64;
    let mut timer = tokio::time::interval(Duration::from_millis(args.interval_ms.max(1)));

    loop {
        timer.tick().await;
        sequence = sequence.wrapping_add(1);
        let now = unix_timestamp_ms();
        for (venue, symbol, offset) in &sources {
            let mid = args.mid + *offset;
            let message = MarketDataMessage::Bbo(Bbo {
                schema_version: MARKET_DATA_SCHEMA_VERSION,
                environment: MarketDataEnvironment::Synthetic,
                venue: *venue,
                symbol: (*symbol).to_owned(),
                bid_price: mid - half_width,
                bid_qty: quantity,
                ask_price: mid + half_width,
                ask_qty: quantity,
                exchange_timestamp_ms: now,
                publisher_timestamp_ms: now,
                sequence,
                stream_id: stream_id.clone(),
            });
            let mut line = serde_json::to_vec(&message)?;
            line.push(b'\n');
            stream.write_all(&line).await.context("write BBO")?;
        }
    }
}
