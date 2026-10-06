use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use tokio::sync::broadcast;
use tokio::time::MissedTickBehavior;
use tracing_subscriber::EnvFilter;

use wide_quoter::config::GatewayConfig;
use wide_quoter::gateway::{HyperliquidGateway, LighterGateway, OrderGateway};
use wide_quoter::market_data::{MarketDataClient, MarketDataEnvironment};
use wide_quoter::order_manager::{
    DesiredOrder, OrderIntent, OrderKey, OrderManager, OrderManagerConfig, OrderManagerEvent,
};
use wide_quoter::quoter::{DesiredQuotes, QuoteChange, QuoteDecision, WideQuoter};
use wide_quoter::telemetry::{install_metrics_listener, install_tls_crypto_provider};
use wide_quoter::types::{Instrument, Side, TimeInForce};
use wide_quoter::AppConfig;

#[derive(Debug, Parser)]
#[command(
    name = "quoter",
    about = "Live-only, wide two-sided perpetual futures quoter"
)]
struct Args {
    /// TOML configuration file.
    #[arg(long)]
    config: PathBuf,

    /// Validate configuration without loading credentials or connecting.
    #[arg(long)]
    validate_only: bool,

    /// Required for any run that can send live-mainnet orders.
    #[arg(long)]
    acknowledge_live_trading: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config =
        AppConfig::load(&args.config).with_context(|| format!("load {}", args.config.display()))?;

    if args.validate_only {
        println!(
            "valid live configuration: instance={} venue={} instrument={}",
            config.instance_id,
            config.gateway.venue(),
            config.quoter.instrument.canonical_symbol
        );
        return Ok(());
    }
    if !args.acknowledge_live_trading {
        return Err(anyhow!(
            "refusing to connect: pass --acknowledge-live-trading to confirm this sends real mainnet orders"
        ));
    }

    let filter =
        EnvFilter::try_new(&config.telemetry.log_filter).context("parse telemetry.log_filter")?;
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .try_init()
        .map_err(|error| anyhow!("install tracing subscriber: {error}"))?;
    // `tracing-subscriber` bridges legacy `log` records. Tungstenite's TRACE
    // records include complete frames, and Lighter subscription frames carry
    // the private bearer token. Keep native `tracing` configurable while
    // preventing dependency TRACE payloads from ever reaching the subscriber.
    log::set_max_level(log::LevelFilter::Debug);
    install_tls_crypto_provider()?;
    install_metrics_listener(config.telemetry.metrics_socket_address()?)?;

    tracing::warn!(
        instance = %config.instance_id,
        venue = %config.gateway.venue(),
        instrument = %config.quoter.instrument.canonical_symbol,
        "LIVE MAINNET TRADING ENABLED"
    );

    match config.gateway.clone() {
        GatewayConfig::Hyperliquid { settings } => {
            let gateway = HyperliquidGateway::connect(settings)
                .await
                .context("connect Hyperliquid mainnet gateway")?;
            run(config, gateway).await
        }
        GatewayConfig::Lighter { settings } => {
            let gateway = LighterGateway::connect(settings)
                .await
                .context("connect Lighter mainnet gateway")?;
            run(config, gateway).await
        }
    }
}

async fn run<G: OrderGateway>(config: AppConfig, gateway: G) -> Result<()> {
    let gateway = Arc::new(gateway);
    let manager = OrderManager::with_config(
        Arc::clone(&gateway),
        OrderManagerConfig {
            command_queue_capacity: config.runtime.order_manager_command_capacity,
            event_capacity: config.runtime.order_manager_event_capacity,
            shutdown_timeout: config.shutdown_timeout(),
            halt_on_any_fill: true,
            fail_closed_on_execution_fault: true,
            ..OrderManagerConfig::default()
        },
    );
    let mut manager_events = manager.subscribe();
    let mut execution_ready = gateway.execution_ready();
    metrics::gauge!(
        "strategy_execution_ready",
        "venue" => config.gateway.venue().to_string()
    )
    .set(if execution_ready { 1.0 } else { 0.0 });
    if !execution_ready {
        tracing::warn!("private execution stream is not ready; quoting remains paused");
    }
    let (mut market_data, market_data_task) = MarketDataClient::spawn(
        config.market_data.clone(),
        config.runtime.market_data_channel_capacity,
    );
    let mut quoter = WideQuoter::new(config.quoter.clone())?;
    let mut revision = 0_u64;
    let mut force_requote = false;
    let mut timer = tokio::time::interval(std::time::Duration::from_millis(
        config.runtime.decision_interval_ms,
    ));
    timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    // Keep operational failures inside this block so every exit path below
    // still runs the cancel-and-confirm shutdown sequence.
    let loop_result: Result<()> = async {
        loop {
            tokio::select! {
                signal = &mut shutdown => {
                    signal?;
                    tracing::info!("shutdown signal received");
                    break Ok(());
                }
                event = market_data.recv() => {
                    let Some(event) = event else {
                        break Err(anyhow!("market-data client stopped unexpectedly"));
                    };
                    let (bbo, received_at) = event.into_parts();
                    if bbo.environment != MarketDataEnvironment::Live {
                        break Err(anyhow!(
                            "refusing synthetic market data from {}:{} while live trading",
                            bbo.venue,
                            bbo.symbol
                        ));
                    }
                    let decision_started = Instant::now();
                    metrics::histogram!(
                        "market_data_consumer_queue_latency_seconds",
                        "source" => config.market_data.name.clone(),
                        "venue" => bbo.venue.to_string(),
                        "symbol" => bbo.symbol.clone()
                    )
                    .record(received_at.elapsed().as_secs_f64());

                    if quoter.update_bbo(bbo, received_at) {
                        // Evaluate at processing time, rather than at socket
                        // ingress, so queue delay contributes to staleness.
                        let decision = quoter.evaluate(decision_started);
                        log_decision(&decision);
                        if execution_ready {
                            apply_decision(
                                &manager,
                                &config.instance_id,
                                &config.quoter.instrument,
                                &decision,
                                &mut revision,
                                force_requote,
                                decision_started,
                            ).await?;
                            force_requote = false;
                        }
                    }
                }
                _ = timer.tick() => {
                    let started = Instant::now();
                    let decision = quoter.evaluate(started);
                    if execution_ready {
                        apply_decision(
                            &manager,
                            &config.instance_id,
                            &config.quoter.instrument,
                            &decision,
                            &mut revision,
                            force_requote,
                            started,
                        ).await?;
                        force_requote = false;
                    }
                }
                event = manager_events.recv() => {
                    match event {
                        Ok(OrderManagerEvent::Fill { key, trade_id, price, quantity, cumulative_quantity, is_final }) => {
                            tracing::info!(
                                strategy = %key.strategy_id,
                                side = ?key.side,
                                %trade_id,
                                %price,
                                %quantity,
                                %cumulative_quantity,
                                is_final,
                                "fill"
                            );
                            metrics::counter!(
                                "strategy_fill_fail_stops_total",
                                "venue" => config.gateway.venue().to_string(),
                                "side" => format!("{:?}", key.side).to_lowercase()
                            ).increment(1);
                            break Err(anyhow!(
                                "fill received for {:?} quote (trade_id={trade_id}); inventory-aware continuation is not implemented",
                                key.side
                            ));
                        }
                        Ok(OrderManagerEvent::GatewayFailure { key, operation, error }) => {
                            tracing::error!(
                                strategy = %key.strategy_id,
                                side = ?key.side,
                                ?operation,
                                %error,
                                "gateway operation failed; stopping live strategy"
                            );
                            break Err(anyhow!(
                                "gateway {operation:?} failed for {:?} quote: {error}",
                                key.side
                            ));
                        }
                        Ok(OrderManagerEvent::Connectivity { connected, reason }) => {
                            execution_ready = connected;
                            force_requote = connected;
                            metrics::gauge!(
                                "strategy_execution_ready",
                                "venue" => config.gateway.venue().to_string()
                            ).set(if connected { 1.0 } else { 0.0 });
                            if connected {
                                tracing::info!("private execution stream ready");
                            } else {
                                tracing::error!(?reason, "private execution stream unavailable; withdrawing desired quotes and stopping live strategy");
                                apply_quotes(
                                    &manager,
                                    &config.instance_id,
                                    &config.quoter.instrument,
                                    None,
                                    &mut revision,
                                    Instant::now(),
                                ).await?;
                                break Err(anyhow!(
                                    "private execution stream became unavailable: {}",
                                    reason.unwrap_or_else(|| "unknown reason".to_owned())
                                ));
                            }
                        }
                        Ok(OrderManagerEvent::StateChanged(state)) => {
                            if let Some(error) = state.last_error.as_deref() {
                                tracing::error!(
                                    strategy = %state.key.strategy_id,
                                    side = ?state.key.side,
                                    revision = state.revision,
                                    state = ?state.state,
                                    client_order_id = ?state.client_order_id,
                                    %error,
                                    "order entered a blocked error state; stopping live strategy"
                                );
                                break Err(anyhow!(
                                    "{:?} quote entered a blocked error state at revision {}: {error}",
                                    state.key.side,
                                    state.revision
                                ));
                            }
                            tracing::debug!(
                                side = ?state.key.side,
                                revision = state.revision,
                                state = ?state.state,
                                client_order_id = ?state.client_order_id,
                                "order state changed"
                            );
                        }
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            tracing::error!(skipped, "strategy lagged order-manager events; stopping live strategy");
                            break Err(anyhow!(
                                "strategy lagged order-manager events by {skipped} messages"
                            ));
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            break Err(anyhow!("order manager stopped unexpectedly"));
                        }
                    }
                }
            }
        }
    }.await;

    drop(market_data);
    market_data_task.abort();
    let _ = market_data_task.await;

    tracing::info!("canceling owned live orders before exit");
    if let Err(error) = manager.shutdown().await {
        tracing::error!(%error, "order-manager shutdown did not confirm every cancellation; attempting gateway cancel-all");
        let fallback = gateway
            .cancel_all(config.quoter.instrument.clone())
            .await
            .context("fallback gateway cancel-all");
        if let Err(fallback_error) = fallback {
            return Err(anyhow!(
                "{error}; fallback cancellation also failed: {fallback_error:#}"
            ));
        }
        return Err(anyhow!(
            "{error}; fallback cancellation was submitted but terminal state was not confirmed"
        ));
    }

    loop_result
}

async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .context("install SIGTERM handler")?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.context("listen for Ctrl-C")?,
            _ = terminate.recv() => {}
        }
        Ok(())
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.context("listen for Ctrl-C")
    }
}

fn log_decision(decision: &QuoteDecision) {
    tracing::debug!(
        fair_value = ?decision.fair_value,
        change = ?decision.change,
        reason = ?decision.reason,
        "quote decision"
    );
}

async fn apply_decision<G: OrderGateway>(
    manager: &OrderManager<G>,
    strategy_id: &str,
    instrument: &Instrument,
    decision: &QuoteDecision,
    revision: &mut u64,
    force: bool,
    started: Instant,
) -> Result<()> {
    match decision.change {
        QuoteChange::None if !force => Ok(()),
        QuoteChange::Cancel => {
            apply_quotes(manager, strategy_id, instrument, None, revision, started).await
        }
        QuoteChange::Replace | QuoteChange::None => {
            let Some(quotes) = decision.desired.as_ref() else {
                return Ok(());
            };
            apply_quotes(
                manager,
                strategy_id,
                instrument,
                Some(quotes),
                revision,
                started,
            )
            .await
        }
    }
}

async fn apply_quotes<G: OrderGateway>(
    manager: &OrderManager<G>,
    strategy_id: &str,
    instrument: &Instrument,
    quotes: Option<&DesiredQuotes>,
    revision: &mut u64,
    started: Instant,
) -> Result<()> {
    *revision = revision
        .checked_add(1)
        .ok_or_else(|| anyhow!("strategy revision exhausted"))?;

    let desired = |side: Side| DesiredOrder {
        key: OrderKey {
            strategy_id: strategy_id.to_owned(),
            instrument: instrument.clone(),
            side,
        },
        revision: *revision,
        target: quotes.map(|quotes| {
            let level = match side {
                Side::Buy => &quotes.bid,
                Side::Sell => &quotes.ask,
            };
            OrderIntent {
                price: level.price,
                quantity: level.quantity,
                time_in_force: TimeInForce::PostOnly,
                reduce_only: false,
            }
        }),
    };

    tokio::try_join!(
        manager.set_desired(desired(Side::Buy)),
        manager.set_desired(desired(Side::Sell)),
    )
    .context("apply two-sided desired orders")?;

    metrics::histogram!(
        "strategy_to_oms_latency_seconds",
        "venue" => instrument.venue.to_string(),
        "symbol" => instrument.canonical_symbol.clone()
    )
    .record(started.elapsed().as_secs_f64());
    Ok(())
}
