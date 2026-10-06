use anyhow::{ensure, Context, Result};
use clap::{Parser, Subcommand};
use pump_copy::{
    config::Config,
    decoder::Decoder,
    engine::Engine,
    model::{Batch, Reserve, Side, Trade, Venue, SOL},
    rpc::{atomic_json, load_batches, load_pools, Coverage, Rpc},
};
use serde_json::json;
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

#[derive(Parser)]
#[command(
    version,
    about = "Pump.fun / PumpSwap research and paper copy trading. No transaction signing or funded orders."
)]
struct Cli {
    #[arg(long, default_value = "config/default.toml", global = true)]
    config: PathBuf,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Compare predeclared entry rules independently at fixed 1.5x/2x targets.
    CompareEntryRules {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        quotes: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Analyze every alert independently and isolate entry/exit feed latency.
    AnalyzeSignals {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        quotes: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Diagnostic cost/delay grid on incomplete logs; not a verified backtest.
    ObservedSweep {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        quotes: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Compare observed logs with complete blocks to quantify missing swaps.
    VerifyStream {
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        archive: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Discover active wallets and observed whale flows; does not certify P&L.
    Discover {
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value_t = 2000)]
        max_pools: usize,
        #[arg(long, default_value_t = 0)]
        follow_seconds: u64,
        #[arg(long, default_value_t = 60)]
        interval_seconds: u64,
        #[arg(long, default_value = "SOLANA_RPC_URL")]
        rpc_env: String,
    },
    /// Acquire bounded complete transaction payloads for a candidate address.
    WalletHistory {
        #[arg(long)]
        wallet: String,
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        since_ms: u64,
        #[arg(long)]
        until_ms: u64,
        #[arg(long, default_value_t = 250)]
        max_transactions: usize,
        #[arg(long)]
        helius: bool,
        #[arg(long, default_value = "SOLANA_RPC_URL")]
        rpc_env: String,
    },
    /// Reconcile observed swap events against wallet token/SOL balance changes.
    AuditWallet {
        #[arg(long)]
        wallet: String,
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value = "SOLANA_RPC_URL")]
        rpc_env: String,
    },
    /// Offline token/SOL balance evidence from a saved full RPC transaction, without event guesses.
    BalanceReceipt {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        wallet: String,
        #[arg(long)]
        output: PathBuf,
    },
    /// Verify historical swap cash flows from executed transfers and full balances, offline.
    ReconcileTransaction {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Cache captured raw transactions for subsequent address-history audits.
    ImportHistory {
        #[arg(long)]
        archive: PathBuf,
        #[arg(long)]
        db: PathBuf,
    },
    /// Persist program logs with explicit commitment, arrival times and gaps.
    RecordStream {
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        status: PathBuf,
        #[arg(long, default_value = "SOLANA_WS_URL")]
        ws_env: String,
        #[arg(long, default_value_t = 3600)]
        seconds: u64,
        #[arg(long, default_value_t = 8_000_000_000)]
        max_bytes: u64,
        #[arg(long, default_value="finalized", value_parser=["confirmed","finalized"])]
        commitment: String,
    },
    /// Inspect persistent stream observations without network access.
    StreamStatus {
        #[arg(long)]
        db: PathBuf,
    },
    /// Validate strategy settings without network access.
    Validate,
    /// Capture every finalized produced block in an inclusive range; resume safely.
    Collect {
        #[arg(long)]
        start_slot: u64,
        #[arg(long)]
        end_slot: u64,
        #[arg(long)]
        archive: PathBuf,
        #[arg(long, default_value = "SOLANA_RPC_URL")]
        rpc_env: String,
        #[arg(long, default_value_t = 1)]
        max_transaction_version: u8,
    },
    /// Follow finalized blocks and print new flow/copy signals and simulated fills.
    Watch {
        #[arg(long)]
        archive: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        start_slot: Option<u64>,
        #[arg(long, default_value = "SOLANA_RPC_URL")]
        rpc_env: String,
        #[arg(long, default_value_t = 1)]
        max_transaction_version: u8,
        #[arg(long, default_value_t = 300)]
        seconds: u64,
    },
    /// Rank observed traders; include rejection reasons and unknown cost-basis flags.
    Rank {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Replay with chronological selection. Older data warms up wallet rankings.
    Backtest {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        trade_start_ms: Option<u64>,
    },
    /// Compare target/delay settings on the same dataset. Selection remains causal.
    Sweep {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, value_delimiter = ',', default_value = "15000,20000,30000,50000")]
        targets_bps: Vec<u32>,
        #[arg(long, value_delimiter = ',', default_value = "1000,5000,15000")]
        latencies_ms: Vec<u64>,
        #[arg(long)]
        trade_start_ms: Option<u64>,
    },
    /// Select a target on training data and evaluate it on a later holdout.
    WalkForward {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        train_end_ms: u64,
        #[arg(long)]
        test_end_ms: u64,
        #[arg(long, value_delimiter = ',', default_value = "15000,20000,30000,50000")]
        targets_bps: Vec<u32>,
    },
    /// Normalize saved RPC blocks with saved pool metadata, entirely offline.
    Normalize {
        #[arg(long)]
        archive: PathBuf,
    },
    /// Create clearly labeled synthetic data and a runnable example config.
    Demo {
        #[arg(long)]
        output: PathBuf,
    },
}
fn read_config(path: &Path) -> Result<Config> {
    let c: Config = toml::from_str(
        &fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?,
    )?;
    c.validate()?;
    Ok(c)
}
fn replay(c: Config, batches: &[Batch]) -> Result<Engine> {
    let mut e = Engine::new(c)?;
    for b in batches {
        e.process(b)?;
    }
    Ok(e)
}
fn save_report(e: &Engine, output: &Path) -> Result<()> {
    fs::create_dir_all(output)?;
    let r = e.report();
    atomic_json(&output.join("report.json"), &r)?;
    atomic_json(&output.join("wallets.json"), &r.wallets)?;
    atomic_json(&output.join("copy-performance.json"), &r.copy_performance)?;
    atomic_json(
        &output.join("copy-list.json"),
        &r.wallets.iter().filter(|w| w.eligible).collect::<Vec<_>>(),
    )?;
    write_jsonl(&output.join("signals.jsonl"), &r.signals)?;
    write_jsonl(&output.join("fills.jsonl"), &r.fills)?;
    let mut equity = fs::File::create(output.join("equity.csv"))?;
    writeln!(
        equity,
        "time_ms,cash_lamports,liquidation_equity_lamports,open_positions"
    )?;
    for row in &r.equity {
        writeln!(
            equity,
            "{},{},{},{}",
            row.time_ms, row.cash_lamports, row.liquidation_equity_lamports, row.open_positions
        )?;
    }
    println!(
        "{}",
        serde_json::to_string(
            &json!({"mode":r.mode,"trades":r.trades,"eligible_wallets":r.wallets.iter().filter(|w|w.eligible).count(),"closed_positions":r.closed_positions,"open_positions":r.open_positions.len(),"cash_sol":r.cash_lamports as f64/SOL as f64,"estimated_equity_sol":r.estimated_liquidation_equity_lamports as f64/SOL as f64,"zero_recovery_equity_sol":r.zero_recovery_equity_lamports as f64/SOL as f64,"output":output})
        )?
    );
    Ok(())
}
fn write_jsonl<T: serde::Serialize>(path: &Path, rows: &[T]) -> Result<()> {
    let mut f = fs::File::create(path)?;
    for row in rows {
        serde_json::to_writer(&mut f, row)?;
        writeln!(f)?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if let Command::Demo { output } = &cli.command {
        return demo(output);
    }
    let mut config = read_config(&cli.config)?;
    match cli.command {
        Command::CompareEntryRules {
            input,
            quotes,
            output,
        } => {
            pump_copy::observed::entry_rules::run(&input, &quotes, &output, &config.strategy)?;
        }
        Command::AnalyzeSignals {
            input,
            quotes,
            output,
        } => {
            pump_copy::observed::signal_study::run(&input, &quotes, &output, &config.strategy)?;
        }
        Command::ObservedSweep {
            input,
            quotes,
            output,
        } => {
            pump_copy::observed::sweep(&input, &quotes, &output, &config.strategy)?;
        }
        Command::VerifyStream {
            db,
            archive,
            output,
        } => atomic_json(&output, &pump_copy::quality::compare_stream(&db, &archive)?)?,
        Command::Discover {
            db,
            output,
            max_pools,
            follow_seconds,
            interval_seconds,
            rpc_env,
        } => {
            ensure!(
                interval_seconds >= 15,
                "discovery interval must be at least 15 seconds"
            );
            let rpc = Rpc::from_env(&rpc_env, 1)?;
            let started = Instant::now();
            loop {
                pump_copy::discovery::snapshot(&rpc, &db, &output, &config.flow, max_pools).await?;
                let remaining =
                    Duration::from_secs(follow_seconds).saturating_sub(started.elapsed());
                if remaining.is_zero() {
                    break;
                }
                tokio::select! {_=tokio::time::sleep(remaining.min(Duration::from_secs(interval_seconds)))=>{},_=tokio::signal::ctrl_c()=>break}
                if started.elapsed() >= Duration::from_secs(follow_seconds) {
                    break;
                }
            }
        }
        Command::WalletHistory {
            wallet,
            db,
            since_ms,
            until_ms,
            max_transactions,
            helius,
            rpc_env,
        } => {
            let rpc = Rpc::from_env(&rpc_env, 1)?;
            println!(
                "{}",
                serde_json::to_string_pretty(
                    &pump_copy::history::backfill(
                        &rpc,
                        &db,
                        &wallet,
                        since_ms,
                        until_ms,
                        max_transactions,
                        helius
                    )
                    .await?
                )?
            );
        }
        Command::AuditWallet {
            wallet,
            db,
            output,
            rpc_env,
        } => pump_copy::history::audit(&Rpc::from_env(&rpc_env, 1)?, &db, &wallet, &output).await?,
        Command::BalanceReceipt {
            input,
            wallet,
            output,
        } => {
            let value: serde_json::Value = serde_json::from_reader(fs::File::open(input)?)?;
            let tx = value.get("result").unwrap_or(&value);
            atomic_json(&output, &pump_copy::balances::receipt(tx, &wallet)?)?;
        }
        Command::ReconcileTransaction { input, output } => {
            let value: serde_json::Value = serde_json::from_reader(fs::File::open(input)?)?;
            atomic_json(
                &output,
                &pump_copy::reconcile::transaction(value.get("result").unwrap_or(&value))?,
            )?;
        }
        Command::ImportHistory { archive, db } => {
            println!("{}", pump_copy::history::import_archive(&db, &archive)?)
        }
        Command::RecordStream {
            db,
            status,
            ws_env,
            seconds,
            max_bytes,
            commitment,
        } => {
            pump_copy::stream::record_at(&ws_env, &db, seconds, max_bytes, &status, &commitment)
                .await?
        }
        Command::StreamStatus { db } => println!(
            "{}",
            serde_json::to_string_pretty(&pump_copy::stream::status(
                &pump_copy::stream::open_store(&db)?
            )?)?
        ),
        Command::Validate => println!(
            "Valid paper strategy: target {:.2}x; no stop loss or maximum holding period",
            config.strategy.target_multiple_bps as f64 / 10_000.0
        ),
        Command::Collect {
            start_slot,
            end_slot,
            archive,
            rpc_env,
            max_transaction_version,
        } => {
            ensure!(start_slot <= end_slot, "start must precede end");
            let rpc = Rpc::from_env(&rpc_env, max_transaction_version)?;
            ensure!(
                end_slot <= rpc.finalized_slot().await?,
                "end slot is not finalized"
            );
            let decoder = Decoder::default();
            let mut pools = load_pools(&archive)?;
            let mut coverage = existing_coverage(&archive, start_slot)?;
            ensure!(
                start_slot <= coverage.end_slot.saturating_add(1)
                    && end_slot.saturating_add(1) >= coverage.start_slot,
                "capture would leave an uncovered gap"
            );
            let mut cursor = start_slot;
            while cursor <= end_slot {
                let end = cursor.saturating_add(31).min(end_slot);
                let slots = rpc.slots(cursor, end).await?;
                for slot in &slots {
                    let b = rpc.capture(*slot, &archive, &decoder, &mut pools).await?;
                    eprintln!("slot {slot}: {} trades", b.trades.len());
                }
                coverage.start_slot = coverage.start_slot.min(cursor);
                coverage.end_slot = coverage.end_slot.max(end);
                coverage.produced_slots.extend(slots);
                coverage.produced_slots.sort_unstable();
                coverage.produced_slots.dedup();
                atomic_json(&archive.join("coverage.json"), &coverage)?;
                if end == u64::MAX {
                    break;
                }
                cursor = end + 1;
            }
        }
        Command::Watch {
            archive,
            output,
            start_slot,
            rpc_env,
            max_transaction_version,
            seconds,
        } => {
            let rpc = Rpc::from_env(&rpc_env, max_transaction_version)?;
            let decoder = Decoder::default();
            let mut pools = load_pools(&archive)?;
            let mut engine = Engine::new(config)?;
            let mut cursor = if archive.join("blocks").exists() {
                let history = load_batches(&archive)?;
                for b in &history {
                    engine.process(b)?;
                }
                let next = history.last().context("empty archive")?.slot + 1;
                ensure!(
                    start_slot.is_none_or(|s| s == next),
                    "start-slot conflicts with existing archive"
                );
                next
            } else {
                start_slot.unwrap_or(rpc.finalized_slot().await?)
            };
            let mut coverage = existing_coverage(&archive, cursor)?;
            let started = Instant::now();
            loop {
                if started.elapsed() >= Duration::from_secs(seconds) {
                    break;
                }
                let latest =
                    tokio::select! {r=rpc.finalized_slot()=>r?,_=tokio::signal::ctrl_c()=>break};
                if latest >= cursor {
                    let end = latest.min(cursor.saturating_add(31));
                    let slots = rpc.slots(cursor, end).await?;
                    for slot in &slots {
                        let b = rpc.capture(*slot, &archive, &decoder, &mut pools).await?;
                        let (old_signals, old_fills) = (engine.signals.len(), engine.fills.len());
                        engine.process(&b)?;
                        for s in &engine.signals[old_signals..] {
                            println!("{}", json!({"type":"signal","data":s}));
                        }
                        for f in &engine.fills[old_fills..] {
                            println!("{}", json!({"type":"paper_fill","data":f}));
                        }
                    }
                    coverage.end_slot = end;
                    coverage.produced_slots.extend(slots);
                    coverage.produced_slots.sort_unstable();
                    coverage.produced_slots.dedup();
                    atomic_json(&archive.join("coverage.json"), &coverage)?;
                    cursor = end + 1;
                    save_report(&engine, &output)?;
                }
                tokio::select! {_=tokio::time::sleep(Duration::from_secs(1))=>{},_=tokio::signal::ctrl_c()=>break}
            }
            save_report(&engine, &output)?;
        }
        Command::Rank { input, output } => {
            // Disable entries while sharing the exact same accounting/selection.
            config.strategy.trade_start_ms = u64::MAX;
            let engine = replay(config, &load_batches(&input)?)?;
            save_report(&engine, &output)?;
        }
        Command::Backtest {
            input,
            output,
            trade_start_ms,
        } => {
            if let Some(t) = trade_start_ms {
                config.strategy.trade_start_ms = t;
            }
            save_report(&replay(config, &load_batches(&input)?)?, &output)?;
        }
        Command::Sweep {
            input,
            output,
            targets_bps,
            latencies_ms,
            trade_start_ms,
        } => {
            ensure!(
                !targets_bps.is_empty() && !latencies_ms.is_empty(),
                "empty sweep"
            );
            if let Some(t) = trade_start_ms {
                config.strategy.trade_start_ms = t;
            }
            let batches = load_batches(&input)?;
            fs::create_dir_all(&output)?;
            let mut rows = vec![];
            for target in targets_bps {
                for latency in &latencies_ms {
                    let mut c = config.clone();
                    c.strategy.target_multiple_bps = target;
                    c.strategy.latency_ms = *latency;
                    let e = replay(c, &batches)?;
                    let r = e.report();
                    let name = format!("target-{target}-latency-{latency}");
                    save_report(&e, &output.join(&name))?;
                    rows.push(json!({"run":name,"target_bps":target,"latency_ms":latency,"closed_positions":r.closed_positions,"open_positions":r.open_positions.len(),"realized_pnl_lamports":r.realized_pnl_lamports,"estimated_equity_lamports":r.estimated_liquidation_equity_lamports,"zero_recovery_equity_lamports":r.zero_recovery_equity_lamports,"max_drawdown_bps":r.max_drawdown_bps}));
                }
            }
            atomic_json(&output.join("comparison.json"), &rows)?;
        }
        Command::WalkForward {
            input,
            output,
            train_end_ms,
            test_end_ms,
            targets_bps,
        } => {
            let result = pump_copy::research::walk_forward(
                &config,
                &load_batches(&input)?,
                train_end_ms,
                test_end_ms,
                &targets_bps,
            )?;
            save_report(&result.training, &output.join("training"))?;
            save_report(&result.holdout, &output.join("holdout"))?;
            atomic_json(
                &output.join("selection.json"),
                &json!({"train_end_ms":train_end_ms,"test_end_ms":test_end_ms,"selection_metric":"training cash after valuing every open position at zero","selected_config":result.selected,"training_candidates":result.candidates,"holdout_portfolio":"starts flat; rankings warm up on prior data"}),
            )?;
        }
        Command::Normalize { archive } => {
            let decoder = Decoder::default();
            let mut pools = load_pools(&archive)?;
            let mut paths: Vec<_> = fs::read_dir(archive.join("raw"))?
                .map(|p| Ok(p?.path()))
                .collect::<Result<_>>()?;
            paths.retain(|p| p.extension().is_some_and(|e| e == "json" || e == "gz"));
            paths.sort_by_key(|p| {
                p.file_name()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.split('.').next()?.parse::<u64>().ok())
            });
            for path in paths {
                let slot = path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .context("slot filename")?
                    .split('.')
                    .next()
                    .context("slot filename")?
                    .parse::<u64>()?;
                let block = if path.extension().is_some_and(|s| s == "gz") {
                    serde_json::from_reader(flate2::read::GzDecoder::new(fs::File::open(path)?))?
                } else {
                    serde_json::from_reader(fs::File::open(path)?)?
                };
                ensure!(
                    decoder.required_pools(&block, &mut pools)?.is_empty(),
                    "missing pool metadata; use collect to resolve"
                );
                let batch = decoder.block(slot, &block, &pools)?;
                atomic_json(&archive.join("blocks").join(format!("{slot}.json")), &batch)?;
            }
            atomic_json(&archive.join("pools.json"), &pools)?;
        }
        Command::Demo { .. } => unreachable!(),
    }
    Ok(())
}
fn existing_coverage(archive: &Path, start: u64) -> Result<Coverage> {
    let path = archive.join("coverage.json");
    if path.exists() {
        Ok(serde_json::from_reader(fs::File::open(path)?)?)
    } else {
        Ok(Coverage {
            start_slot: start,
            end_slot: start.saturating_sub(1),
            produced_slots: vec![],
            commitment: "finalized".into(),
        })
    }
}
fn demo(out: &Path) -> Result<()> {
    fs::create_dir_all(out)?;
    let mut c: Config = toml::from_str(include_str!("../config/default.toml"))?;
    c.selection.min_closed_positions = 2;
    c.selection.min_distinct_mints = 2;
    c.selection.min_history_ms = 0;
    c.selection.min_conservative_profit_lamports = SOL / 2;
    c.selection.max_profit_concentration_bps = 10_000;
    c.strategy.latency_ms = 1000;
    c.strategy.entry_mode = pump_copy::config::EntryMode::Copy;
    c.strategy.trade_start_ms = 1_700_000_010_000;
    fs::write(out.join("demo.toml"), toml::to_string_pretty(&c)?)?;
    let mut batches = vec![];
    let mut add = |mint: &str,
                   wallet: &str,
                   side: Side,
                   quote: u64,
                   base: u64,
                   reserve_quote: u64,
                   venue: Venue| {
        let i = batches.len() as u64;
        batches.push(Batch {
            schema_version: 1,
            slot: i + 1,
            block_time_ms: 1_700_000_000_000 + i * 2000,
            blockhash: format!("SYNTHETIC-{i}"),
            skipped_non_sol: 0,
            trades: vec![Trade {
                signature: format!("SYNTHETIC-{i}"),
                tx_index: 0,
                event_index: 0,
                mint: mint.into(),
                wallet: wallet.into(),
                pool: format!("SYNTHETIC-{mint}-{venue:?}"),
                venue,
                side,
                tokens: 1_000_000,
                quote_lamports: quote,
                network_fee_lamports: 5000,
                reserve: Reserve {
                    base,
                    quote: reserve_quote,
                    real_base: base,
                    real_quote: reserve_quote,
                    fee_bps: 100,
                },
            }],
        });
    };
    for mint in ["TRAIN_A", "TRAIN_B"] {
        add(
            mint,
            "SYNTHETIC_LEADER",
            Side::Buy,
            SOL,
            1_000_000_000,
            100 * SOL,
            Venue::PumpFun,
        );
        add(
            mint,
            "SYNTHETIC_LEADER",
            Side::Sell,
            2 * SOL,
            1_000_000_000,
            100 * SOL,
            Venue::PumpFun,
        );
    }
    add(
        "WAIT",
        "OTHER",
        Side::Buy,
        SOL,
        1_000_000_000,
        100 * SOL,
        Venue::PumpFun,
    );
    add(
        "WINNER",
        "SYNTHETIC_LEADER",
        Side::Buy,
        SOL,
        1_000_000_000,
        100 * SOL,
        Venue::PumpFun,
    );
    add(
        "WINNER",
        "OTHER",
        Side::Buy,
        SOL,
        1_000_000_000,
        100 * SOL,
        Venue::PumpFun,
    );
    add(
        "WINNER",
        "OTHER",
        Side::Buy,
        SOL,
        800_000_000,
        200 * SOL,
        Venue::PumpSwap,
    );
    add(
        "WINNER",
        "OTHER",
        Side::Buy,
        SOL,
        700_000_000,
        220 * SOL,
        Venue::PumpSwap,
    );
    add(
        "LOSER",
        "SYNTHETIC_LEADER",
        Side::Buy,
        SOL,
        1_000_000_000,
        100 * SOL,
        Venue::PumpFun,
    );
    add(
        "LOSER",
        "OTHER",
        Side::Buy,
        SOL,
        1_000_000_000,
        100 * SOL,
        Venue::PumpFun,
    );
    add(
        "LOSER",
        "OTHER",
        Side::Sell,
        SOL,
        2_000_000_000,
        2 * SOL,
        Venue::PumpFun,
    );
    write_jsonl(&out.join("synthetic.jsonl"), &batches)?;
    fs::write(out.join("SYNTHETIC_DATA.txt"),"Illustrative fixtures only. No measured market performance. Wallets and tokens are fictional.\n")?;
    println!("Synthetic demo written to {}", out.display());
    Ok(())
}
