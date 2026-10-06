//! Independent signal diagnostics. There is no shared portfolio or slot budget.
use super::*;
use std::collections::BTreeSet;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Feed {
    Recorded,
    FastEntry,
    FastExit,
    FastBoth,
}
impl Feed {
    fn name(self) -> &'static str {
        match self {
            Self::Recorded => "recorded",
            Self::FastEntry => "one_second_entry_only",
            Self::FastExit => "one_second_exit_and_confirmation_only",
            Self::FastBoth => "one_second_both",
        }
    }
}

fn prepare(input: &Input, quotes: &[Quote], alert: &Alert, feed: Feed) -> Result<(Dataset, u64)> {
    ensure!(
        quotes.iter().any(|q| q.received_ms == alert.observed_at_ms),
        "no recorded trigger for {}",
        alert.mint
    );
    // All information used by this fixed signal must already exist on chain.
    // Do not move a signal ahead of a later-chain event that arrived early.
    let known_chain_ms = quotes
        .iter()
        .filter(|q| q.received_ms <= alert.observed_at_ms)
        .map(|q| q.chain_ms)
        .max()
        .context("signal has no known quote")?;
    let mut alert = alert.clone();
    if matches!(feed, Feed::FastEntry | Feed::FastBoth) {
        alert.observed_at_ms = known_chain_ms + 1000;
    }
    let mut quotes = quotes.to_vec();
    if matches!(feed, Feed::FastExit | Feed::FastBoth) {
        for q in &mut quotes {
            q.received_ms = q.chain_ms + 1000;
        }
        quotes.retain(|q| q.received_ms <= input.end_ms);
    }
    Ok((
        Dataset::new(
            Input {
                end_ms: input.end_ms,
                coverage: input.coverage.clone(),
                alerts: vec![alert],
            },
            quotes,
        )?,
        known_chain_ms,
    ))
}

fn independent_settings(
    strategy: &Strategy,
    target: u32,
    delay: u64,
    haircut: u32,
    extra_costs: bool,
) -> Settings {
    let mut s = Settings::from_strategy(strategy, target, delay, haircut);
    // Each signal starts with its own fee reserve. The value is an internal
    // accounting convenience, not a bankroll or portfolio performance claim.
    s.initial_cash_lamports = 100 * crate::model::SOL;
    s.max_positions = usize::MAX;
    s.max_exposure_lamports = u64::MAX;
    if !extra_costs {
        s.network_fee_lamports = 0;
        s.entry_rent_lamports = 0;
        s.adverse_slippage_bps = 0;
    }
    s
}

fn path_summary(data: &Dataset, r: &ResultRow) -> Value {
    let Some(entry) = r
        .executions
        .iter()
        .find(|e| e.side == "buy" && e.outcome == "modeled_fill")
    else {
        return Value::Null;
    };
    let tokens = entry.tokens.unwrap();
    let cost = entry.position_cost_lamports as f64;
    let reserve = entry.conservative_reserve.as_ref().unwrap();
    let entry_spot = reserve.quote as f64 / reserve.base as f64;
    let future: Vec<_> = data
        .blocks
        .iter()
        .filter(|b| b.slot > entry.slot.unwrap())
        .collect();
    let peak_net = future
        .iter()
        .filter_map(|b| quote_sell(b, tokens, &r.settings))
        .map(|(net, _)| net as f64 / cost)
        .reduce(f64::max);
    let peak_spot = future
        .iter()
        .map(|b| {
            b.quotes
                .iter()
                .map(|q| (q.reserve.quote as f64 / q.reserve.base as f64) / entry_spot)
                .reduce(f64::min)
                .unwrap()
        })
        .reduce(f64::max);
    let mut horizons = Vec::new();
    for seconds in [30u64, 60, 300, 600] {
        let at = entry.execution_ms + seconds * 1000;
        let covered = data
            .input
            .coverage
            .iter()
            .any(|c| entry.execution_ms >= c.chain_start_ms && at + 5000 <= c.chain_end_ms);
        let b = future
            .iter()
            .find(|b| b.chain_ms >= at && b.chain_ms <= at + 5000);
        let net = if covered {
            b.and_then(|b| quote_sell(b, tokens, &r.settings))
                .map(|(n, _)| n as f64 / cost)
        } else {
            None
        };
        horizons.push(json!({"seconds_after_entry":seconds,"net_multiple":net,"within_recorded_session":covered}));
    }
    json!({"entry_execution_ms":entry.execution_ms,"entry_slot":entry.slot,
        "peak_observed_net_multiple":peak_net,"peak_observed_spot_multiple":peak_spot,
        "horizon_marks":horizons,
        "limitation":"Observed post-entry path, including quotes after a modeled exit. Maxima are hindsight diagnostics, never an exit rule. Missing horizon quotes remain null."})
}

pub fn run(input: &Path, quotes: &Path, output: &Path, strategy: &Strategy) -> Result<()> {
    let input: Input = serde_json::from_reader(File::open(input)?)?;
    let mut by_mint: BTreeMap<String, Vec<Quote>> = BTreeMap::new();
    for line in BufReader::new(GzDecoder::new(File::open(quotes)?)).lines() {
        let q: Quote = serde_json::from_str(&line?)?;
        by_mint.entry(q.mint.clone()).or_default().push(q);
    }
    let mut alerts = input.alerts.clone();
    alerts.sort_by_key(|a| (a.observed_at_ms, a.mint.clone()));
    let mut first = BTreeSet::new();
    let mut rows = Vec::new();
    let mut signal_metadata = Vec::new();
    for (signal_index, alert) in alerts.iter().enumerate() {
        let first_for_mint = first.insert(alert.mint.clone());
        let quotes = by_mint.get(&alert.mint).context("missing signal mint")?;
        let cohort = if alert.venue == Venue::PumpSwap {
            "post_graduation"
        } else if alert.whale_buy_slots.len() == 1 {
            "pre_same_slot"
        } else {
            "pre_spread_slots"
        };
        let trigger_chain = quotes
            .iter()
            .filter(|q| q.received_ms == alert.observed_at_ms)
            .map(|q| q.chain_ms)
            .max()
            .context("missing trigger")?;
        signal_metadata.push(
            json!({"signal_index":signal_index,"mint":alert.mint,"first_for_mint":first_for_mint,
            "cohort":cohort,"observed_ms":alert.observed_at_ms,"trigger_chain_ms":trigger_chain,
            "event_to_receipt_ms":alert.observed_at_ms-trigger_chain,
            "remaining_capture_seconds":(input.end_ms-alert.observed_at_ms)/1000}),
        );
        for feed in [
            Feed::Recorded,
            Feed::FastEntry,
            Feed::FastExit,
            Feed::FastBoth,
        ] {
            let (data, known_chain_ms) = prepare(&input, quotes, alert, feed)?;
            let cases: Vec<(u64, u32, bool)> = if feed == Feed::Recorded {
                vec![
                    (1000, 100, true),
                    (3000, 100, true),
                    (10000, 100, true),
                    (1000, 200, true),
                    (3000, 200, true),
                    (10000, 200, true),
                    (1000, 0, false),
                ]
            } else if feed == Feed::FastBoth {
                vec![(1000, 100, true), (1000, 0, false)]
            } else {
                vec![(1000, 100, true)]
            };
            for (delay, haircut, extra_costs) in cases {
                for target in [15000, 20000, 30000] {
                    let result = replay(
                        &data,
                        independent_settings(strategy, target, delay, haircut, extra_costs),
                    )?;
                    ensure!(
                        result.skipped_capacity == 0,
                        "independent study unexpectedly constrained"
                    );
                    let path = path_summary(&data, &result);
                    rows.push(json!({"signal_index":signal_index,"mint":alert.mint,"first_for_mint":first_for_mint,
                        "cohort":cohort,"feed_case":feed.name(),"cost_case":if extra_costs {"modeled_costs"}else{"dex_and_impact_only"},
                        "latest_known_chain_ms":known_chain_ms,"decision_ms":data.alerts[0].observed_at_ms,
                        "path":path,"result":result}));
                }
            }
        }
    }
    atomic_json(
        &output.join("all-signals.json"),
        &json!({
        "mode":"independent_incomplete_log_signal_study","signals":signal_metadata,
        "scenario_count":33,"rows":rows,"funded_ready":false,
        "assumptions":[
            "Every alert is evaluated in isolation; no shared cash, position cap, exposure cap or competition between signals. Repeated alerts are retained and tagged, not counted as independent coins.",
            "All variants preserve the original alert cohort. Faster-feed cases move the fixed signal to one second after its latest already-known chain event; they do not regenerate the entire whale detector or predict a real provider's latency.",
            "Fast-entry changes only signal arrival. Fast-exit changes quote arrival and the proxy for order confirmation. Fast-both changes both. Event timestamps have second resolution.",
            "Recorded timing grid: 1/3/10-second submission delay and 1%/2% adverse haircut per leg. Timing isolation fixes one-second submission delay and 1% per leg.",
            "DEX-and-impact-only cases remove extra haircuts, network fees and entry rent, but retain observed DEX fees and size-dependent impact. These are diagnostic ablations, not achievable cost promises.",
            "The underlying quote, gap, liquidity, target recheck, retry and hold-until-target rules remain. End-of-capture unresolved positions are censored observations, not demonstrated permanent losses.",
            "Unknown entries and stale marks are separately reported. Zero recovery is an explicit adverse valuation, not realized P&L. Complete capture and real execution are not certified.",
            "A 100-SOL internal fee reserve per independent observation prevents cash constraints; do not interpret it as a strategy bankroll or compute bankroll returns from it."
        ]}),
    )?;
    println!(
        "Analyzed {} alerts across {} distinct mints; {} independent evaluations",
        alerts.len(),
        first.len(),
        rows.len()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::tests::{data, quote, settings};
    use super::*;

    #[test]
    fn each_alert_has_its_own_position_and_cash() {
        let mut input = data(vec![], 40000).input;
        input.alerts.push(Alert {
            mint: "n".into(),
            observed_at_ms: 10000,
            venue: Venue::PumpSwap,
            whale_buy_slots: vec![1],
        });
        let mut entries = 0;
        for a in &input.alerts {
            let qs = vec![
                quote(&a.mint, 1000, 10000, 1),
                quote(&a.mint, 12000, 22000, 1),
            ];
            let (d, _) = prepare(&input, &qs, a, Feed::Recorded).unwrap();
            let mut s = settings();
            s.max_positions = 1;
            s.initial_cash_lamports = s.cost();
            let r = replay(&d, s).unwrap();
            assert_eq!(r.skipped_capacity, 0);
            entries += r.entries;
        }
        assert_eq!(entries, 2);
    }
    #[test]
    fn fast_signal_waits_for_all_originally_known_chain_events() {
        let input = data(vec![], 40000).input;
        let qs = vec![quote("m", 9000, 9500, 1), quote("m", 1000, 10000, 1)];
        let (d, known) = prepare(&input, &qs, &input.alerts[0], Feed::FastBoth).unwrap();
        assert_eq!(known, 9000);
        assert_eq!(d.alerts[0].observed_at_ms, 10000);
        assert!(d.blocks.iter().all(|b| b.observed_ms == b.chain_ms + 1000));
    }
}
