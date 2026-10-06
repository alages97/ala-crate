//! Fixed-target, independent evaluation of predeclared entry-rule signals.
use super::*;

#[derive(Deserialize)]
struct Signal {
    rule: String,
    signal_id: String,
    source_wallet: Option<String>,
    #[serde(flatten)]
    alert: Alert,
}
#[derive(Deserialize)]
struct RuleInput {
    end_ms: u64,
    coverage: Vec<Coverage>,
    signals: Vec<Signal>,
    rule_status: Value,
    provenance: Value,
}
pub fn run(input: &Path, quotes: &Path, output: &Path, strategy: &Strategy) -> Result<()> {
    let input: RuleInput = serde_json::from_reader(File::open(input)?)?;
    let mut by_mint: BTreeMap<String, Vec<Quote>> = BTreeMap::new();
    for line in BufReader::new(GzDecoder::new(File::open(quotes)?)).lines() {
        let q: Quote = serde_json::from_str(&line?)?;
        by_mint.entry(q.mint.clone()).or_default().push(q);
    }
    let mut ids = std::collections::BTreeSet::new();
    let mut rows = Vec::new();
    for signal in input.signals {
        ensure!(ids.insert(signal.signal_id.clone()), "duplicate signal id");
        let data = Dataset::new(
            Input {
                end_ms: input.end_ms,
                coverage: input.coverage.clone(),
                alerts: vec![signal.alert.clone()],
            },
            by_mint.get(&signal.alert.mint).cloned().unwrap_or_default(),
        )?;
        for target in [15000, 20000] {
            for haircut in [100, 200] {
                let mut settings = Settings::from_strategy(strategy, target, 1000, haircut);
                settings.initial_cash_lamports = 100 * crate::model::SOL;
                settings.max_exposure_lamports = u64::MAX;
                settings.max_positions = usize::MAX;
                let result = replay(&data, settings)?;
                ensure!(
                    result.skipped_capacity == 0,
                    "unexpected cross-signal constraint"
                );
                rows.push(json!({"rule":signal.rule,"signal_id":signal.signal_id,"mint":signal.alert.mint,
                    "observed_at_ms":signal.alert.observed_at_ms,"source_wallet":signal.source_wallet,"result":result}));
            }
        }
    }
    let mut summaries = Vec::new();
    for rule in input
        .rule_status
        .as_object()
        .context("rule_status object")?
        .keys()
    {
        for target in [15000, 20000] {
            for haircut in [100, 200] {
                let selected: Vec<_> = rows
                    .iter()
                    .filter(|r| {
                        r["rule"] == *rule
                            && r["result"]["settings"]["target_bps"] == target
                            && r["result"]["settings"]["adverse_slippage_bps"] == haircut
                    })
                    .collect();
                let sum = |key: &str| {
                    selected
                        .iter()
                        .map(|r| r["result"][key].as_i64().unwrap())
                        .sum::<i64>()
                };
                let known: Vec<_> = selected
                    .iter()
                    .filter(|r| r["result"]["entries"] == 1)
                    .collect();
                let known_pnl: i64 = known
                    .iter()
                    .map(|r| r["result"]["zero_recovery_pnl_lamports"].as_i64().unwrap())
                    .sum();
                let known_marked: i64 = known
                    .iter()
                    .map(|r| {
                        r["result"]["estimated_marked_pnl_lamports"]
                            .as_i64()
                            .unwrap()
                    })
                    .sum();
                summaries.push(json!({"rule":rule,"status":input.rule_status[rule],"target_multiple":target as f64/10000.,"haircut_bps_per_leg":haircut,"signals":selected.len(),"known_entries":sum("entries"),"exits":sum("exits"),"unknown_entries":sum("unknown_entries"),"failed_buys":sum("failed_buys"),"skipped_capacity":sum("skipped_capacity"),"known_entry_zero_recovery_sol":known_pnl as f64/1e9,"known_entry_marked_sol":known_marked as f64/1e9,"all_entry_adverse_valuation_sol":sum("zero_recovery_pnl_lamports") as f64/1e9,"known_entry_zero_recovery_per_entry_sol":if known.is_empty(){None}else{Some(known_pnl as f64/1e9/known.len() as f64)},"stale_or_unknown_marks":sum("stale_or_unknown_open_marks"),"unobserved_sell_attempts":sum("unobserved_sell_attempts"),"failed_sell_attempts":sum("failed_sell_attempts")}));
            }
        }
    }
    atomic_json(&output.join("details.json"), &rows)?;
    atomic_json(
        &output.join("comparison.json"),
        &json!({"mode":"independent_entry_rule_diagnostic","provenance":input.provenance,"results":summaries,"funded_ready":false,"limitations":["Every signal is evaluated independently. Missing entry outcomes remain separate from known-entry results.","Only net 1.5x and 2x targets; one-second additional submission delay; observed feed lag retained; 1% and 2% adverse haircut per leg.","Unresolved holdings are censored at cutoff. Zero recovery is a valuation assumption, not permanent loss or forced liquidation.","A disabled audited-copy rule has no eligible signals; exploratory nominee-copy results do not qualify wallets.","Quote coverage, inclusion and fresh executable balances are not certified by this log diagnostic."]}),
    )?;
    println!("{}", serde_json::to_string_pretty(&summaries)?);
    Ok(())
}
