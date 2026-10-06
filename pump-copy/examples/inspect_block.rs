use pump_copy::{decoder::Decoder, rpc::load_pools};
use serde_json::{json, Value};
fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let archive = std::path::PathBuf::from(args.next().unwrap());
    let slot = args.next().unwrap();
    let f = std::fs::File::open(archive.join("raw").join(format!("{slot}.json.gz")))?;
    let block: Value = serde_json::from_reader(flate2::read::GzDecoder::new(f))?;
    let d = Decoder::default();
    let mut pools = load_pools(&archive)?;
    d.required_pools(&block, &mut pools)?;
    for (tx_index, tx) in block["transactions"].as_array().unwrap().iter().enumerate() {
        for (event_index, event) in d.transaction_events(tx)?.into_iter().enumerate() {
            let fields = event.fields.clone();
            let (t, _) = d.trade_from_event(
                event,
                tx["transaction"]["signatures"][0].as_str().unwrap(),
                tx_index as u32,
                event_index as u32,
                tx["meta"]["fee"].as_u64().unwrap(),
                &pools,
            )?;
            if let Some(t) = t {
                if t.tokens == 0 || t.quote_lamports == 0 || t.reserve.validate().is_err() {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&json!({"trade":t,"fields":fields}))?
                    );
                }
            }
        }
    }
    Ok(())
}
