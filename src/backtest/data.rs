//! Strict, deterministic loading for the canonical research-bar CSV format.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use rust_decimal::Decimal;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer};
use thiserror::Error;

use super::types::{Bar, BarBatch, MarketId};
use crate::types::Venue;

const BAR_HEADERS: [&str; 15] = [
    "venue",
    "symbol",
    "open_time_ms",
    "close_time_ms",
    "available_at_ms",
    "open",
    "high",
    "low",
    "close",
    "volume_usd",
    "open_interest_usd",
    "market_cap_usd",
    "mark_price",
    "funding_rate",
    "quantity_step",
];

#[derive(Debug, Error)]
pub enum DataError {
    #[error("expected_interval_ms must be positive")]
    ZeroInterval,
    #[error("read historical CSV: {0}")]
    Csv(#[from] csv::Error),
    #[error("historical CSV has an invalid header; expected exactly: {expected}")]
    InvalidHeader { expected: String },
    #[error("historical CSV contains no bar rows")]
    Empty,
    #[error("invalid historical CSV row {row}: {reason}")]
    InvalidRow { row: u64, reason: String },
    #[error("duplicate bar for {market:?} at open_time_ms {open_time_ms} (row {row})")]
    DuplicateBar {
        market: MarketId,
        open_time_ms: u64,
        row: u64,
    },
    #[error(
        "batch availability is not strictly chronological: batch at {open_time_ms} is available at {available_at_ms}, previous batch was available at {previous_available_at_ms}"
    )]
    NonChronologicalAvailability {
        open_time_ms: u64,
        available_at_ms: u64,
        previous_available_at_ms: u64,
    },
    #[error(
        "non-contiguous batches: previous close_time_ms {previous_close_time_ms}, next open_time_ms {next_open_time_ms}"
    )]
    NonContiguousBatches {
        previous_close_time_ms: u64,
        next_open_time_ms: u64,
    },
}

#[derive(Debug, Deserialize)]
struct CsvBarRow {
    venue: Venue,
    symbol: String,
    open_time_ms: u64,
    close_time_ms: u64,
    available_at_ms: u64,
    #[serde(deserialize_with = "deserialize_decimal")]
    open: Decimal,
    #[serde(deserialize_with = "deserialize_decimal")]
    high: Decimal,
    #[serde(deserialize_with = "deserialize_decimal")]
    low: Decimal,
    #[serde(deserialize_with = "deserialize_decimal")]
    close: Decimal,
    #[serde(deserialize_with = "deserialize_optional_decimal")]
    volume_usd: Option<Decimal>,
    #[serde(deserialize_with = "deserialize_optional_decimal")]
    open_interest_usd: Option<Decimal>,
    #[serde(deserialize_with = "deserialize_optional_decimal")]
    market_cap_usd: Option<Decimal>,
    #[serde(deserialize_with = "deserialize_optional_decimal")]
    mark_price: Option<Decimal>,
    #[serde(deserialize_with = "deserialize_optional_decimal")]
    funding_rate: Option<Decimal>,
    #[serde(deserialize_with = "deserialize_optional_decimal")]
    quantity_step: Option<Decimal>,
}

/// Load, validate, and deterministically batch canonical historical bars.
///
/// The format deliberately requires an exact header. Every batch contains all
/// rows with the same half-open interval `[open_time_ms, close_time_ms)`, waits
/// until its slowest row is available, and orders markets by [`MarketId`].
pub fn load_bars_csv(
    path: impl AsRef<Path>,
    expected_interval_ms: u64,
    require_contiguous_batches: bool,
) -> Result<Vec<BarBatch>, DataError> {
    if expected_interval_ms == 0 {
        return Err(DataError::ZeroInterval);
    }

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(false)
        .trim(csv::Trim::None)
        .from_path(path)?;
    let headers = reader.headers()?;
    if headers.len() != BAR_HEADERS.len()
        || !headers
            .iter()
            .zip(BAR_HEADERS)
            .all(|(received, expected)| received == expected)
    {
        return Err(DataError::InvalidHeader {
            expected: BAR_HEADERS.join(","),
        });
    }

    let mut grouped = BTreeMap::<u64, Vec<Bar>>::new();
    let mut seen = BTreeSet::<(MarketId, u64)>::new();

    for (record_index, result) in reader.deserialize::<CsvBarRow>().enumerate() {
        // CSV record one is the header, so the first data record is line two.
        let row_number = record_index as u64 + 2;
        let row = result.map_err(|error| DataError::InvalidRow {
            row: row_number,
            reason: error.to_string(),
        })?;
        let bar = validate_row(row, row_number, expected_interval_ms)?;
        let key = (bar.market.clone(), bar.open_time_ms);
        if !seen.insert(key) {
            return Err(DataError::DuplicateBar {
                market: bar.market.clone(),
                open_time_ms: bar.open_time_ms,
                row: row_number,
            });
        }
        grouped.entry(bar.open_time_ms).or_default().push(bar);
    }

    if grouped.is_empty() {
        return Err(DataError::Empty);
    }

    let mut batches = Vec::with_capacity(grouped.len());
    let mut previous_close_time_ms = None;
    let mut previous_available_at_ms = None;

    for (open_time_ms, mut bars) in grouped {
        bars.sort_by(|left, right| left.market.cmp(&right.market));
        let close_time_ms = bars[0].close_time_ms;
        debug_assert!(bars.iter().all(|bar| bar.close_time_ms == close_time_ms));
        let available_at_ms = bars
            .iter()
            .map(|bar| bar.available_at_ms)
            .max()
            .expect("a grouped batch is nonempty");

        if let Some(previous) = previous_available_at_ms {
            if available_at_ms <= previous {
                return Err(DataError::NonChronologicalAvailability {
                    open_time_ms,
                    available_at_ms,
                    previous_available_at_ms: previous,
                });
            }
        }
        if require_contiguous_batches {
            if let Some(previous) = previous_close_time_ms {
                if open_time_ms != previous {
                    return Err(DataError::NonContiguousBatches {
                        previous_close_time_ms: previous,
                        next_open_time_ms: open_time_ms,
                    });
                }
            }
        }

        batches.push(BarBatch {
            index: batches.len(),
            open_time_ms,
            close_time_ms,
            available_at_ms,
            bars,
        });
        previous_close_time_ms = Some(close_time_ms);
        previous_available_at_ms = Some(available_at_ms);
    }

    Ok(batches)
}

fn validate_row(
    row: CsvBarRow,
    row_number: u64,
    expected_interval_ms: u64,
) -> Result<Bar, DataError> {
    let invalid = |reason: String| DataError::InvalidRow {
        row: row_number,
        reason,
    };

    if row.symbol.is_empty() || row.symbol.trim() != row.symbol {
        return Err(invalid(
            "symbol must be nonempty and have no surrounding whitespace".to_owned(),
        ));
    }
    let expected_close = row
        .open_time_ms
        .checked_add(expected_interval_ms)
        .ok_or_else(|| invalid("bar close timestamp overflows u64".to_owned()))?;
    if row.close_time_ms != expected_close {
        return Err(invalid(format!(
            "close_time_ms must equal open_time_ms + expected interval ({expected_close})"
        )));
    }
    if row.open_time_ms % expected_interval_ms != 0 || row.close_time_ms % expected_interval_ms != 0
    {
        return Err(invalid(format!(
            "bar timestamps must be aligned to {expected_interval_ms} ms"
        )));
    }
    if row.available_at_ms < row.close_time_ms {
        return Err(invalid(
            "available_at_ms must be at or after close_time_ms".to_owned(),
        ));
    }

    for (field, value) in [
        ("open", row.open),
        ("high", row.high),
        ("low", row.low),
        ("close", row.close),
    ] {
        if value <= Decimal::ZERO {
            return Err(invalid(format!("{field} must be positive")));
        }
    }
    if row.low > row.open
        || row.low > row.close
        || row.high < row.open
        || row.high < row.close
        || row.low > row.high
    {
        return Err(invalid(
            "OHLC values are inconsistent (low <= open/close <= high is required)".to_owned(),
        ));
    }

    for (field, value) in [
        ("volume_usd", row.volume_usd),
        ("open_interest_usd", row.open_interest_usd),
        ("market_cap_usd", row.market_cap_usd),
    ] {
        if value.is_some_and(|value| value < Decimal::ZERO) {
            return Err(invalid(format!("{field} must be nonnegative when present")));
        }
    }
    if row.mark_price.is_some_and(|value| value <= Decimal::ZERO) {
        return Err(invalid(
            "mark_price must be positive when present".to_owned(),
        ));
    }
    if row
        .quantity_step
        .is_some_and(|value| value <= Decimal::ZERO)
    {
        return Err(invalid(
            "quantity_step must be positive when present".to_owned(),
        ));
    }

    Ok(Bar {
        market: MarketId::new(row.venue, row.symbol),
        open_time_ms: row.open_time_ms,
        close_time_ms: row.close_time_ms,
        available_at_ms: row.available_at_ms,
        open: row.open,
        high: row.high,
        low: row.low,
        close: row.close,
        volume_usd: row.volume_usd,
        open_interest_usd: row.open_interest_usd,
        market_cap_usd: row.market_cap_usd,
        mark_price: row.mark_price,
        funding_rate: row.funding_rate,
        quantity_step: row.quantity_step,
    })
}

fn deserialize_decimal<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    parse_plain_decimal(&value).map_err(D::Error::custom)
}

fn deserialize_optional_decimal<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    if value.is_empty() {
        Ok(None)
    } else {
        parse_plain_decimal(&value)
            .map(Some)
            .map_err(D::Error::custom)
    }
}

fn parse_plain_decimal(value: &str) -> Result<Decimal, String> {
    if value.is_empty() || value.trim() != value {
        return Err("decimal must be a nonempty string without surrounding whitespace".to_owned());
    }
    let unsigned = value.strip_prefix('-').unwrap_or(value);
    if unsigned.is_empty() {
        return Err("decimal contains no digits".to_owned());
    }
    let mut parts = unsigned.split('.');
    let integer = parts.next().expect("split always yields one part");
    let fractional = parts.next();
    if parts.next().is_some()
        || integer.is_empty()
        || !integer.bytes().all(|byte| byte.is_ascii_digit())
        || fractional.is_some_and(|digits| {
            digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit())
        })
    {
        return Err(format!(
            "{value:?} is not a plain decimal string (scientific notation is not allowed)"
        ));
    }
    Decimal::from_str_exact(value).map_err(|error| format!("invalid decimal {value:?}: {error}"))
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;

    const HEADER: &str = "venue,symbol,open_time_ms,close_time_ms,available_at_ms,open,high,low,close,volume_usd,open_interest_usd,market_cap_usd,mark_price,funding_rate,quantity_step\n";

    fn csv_file(rows: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().expect("temporary CSV");
        file.write_all(HEADER.as_bytes()).expect("header");
        file.write_all(rows.as_bytes()).expect("rows");
        file.flush().expect("flush");
        file
    }

    #[test]
    fn loads_unsorted_multi_market_rows_into_deterministic_batches() {
        let file = csv_file(concat!(
            "hyperliquid,ETH,60000,120000,120003,20,22,19,21,,,,21.1,-0.0001,0.01\n",
            "hyperliquid,ETH,0,60000,60005,10,12,9,11,1000,5000,,,0.0001,0.01\n",
            "hyperliquid,BTC,0,60000,60010,100,102,99,101,2000,9000,100000,101,,0.001\n",
            "hyperliquid,BTC,60000,120000,120001,101,103,100,102,,,,,,0.001\n",
        ));

        let batches = load_bars_csv(file.path(), 60_000, true).expect("valid bars");
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].index, 0);
        assert_eq!(batches[0].available_at_ms, 60_010);
        assert_eq!(batches[0].bars[0].market.symbol, "BTC");
        assert_eq!(batches[0].bars[1].market.symbol, "ETH");
        assert_eq!(batches[1].index, 1);
        assert_eq!(batches[1].available_at_ms, 120_003);
        assert_eq!(batches[1].bars[0].volume_usd, None);
    }

    #[test]
    fn rejects_non_plain_decimal_and_future_bar_availability() {
        let scientific = csv_file("hyperliquid,BTC,0,60000,60000,1e2,102,99,101,,,,,,\n");
        assert!(matches!(
            load_bars_csv(scientific.path(), 60_000, false),
            Err(DataError::InvalidRow { .. })
        ));

        let early = csv_file("hyperliquid,BTC,0,60000,59999,100,102,99,101,,,,,,\n");
        assert!(matches!(
            load_bars_csv(early.path(), 60_000, false),
            Err(DataError::InvalidRow { .. })
        ));
    }

    #[test]
    fn rejects_duplicates_and_required_time_gaps() {
        let duplicate = csv_file(concat!(
            "hyperliquid,BTC,0,60000,60000,100,102,99,101,,,,,,\n",
            "hyperliquid,BTC,0,60000,60001,100,102,99,101,,,,,,\n",
        ));
        assert!(matches!(
            load_bars_csv(duplicate.path(), 60_000, false),
            Err(DataError::DuplicateBar { .. })
        ));

        let gap = csv_file(concat!(
            "hyperliquid,BTC,0,60000,60000,100,102,99,101,,,,,,\n",
            "hyperliquid,BTC,120000,180000,180000,101,103,100,102,,,,,,\n",
        ));
        assert!(matches!(
            load_bars_csv(gap.path(), 60_000, true),
            Err(DataError::NonContiguousBatches { .. })
        ));
        assert_eq!(
            load_bars_csv(gap.path(), 60_000, false)
                .expect("gaps allowed")
                .len(),
            2
        );
    }

    #[test]
    fn requires_the_exact_header_and_aligned_interval() {
        let mut bad_header = NamedTempFile::new().expect("temporary CSV");
        writeln!(bad_header, "symbol,venue").expect("header");
        assert!(matches!(
            load_bars_csv(bad_header.path(), 60_000, false),
            Err(DataError::InvalidHeader { .. })
        ));

        let unaligned = csv_file("hyperliquid,BTC,1,60001,60001,100,102,99,101,,,,,,\n");
        assert!(matches!(
            load_bars_csv(unaligned.path(), 60_000, false),
            Err(DataError::InvalidRow { .. })
        ));
    }
}
