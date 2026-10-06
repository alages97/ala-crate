use std::collections::VecDeque;

use rust_decimal::Decimal;
use thiserror::Error;

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum IndicatorError {
    #[error("indicator period must be positive")]
    ZeroPeriod,
}

/// Online exponential moving average seeded with a simple average.
///
/// The first value is emitted after `period` observations. Subsequent values
/// use `alpha = 2 / (period + 1)`. All arithmetic remains in `Decimal`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ema {
    period: usize,
    alpha: Decimal,
    observations: usize,
    seed_sum: Decimal,
    value: Option<Decimal>,
}

impl Ema {
    pub fn new(period: usize) -> Result<Self, IndicatorError> {
        if period == 0 {
            return Err(IndicatorError::ZeroPeriod);
        }

        Ok(Self {
            period,
            alpha: Decimal::TWO / Decimal::from((period + 1) as u64),
            observations: 0,
            seed_sum: Decimal::ZERO,
            value: None,
        })
    }

    pub fn update(&mut self, observation: Decimal) -> Option<Decimal> {
        self.observations += 1;

        let next = match self.value {
            Some(previous) => previous + self.alpha * (observation - previous),
            None => {
                self.seed_sum += observation;
                if self.observations < self.period {
                    return None;
                }
                self.seed_sum / Decimal::from(self.period as u64)
            }
        };

        self.value = Some(next);
        self.value
    }

    pub fn value(&self) -> Option<Decimal> {
        self.value
    }

    pub fn reset(&mut self) {
        self.observations = 0;
        self.seed_sum = Decimal::ZERO;
        self.value = None;
    }
}

/// Online Wilder relative-strength index.
///
/// A period of `n` requires `n + 1` closes because RSI is formed from price
/// changes. A flat seed/window is defined as neutral RSI 50.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WilderRsi {
    period: usize,
    previous_close: Option<Decimal>,
    changes: usize,
    seed_gain: Decimal,
    seed_loss: Decimal,
    average_gain: Option<Decimal>,
    average_loss: Option<Decimal>,
    value: Option<Decimal>,
}

impl WilderRsi {
    pub fn new(period: usize) -> Result<Self, IndicatorError> {
        if period == 0 {
            return Err(IndicatorError::ZeroPeriod);
        }

        Ok(Self {
            period,
            previous_close: None,
            changes: 0,
            seed_gain: Decimal::ZERO,
            seed_loss: Decimal::ZERO,
            average_gain: None,
            average_loss: None,
            value: None,
        })
    }

    pub fn update(&mut self, close: Decimal) -> Option<Decimal> {
        let previous = self.previous_close.replace(close)?;

        let change = close - previous;
        let gain = change.max(Decimal::ZERO);
        let loss = (-change).max(Decimal::ZERO);
        self.changes += 1;

        let (average_gain, average_loss) = match (self.average_gain, self.average_loss) {
            (Some(previous_gain), Some(previous_loss)) => {
                let divisor = Decimal::from(self.period as u64);
                let prior_weight = Decimal::from((self.period - 1) as u64);
                (
                    (previous_gain * prior_weight + gain) / divisor,
                    (previous_loss * prior_weight + loss) / divisor,
                )
            }
            _ => {
                self.seed_gain += gain;
                self.seed_loss += loss;
                if self.changes < self.period {
                    return None;
                }
                let divisor = Decimal::from(self.period as u64);
                (self.seed_gain / divisor, self.seed_loss / divisor)
            }
        };

        self.average_gain = Some(average_gain);
        self.average_loss = Some(average_loss);
        self.value = Some(rsi_from_averages(average_gain, average_loss));
        self.value
    }

    pub fn value(&self) -> Option<Decimal> {
        self.value
    }

    pub fn reset(&mut self) {
        self.previous_close = None;
        self.changes = 0;
        self.seed_gain = Decimal::ZERO;
        self.seed_loss = Decimal::ZERO;
        self.average_gain = None;
        self.average_loss = None;
        self.value = None;
    }
}

fn rsi_from_averages(average_gain: Decimal, average_loss: Decimal) -> Decimal {
    if average_loss.is_zero() {
        return if average_gain.is_zero() {
            Decimal::from(50)
        } else {
            Decimal::ONE_HUNDRED
        };
    }
    if average_gain.is_zero() {
        return Decimal::ZERO;
    }

    let relative_strength = average_gain / average_loss;
    Decimal::ONE_HUNDRED - Decimal::ONE_HUNDRED / (Decimal::ONE + relative_strength)
}

/// Simple return over an exact rolling number of observations.
///
/// For a lookback of `n`, the first result is available on observation
/// `n + 1` and equals `current / lagged - 1`. A zero lagged value yields no
/// result rather than dividing by zero.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollingReturn {
    lookback: usize,
    observations: VecDeque<Decimal>,
    value: Option<Decimal>,
}

impl RollingReturn {
    pub fn new(lookback: usize) -> Result<Self, IndicatorError> {
        if lookback == 0 {
            return Err(IndicatorError::ZeroPeriod);
        }

        Ok(Self {
            lookback,
            observations: VecDeque::with_capacity(lookback + 1),
            value: None,
        })
    }

    pub fn update(&mut self, observation: Decimal) -> Option<Decimal> {
        self.observations.push_back(observation);
        if self.observations.len() > self.lookback + 1 {
            self.observations.pop_front();
        }

        if self.observations.len() != self.lookback + 1 {
            self.value = None;
            return None;
        }

        let lagged = self.observations.front().copied().unwrap_or_default();
        self.value = if lagged.is_zero() {
            None
        } else {
            Some(observation / lagged - Decimal::ONE)
        };
        self.value
    }

    pub fn value(&self) -> Option<Decimal> {
        self.value
    }

    pub fn reset(&mut self) {
        self.observations.clear();
        self.value = None;
    }
}

/// Fixed-window arithmetic mean for point-in-time volume gating.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollingAverage {
    period: usize,
    observations: VecDeque<Decimal>,
    sum: Decimal,
    value: Option<Decimal>,
}

impl RollingAverage {
    pub fn new(period: usize) -> Result<Self, IndicatorError> {
        if period == 0 {
            return Err(IndicatorError::ZeroPeriod);
        }

        Ok(Self {
            period,
            observations: VecDeque::with_capacity(period),
            sum: Decimal::ZERO,
            value: None,
        })
    }

    pub fn update(&mut self, observation: Decimal) -> Option<Decimal> {
        self.observations.push_back(observation);
        self.sum += observation;

        if self.observations.len() > self.period {
            if let Some(expired) = self.observations.pop_front() {
                self.sum -= expired;
            }
        }

        self.value = (self.observations.len() == self.period)
            .then(|| self.sum / Decimal::from(self.period as u64));
        self.value
    }

    pub fn value(&self) -> Option<Decimal> {
        self.value
    }

    pub fn reset(&mut self) {
        self.observations.clear();
        self.sum = Decimal::ZERO;
        self.value = None;
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    fn d(value: &str) -> Decimal {
        Decimal::from_str(value).expect("valid test decimal")
    }

    #[test]
    fn indicators_reject_zero_periods() {
        assert_eq!(Ema::new(0), Err(IndicatorError::ZeroPeriod));
        assert_eq!(WilderRsi::new(0), Err(IndicatorError::ZeroPeriod));
        assert_eq!(RollingReturn::new(0), Err(IndicatorError::ZeroPeriod));
        assert_eq!(RollingAverage::new(0), Err(IndicatorError::ZeroPeriod));
    }

    #[test]
    fn ema_seeds_with_sma_then_updates_online() {
        let mut ema = Ema::new(3).unwrap();
        assert_eq!(ema.update(d("1")), None);
        assert_eq!(ema.update(d("2")), None);
        assert_eq!(ema.update(d("3")), Some(d("2")));
        assert_eq!(ema.update(d("4")), Some(d("3")));
        assert_eq!(ema.update(d("5")), Some(d("4")));
        assert_eq!(ema.value(), Some(d("4")));
    }

    #[test]
    fn ema_period_one_tracks_the_latest_observation() {
        let mut ema = Ema::new(1).unwrap();
        assert_eq!(ema.update(d("7")), Some(d("7")));
        assert_eq!(ema.update(d("11")), Some(d("11")));
    }

    #[test]
    fn wilder_rsi_has_explicit_up_down_and_flat_behavior() {
        let mut up = WilderRsi::new(3).unwrap();
        let mut down = WilderRsi::new(3).unwrap();
        let mut flat = WilderRsi::new(3).unwrap();

        for value in ["1", "2", "3"] {
            assert_eq!(up.update(d(value)), None);
        }
        assert_eq!(up.update(d("4")), Some(d("100")));

        for value in ["4", "3", "2"] {
            assert_eq!(down.update(d(value)), None);
        }
        assert_eq!(down.update(d("1")), Some(d("0")));

        for _ in 0..3 {
            assert_eq!(flat.update(d("9")), None);
        }
        assert_eq!(flat.update(d("9")), Some(d("50")));
    }

    #[test]
    fn wilder_rsi_uses_wilders_recursive_smoothing() {
        let mut rsi = WilderRsi::new(3).unwrap();
        for value in ["10", "12", "11"] {
            assert_eq!(rsi.update(d(value)), None);
        }
        assert_eq!(
            rsi.update(d("14")),
            Some(d("83.33333333333333333333333334"))
        );

        // Prior average gain = 5/3, prior average loss = 1/3. The next
        // one-point loss gives smoothed averages 10/9 and 5/9, hence RSI 200/3.
        assert_eq!(
            rsi.update(d("13")),
            Some(d("66.666666666666666666666666669"))
        );
    }

    #[test]
    fn rolling_return_uses_exact_lag_and_rolls_forward() {
        let mut returns = RollingReturn::new(2).unwrap();
        assert_eq!(returns.update(d("100")), None);
        assert_eq!(returns.update(d("110")), None);
        assert_eq!(returns.update(d("121")), Some(d("0.21")));
        assert_eq!(returns.update(d("133.1")), Some(d("0.21")));
        assert_eq!(returns.value(), Some(d("0.21")));
    }

    #[test]
    fn rolling_return_does_not_divide_by_zero() {
        let mut returns = RollingReturn::new(1).unwrap();
        assert_eq!(returns.update(Decimal::ZERO), None);
        assert_eq!(returns.update(Decimal::ONE), None);
        assert_eq!(returns.update(d("2")), Some(Decimal::ONE));
    }

    #[test]
    fn rolling_average_requires_full_window_and_expires_old_values() {
        let mut average = RollingAverage::new(3).unwrap();
        assert_eq!(average.update(d("1")), None);
        assert_eq!(average.update(d("2")), None);
        assert_eq!(average.update(d("3")), Some(d("2")));
        assert_eq!(average.update(d("7")), Some(d("4")));
    }

    #[test]
    fn reset_removes_all_warmup_state() {
        let mut ema = Ema::new(2).unwrap();
        assert_eq!(ema.update(d("2")), None);
        assert_eq!(ema.update(d("4")), Some(d("3")));
        ema.reset();
        assert_eq!(ema.value(), None);
        assert_eq!(ema.update(d("10")), None);

        let mut rsi = WilderRsi::new(1).unwrap();
        assert_eq!(rsi.update(d("1")), None);
        assert_eq!(rsi.update(d("2")), Some(d("100")));
        rsi.reset();
        assert_eq!(rsi.value(), None);
        assert_eq!(rsi.update(d("5")), None);

        let mut returns = RollingReturn::new(1).unwrap();
        returns.update(d("1"));
        assert!(returns.update(d("2")).is_some());
        returns.reset();
        assert_eq!(returns.update(d("3")), None);

        let mut average = RollingAverage::new(2).unwrap();
        average.update(d("1"));
        assert!(average.update(d("3")).is_some());
        average.reset();
        assert_eq!(average.update(d("9")), None);
    }
}
