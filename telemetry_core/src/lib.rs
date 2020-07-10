pub mod debug;
pub mod error;
pub mod parser;
pub mod scrape;

use std::str::FromStr;

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum SampleValue {
    F64(f64),
    I64(i64),
}

impl SampleValue {
    pub fn to_f64(&self) -> f64 {
        match *self {
            SampleValue::F64(val) => val,
            SampleValue::I64(val) => val as f64,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum SeriesType {
    Counter,
    CounterInteger,
    Gauge,
    GaugeInteger,
}

impl SeriesType {
    pub fn as_str(&self) -> &str {
        match self {
            SeriesType::Counter => "Counter",
            SeriesType::CounterInteger => "CounterInteger",
            SeriesType::Gauge => "Gauge",
            SeriesType::GaugeInteger => "GaugeInteger",
        }
    }
}

impl FromStr for SeriesType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Counter" => Ok(SeriesType::Counter),
            "CounterInteger" => Ok(SeriesType::CounterInteger),
            "Gauge" => Ok(SeriesType::Gauge),
            "GaugeInteger" => Ok(SeriesType::GaugeInteger),
            _ => Err(()),
        }
    }
}
