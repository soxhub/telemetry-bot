
pub enum SeriesType {
    Counter,
    Gauge,
}

pub struct Series {
    name: String,
    type_: SeriesType,
    labels: Vec<String>,
}
