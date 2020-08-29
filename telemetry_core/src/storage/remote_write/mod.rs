//! # Storage Backend: Remote Write
//!
//! This storage backend uses the Prometheus remote write protocol.
//!

use anyhow::{Context, Result};
use prost::Message;

use crate::parser::Sample;

const MILLIS_PER_SECOND: i64 = 1000;

/// Write a batch of samples to the remote write storage backend
pub async fn write_samples(
    write_url: &str,
    default_timestamp: i64,
    scraped_samples: Vec<Sample<'_>>,
    static_labels: &[(String, String)],
) -> Result<usize> {
    let out = scraped_samples.len();

    // Build WriteRequest from samples
    let mut timeseries = Vec::new();
    for sample in scraped_samples {
        let timestamp = sample.timestamp.unwrap_or(default_timestamp) * MILLIS_PER_SECOND;
        let value = sample.value.to_f64();
        let samples = vec![proto::Sample { timestamp, value }];
        let mut labels = Vec::with_capacity(1 + static_labels.len() + sample.labels.len());
        labels.push(proto::Label {
            name: "__name__".into(),
            value: sample.name.into(),
        });
        for (name, value) in static_labels {
            labels.push(proto::Label {
                name: name.into(),
                value: value.into(),
            });
        }
        for (name, value) in sample.labels {
            labels.push(proto::Label {
                name: name.into(),
                value: value.into(),
            });
        }
        timeseries.push(proto::TimeSeries { labels, samples });
    }
    let request = proto::WriteRequest { timeseries };

    // Serialize request
    let mut bytes: Vec<u8> = Vec::with_capacity(request.encoded_len());
    request
        .encode(&mut bytes)
        .context("failed to encode write request")?;

    // Compress request body
    let mut encoder = snap::raw::Encoder::new();
    let compressed = encoder
        .compress_vec(&bytes)
        .context("failed to compress write request body")?;

    // Send request
    surf::post(write_url)
        .body_bytes(compressed)
        .await
        .map_err(|err| anyhow::format_err!("error during remote write: {}", err))?;

    Ok(out)
}

/// The proto module defines the protobuf message structure.
///
/// The fields must be in the same order as the protobuf definition.
/// See the [prost](https://github.com/danburkert/prost] for more options.
pub mod proto {
    use prost::Message;

    #[derive(Clone, PartialEq, Message)]
    pub struct Label {
        #[prost(string)]
        pub name: String,
        #[prost(string)]
        pub value: String,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct Sample {
        #[prost(double)]
        pub value: f64,
        #[prost(int64)]
        pub timestamp: i64,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct TimeSeries {
        #[prost(message, repeated)]
        pub labels: Vec<Label>,
        #[prost(message, repeated)]
        pub samples: Vec<Sample>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct WriteRequest {
        #[prost(message, repeated)]
        pub timeseries: Vec<TimeSeries>,
    }
}
