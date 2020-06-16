use std::borrow::Cow;
use std::collections::HashMap;

use crate::{SampleValue, SeriesType};

/// A single measurement for a metric at a particular timestamp (or "now")
#[derive(Clone, Debug)]
pub struct Sample<'a> {
    pub name: &'a str,
    pub labels: Vec<(&'a str, Cow<'a, str>)>,
    pub value: SampleValue,
    pub timestamp: Option<i64>,
}

pub type MetricTypes<'a> = HashMap<Cow<'a, str>, SeriesType>;

/// A parser for Prometheus's text exposition format.
pub fn parse(input: &str) -> (MetricTypes, Vec<Sample>) {
    let mut values = Vec::new();
    let mut metrics = HashMap::new();
    let mut continue_help = false;
    for line in input.lines() {
        if continue_help {
            continue_help = line.ends_with('\\');
            continue;
        }

        // The line defines a metric's help text
        if line.starts_with("# HELP ") {
            continue_help = line.ends_with('\\');
            continue;
        }
        // The line defines a metric's type
        else if line.starts_with("# TYPE ") {
            let mut line_parts = line.split(' ').filter(|x| !x.trim().is_empty()).skip(2);
            let (name, type_str) = match (line_parts.next(), line_parts.next()) {
                (Some(name), Some(type_str)) => (name, type_str),
                _ => continue,
            };
            match type_str {
                "counter" => {
                    metrics.insert(name.into(), SeriesType::CounterInteger); // maybe not a good assumption; but 😬
                }
                "gauge" => {
                    metrics.insert(name.into(), SeriesType::Gauge);
                }
                "summary" => {
                    metrics.insert(name.into(), SeriesType::Counter);
                    metrics.insert(format!("{}_count", name).into(), SeriesType::CounterInteger);
                    metrics.insert(format!("{}_sum", name).into(), SeriesType::Counter);
                }
                "histogram" => {
                    metrics.insert(
                        format!("{}_bucket", name).into(),
                        SeriesType::CounterInteger,
                    );
                    metrics.insert(format!("{}_count", name).into(), SeriesType::CounterInteger);
                    metrics.insert(format!("{}_sum", name).into(), SeriesType::Counter);
                }
                _ => (),
            }
        }
        // The line is a comment or is empty
        else if line.starts_with('#') || line.trim().is_empty() {
        }
        // The line contains data
        else {
            let value = match parse_value(&metrics, line) {
                Some(value) => value,
                None => continue,
            };
            values.push(value);
        }
    }
    (metrics, values)
}

fn parse_value<'i>(metric_types: &MetricTypes, line: &'i str) -> Option<Sample<'i>> {
    // Split `metric_name{labels} value timestamp` into parts
    let (metric_str, value_str, unix_str) = if line.contains('}') {
        let mut reverse_parts = line.rsplitn(2, '}'); // use rsplit, because lables could contain escaped '}'
        let mut line_parts = reverse_parts.next()?.trim().splitn(2, ' ');
        let metric_str = reverse_parts.next()?;
        let value_str = line_parts.next()?;
        let unix_str = line_parts.next();
        (metric_str, value_str, unix_str)
    } else {
        let mut line_parts = line.split(' ').filter(|x| !x.is_empty());
        let metric_str = line_parts.next()?;
        let value_str = line_parts.next()?;
        let unix_str = line_parts.next();
        (metric_str, value_str, unix_str)
    };

    // Split `metric_name{label1="value1",label2="value2"}` into parts
    let mut metric_parts = metric_str.trim().splitn(2, '{');
    let name = metric_parts.next()?;
    let mut labels = Vec::new();
    if let Some(labels_str) = metric_parts.next() {
        let mut tail = labels_str;
        loop {
            // Split first label
            let mut parts = tail.splitn(2, '=');
            let head = parts.next().unwrap_or("");
            tail = parts.next().unwrap_or("");
            if head.is_empty() || tail.is_empty() {
                break;
            }

            // Parse the label
            let name = head;
            let (value, offset) = parse_label_value(tail);
            labels.push((name, value));

            // Advance to next label
            tail = &tail[offset..];
            if !tail.starts_with(',') {
                break;
            }
            tail = &tail[1..];
        }

        // Skip this metric if the labels couldn't be parsed
        if !tail.is_empty() {
            return None;
        }
    }

    let value = match metric_types.get(name)? {
        SeriesType::Counter | SeriesType::Gauge => SampleValue::F64(value_str.trim().parse().ok()?),
        SeriesType::CounterInteger | SeriesType::GaugeInteger => {
            SampleValue::I64(value_str.trim().parse().ok()?)
        }
    };

    let timestamp = match unix_str {
        Some(unix_str) => Some(unix_str.trim().parse().ok()?),
        None => None,
    };

    Some(Sample {
        name,
        labels,
        value,
        timestamp,
    })
}

fn parse_label_value(value: &str) -> (Cow<str>, usize) {
    if value.contains('\\') {
        let mut n = 1;
        let mut esc = String::new();
        let mut chars = value.chars().skip(1);
        while let Some(ch) = chars.next() {
            n += 1;
            match ch {
                '\\' => match chars.next() {
                    Some('n') => {
                        esc.push('\n');
                        n += 1;
                    }
                    Some(ch) => {
                        esc.push(ch);
                        n += 1;
                    }
                    None => (),
                },
                '"' => break,
                _ => esc.push(ch),
            }
        }
        (esc.into(), n)
    } else if value.is_empty() {
        ("".into(), 0)
    } else {
        let mut iter = value[1..].split('"');
        let substr = iter.next().unwrap_or("");
        (
            substr.into(),
            1 + substr.len() + if iter.next().is_some() { 1 } else { 0 },
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::SampleValue;

    #[test]
    fn parses_text_format() {
        let (_, values) = parse(
            r#"
# HELP http_requests_total The total number of HTTP requests.
# TYPE http_requests_total counter
http_requests_total{method="post",code="200"} 1027 1395066363000
http_requests_total{method="post",code="400"}    3 1395066363000
http_requests_total{method="post",code="2,00",content="text==true"}  24 1395066363000

# Escaping in label values:
# TYPE msdos_file_access_time_seconds gauge
msdos_file_access_time_seconds{path="C:\\DIR\\FILE.TXT",error="Cannot find file:\n\"FILE.TXT\""} 1.458255915e9

# Minimalistic line:
# TYPE metric_without_timestamp_and_labels gauge
metric_without_timestamp_and_labels 12.47

# A weird metric from before the epoch:
# HELP something_weird The help text can contain \
#      escaped \
#      newlines
something_weird{problem="division by zero"} +Inf -3982045
something_weird{problem="missing closing quote} 1

# A histogram, which has a pretty complex representation in the text format:
# HELP http_request_duration_seconds A histogram of the request duration.
# TYPE http_request_duration_seconds histogram
http_request_duration_seconds_bucket{le="0.05"} 24054
http_request_duration_seconds_bucket{le="0.1"} 33444
http_request_duration_seconds_bucket{le="0.2"} 100392
http_request_duration_seconds_bucket{le="0.5"} 129389
http_request_duration_seconds_bucket{le="1"} 133988
http_request_duration_seconds_bucket{le="+Inf"} 144320
http_request_duration_seconds_sum 53423
http_request_duration_seconds_count 144320

# Finally a summary, which has a complex representation, too:
# HELP rpc_duration_seconds A summary of the RPC duration in seconds.
# TYPE rpc_duration_seconds summary
rpc_duration_seconds{quantile="0.01"} 3102
rpc_duration_seconds{quantile="0.05"} 3272
rpc_duration_seconds{quantile="0.5"} 4773
rpc_duration_seconds{quantile="0.9"} 9001
rpc_duration_seconds{quantile="0.99"} 76656
rpc_duration_seconds_sum 1.7560473

# TYPE unsual_spacing counter
unsual_spacing{label="}"}     1     1395066363000
unsual_spacing     1     1395066363000

# TYPE escaped_counter counter
escaped_counter{label="}"} 1
        "#,
        );

        // It parses all valid measurements, ignoring invalid measurements
        assert_eq!(
            values.len(),
            0
            + 3 // http_requests_total
            + 1 // msdos_file_access_time_seconds
            + 1 // metric_without_timestamp_and_labels
            + 8 // http_request_duration_seconds
            + 6 // rpc_duration_seconds
            + 2 // unusual_spacing
            + 1 // escaped_counter
        );

        // It can parse label and number values
        assert_eq!(values[0].labels.len(), 2);
        assert_eq!(values[0].labels[1].0, "code");
        assert_eq!(values[0].labels[1].1, "200");
        assert!(matches!(values[0].value, SampleValue::I64(1027)));
        assert_eq!(values[1].labels[1].1, "400");
        assert!(matches!(values[1].value, SampleValue::I64(3)));

        // It can handle commas and equal signs in label values
        assert_eq!(values[2].labels.len(), 3);
        assert_eq!(values[2].labels[0].0, "method");
        assert_eq!(values[2].labels[0].1, "post");
        assert_eq!(values[2].labels[1].0, "code");
        assert_eq!(values[2].labels[1].1, "2,00");
        assert_eq!(values[2].labels[2].0, "content");
        assert_eq!(values[2].labels[2].1, "text==true");

        // It parses escaped characters in label values
        assert_eq!(values[3].labels[0].0, "path");
        assert_eq!(values[3].labels[0].1, "C:\\DIR\\FILE.TXT");
        assert_eq!(values[3].labels[1].0, "error");
        assert_eq!(values[3].labels[1].1, "Cannot find file:\n\"FILE.TXT\"");

        // It parses measurements with either zero or one labels
        assert_eq!(values[4].name, "metric_without_timestamp_and_labels");
        assert_eq!(values[4].labels.len(), 0);
        assert_eq!(values[5].name, "http_request_duration_seconds_bucket");
        assert_eq!(values[5].labels.len(), 1);

        // It handles unusual spacing between value and timestamp
        let unusual_spacing = &values[values.len() - 2];
        assert!(matches!(unusual_spacing.value, SampleValue::I64(1)));
        assert!(matches!(unusual_spacing.timestamp, Some(1395066363000)));

        // It handles '}' in label value
        let escaped_counter = &values[values.len() - 1];
        assert_eq!(escaped_counter.labels[0].1, "}");
    }
}
