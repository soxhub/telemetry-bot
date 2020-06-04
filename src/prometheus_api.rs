use chrono::prelude::*;
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RequestError {
    #[error("Prometheus Api: in {}: {}: {}", .path, .error_type, .error)]
    Failed {
        path: &'static str,
        error_type: String,
        error: String,
    },
}

impl RequestError {
    fn from_http(path: &'static str, err: surf::Error) -> Self {
        RequestError::Failed {
            path,
            error_type: "Http Error".into(),
            error: err.to_string(),
        }
    }
}

pub type Response = std::result::Result<Value, RequestError>;

fn extract(path: &'static str, mut value: Value) -> Response {
    if value["status"].as_str() == Some("success") {
        Ok(value["data"].take())
    } else {
        let error_type = value["error_type"].as_str().unwrap_or("unknown").into();
        let error = value["error"]
            .as_str()
            .unwrap_or("an error occurred without an error message")
            .into();
        Err(RequestError::Failed {
            path,
            error_type,
            error,
        })
    }
}

/// Return all labels defined in prometheus
pub async fn labels<'a>(prom_url: &'a str) -> Response {
    let path = "/api/v1/labels";
    let request_url = format!("{}{}", prom_url, path);
    let response = surf::get(request_url)
        .recv_json()
        .await
        .map_err(|err| RequestError::from_http(path, err))?;
    extract(path, response)
}

/// Returns all metrics defined in prometheus
pub async fn metadata<'a>(prom_url: &'a str) -> Response {
    let path = "/api/v1/metadata";
    let request_url = format!("{}{}", prom_url, path);
    let response = surf::get(request_url)
        .recv_json()
        .await
        .map_err(|err| RequestError::from_http(path, err))?;
    extract(path, response)
}

/// Evaluates a PromQL query over a range of time
pub async fn query_range<'a>(
    prom_url: &'a str,
    query: &'a str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    // e.g. '1m' for one minute
    timeout: &'static str,
) -> Response {
    let path = "/api/v1/query_range";
    let request_url = format!(
        "{}{}?query={}&start={}&end={}&timeout={}",
        prom_url,
        path,
        query,
        start.timestamp(),
        end.timestamp(),
        timeout,
    );
    let response = surf::get(request_url)
        .recv_json()
        .await
        .map_err(|err| RequestError::from_http(path, err))?;
    extract(path, response)
}
