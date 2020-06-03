use anyhow::Result;
use chrono::prelude::*;

/// Evaluates a PromQL query over a range of time
pub async fn query_range<'a>(
    prom_url: &'a str,
    query: &'a str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    // e.g. '1m' for one minute
    timeout: &'static str,
) -> Result<serde_json::Value> {
    let request_url = format!(
        "{}/api/v1/query_range?query={}&start={}&end={}&timeout={}",
        prom_url,
        query,
        start.timestamp(),
        end.timestamp(),
        timeout,
    );
    let response = surf::get(request_url)
        .recv_json()
        .await
        // Surf 1.0's error doesn't correctly impl std::error::Error, so convert it to an error string
        .map_err(|err| anyhow::anyhow!("in prometheus_api::query_range: {}", err))?;
    Ok(response)
}
