use anyhow::Result;
use k8s_openapi::api::core::v1 as k8s;
use std::convert::TryFrom;

use crate::scrape::ScrapeTarget;

pub async fn find_scrape_targets() -> Result<Vec<ScrapeTarget>> {
    // Get list of pods from prometheus
    let mut options = k8s_openapi::ListOptional::default();
    options.timeout_seconds = Some(3 * 60);
    let (request, _) = k8s::Pod::list_pod_for_all_namespaces(options)?;
    let response: k8s_openapi::List<k8s::Pod> = execute(request).await?;

    // Collect pods with the appropriate annotation
    let mut targets = Vec::new();
    for pod in response.items {
        let metadata = match pod.metadata {
            Some(meta) => meta,
            None => continue,
        };
        let annotations = metadata.annotations.unwrap_or_default();
        if !annotations.contains_key("prometheus.io/scrape") {
            continue;
        }
        let pod_name = metadata.name;
        let pod_namespace = metadata.namespace;
        let pod_port = annotations
            .get("prometheus.io/port")
            .and_then(|v| v.parse::<u16>().ok());
        let pod_ip = pod.status.and_then(|s| s.pod_ip);
        match (pod_name, pod_namespace, pod_port, pod_ip) {
            (Some(name), Some(namespace), Some(port), Some(ip)) => {
                let url = format!("http://{}:{}/metrics", port, ip);
                targets.push(ScrapeTarget {
                    name,
                    namespace,
                    url,
                });
            }
            _ => (),
        }
    }

    Ok(targets)
}

async fn execute<T>(request: http::Request<Vec<u8>>) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let (parts, body) = request.into_parts();
    let body = http_types::Body::from_bytes(body);
    let request = http::Request::from_parts(parts, body);
    let request = http_types::Request::try_from(request)?;
    let request = surf::Request::try_from(request)?;
    let response = request
        .recv_json()
        .await
        .map_err(|err| anyhow::anyhow!("failed to list pods: {}", err))?;
    Ok(response)
}
