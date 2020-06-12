use anyhow::Result;
use arc_swap::ArcSwap;
use k8s_openapi::api::core::v1 as k8s;
use std::convert::TryFrom;
use std::sync::Arc;

pub struct ScrapeTarget {
    /// The url to be scraped.
    ///
    /// e.g. http://10.0.15.15:9025/metrics
    pub url: String,

    /// A static set of labels to append to scraped metrics
    pub labels: Vec<(&'static str, Option<String>)>,
}

impl ScrapeTarget {
    pub async fn scrape(&self) -> Result<String> {
        async_std::future::timeout(crate::SCRAPE_TIMEOUT, async {
            surf::get(&self.url)
                .recv_string()
                .await
                .map_err(|err| anyhow::anyhow!("failed to scrape metrics: {}", err))
        })
        .await
        .map_err(|err| anyhow::anyhow!("failed to scrape metrics: {}", err))?
    }
}

pub struct ScrapeList {
    list: ArcSwap<Vec<Arc<ScrapeTarget>>>,
}

impl ScrapeList {
    pub fn shared() -> Arc<Self> {
        Arc::new(Self {
            list: ArcSwap::new(Arc::new(Vec::new())),
        })
    }

    pub fn get(&self) -> Vec<Arc<ScrapeTarget>> {
        Vec::clone(&self.list.load())
    }

    pub fn put(&self, list: Vec<Arc<ScrapeTarget>>) {
        self.list.store(Arc::new(list));
    }

    pub async fn update(&self) -> Result<()> {
        let targets = find_scrape_targets().await?;
        self.put(targets);
        Ok(())
    }
}

async fn find_scrape_targets() -> Result<Vec<Arc<ScrapeTarget>>> {
    // Get list of pods from prometheus
    let mut options = k8s_openapi::ListOptional::default();
    options.label_selector = Some("telemetry=true");
    options.timeout_seconds = Some(3 * 60);
    let (request, _) = k8s::Pod::list_pod_for_all_namespaces(options)?;
    let response: k8s_openapi::List<k8s::Pod> = execute(request).await?;

    // Collect pods scrape configuration
    let mut targets = Vec::new();
    for pod in response.items {
        // Check if scraping is enabled for this pod
        let metadata = match pod.metadata {
            Some(meta) => meta,
            None => continue,
        };
        let annotations = metadata.annotations.unwrap_or_default();
        if !annotations.contains_key("telemetry.bot/scrape") {
            continue;
        }

        // Determine the endpoint to scrape
        let host = match pod.status.and_then(|s| s.pod_ip) {
            Some(ip) => ip,
            None => continue,
        };
        let port = annotations
            .get("telemetry.bot/port")
            .and_then(|v| v.parse::<u16>().ok())
            .unwrap_or(80);
        let path = annotations
            .get("telemetry.bot/path")
            .map(|path| path.trim_start_matches('/'))
            .unwrap_or("metrics");
        let url = format!("http://{}:{}/{}", host, port, path);

        // Collect other labels to scrape
        let collect = annotations
            .get("telemetry.bot/metadata")
            .map(String::as_str)
            .unwrap_or("pod,namespace")
            .split(",")
            .filter(|x| !x.is_empty())
            .collect::<Vec<_>>();
        let mut labels = Vec::with_capacity(collect.len());
        for label in collect {
            match label {
                "pod" => labels.push(("pod", metadata.name.clone())),
                "namespace" => labels.push(("namespace", metadata.namespace.clone())),
                _ => (),
            }
        }

        // Accept the pod
        targets.push(Arc::new(ScrapeTarget { url, labels }));
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
