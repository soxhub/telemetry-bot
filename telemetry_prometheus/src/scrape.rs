use anyhow::Result;
use arc_swap::ArcSwap;
use chrono::prelude::*;
use futures::stream::{StreamExt, TryStreamExt};
use indexmap::IndexMap; // hash table w/ fast iter preserving insertion order
use k8s_openapi::api::core::v1 as k8s;
use parking_lot::Mutex; // faster Mutex for non-contentious access
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use crate::debug::DEBUG;
use crate::error::debug_error;

pub struct ScrapeTarget {
    pub name: String,

    /// The url to be scraped.
    ///
    /// e.g. http://10.0.15.15:9025/metrics
    pub url: String,

    /// A static set of labels to append to scraped metrics
    pub labels: Vec<(&'static str, Option<String>)>,

    /// The time of the last scrape
    pub last_scrape: AtomicI64,
}

impl ScrapeTarget {
    pub fn new(name: String, url: String) -> Self {
        let timestamp = Utc::now().naive_utc().timestamp();
        ScrapeTarget {
            name,
            url,
            labels: Vec::new(),
            last_scrape: AtomicI64::new(timestamp),
        }
    }

    // Performs equality comparison using metadata, excluding tracking state (e.g. `last_scrape`)
    pub fn metadata_eq(&self, other: &Self) -> bool {
        self.name == other.name && self.url == other.url && self.labels == other.labels
    }
}

impl ScrapeTarget {
    fn from_pod(pod: k8s::Pod) -> Option<Self> {
        // Check if scraping is enabled for this pod
        let metadata = pod.metadata?;
        let annotations = metadata.annotations.unwrap_or_default();
        if !annotations.contains_key("telemetry.bot/scrape") {
            return None;
        }

        // Get the pod name
        let name = metadata.name?;

        // Determine the endpoint to scrape
        let host = pod.status?.pod_ip?;
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
            .unwrap_or("pod,instance,namespace")
            .split(',')
            .filter(|x| !x.is_empty())
            .collect::<Vec<_>>();
        let mut labels = Vec::with_capacity(collect.len());
        for label in collect {
            match label {
                "pod" => labels.push(("pod", Some(name.clone()))),
                "instance" => labels.push(("instance", Some(host.clone()))),
                "namespace" => labels.push(("namespace", metadata.namespace.clone())),
                _ => (),
            }
        }

        let initial_time = Utc::now().naive_utc().timestamp();
        Some(ScrapeTarget {
            name,
            url,
            labels,
            last_scrape: AtomicI64::new(initial_time),
        })
    }

    /// Update the `last_scrape` timestamp, and get the previous value
    pub fn bookmark(&self, timestamp: i64) -> i64 {
        self.last_scrape.swap(timestamp, Ordering::Relaxed)
    }

    /// Make a request to the scrape target and return the response
    pub async fn scrape(&self, timeout: std::time::Duration) -> Result<String> {
        async_std::future::timeout(timeout, async {
            surf::get(&self.url)
                .recv_string()
                .await
                .map_err(|err| anyhow::format_err!("failed to scrape: {}", err))
        })
        .await
        .map_err(|err| anyhow::format_err!("failed to scrape: {}", err))?
    }
}

pub struct ScrapeList {
    api: kube::Api<k8s::Pod>,

    /// A map of pod names to scrape targets; for bookkeeping.
    ///
    /// Only intended to be accessed by a single thread.
    map: Mutex<IndexMap<String, Arc<ScrapeTarget>>>,

    /// A list of scrape targets, that can be read from by
    /// multiple threads without acquiring a lock.
    list: ArcSwap<Vec<Arc<ScrapeTarget>>>,
}

impl ScrapeList {
    pub fn shared(client: kube::Client) -> Arc<Self> {
        let api = kube::Api::all(client);
        let map = Mutex::new(IndexMap::new());
        let list = ArcSwap::new(Arc::new(Vec::new()));
        Arc::new(Self { api, map, list })
    }

    pub fn len(&self) -> usize {
        self.list.load().len()
    }

    pub fn get(&self) -> Vec<Arc<ScrapeTarget>> {
        Vec::clone(&self.list.load())
    }

    pub fn update(&self, list: Vec<ScrapeTarget>) {
        let mut map_ptr = self.map.lock();
        let mut new_map = IndexMap::new();
        let mut new_list = Vec::new();
        for target in list {
            if let Some(prev) = map_ptr.get(&target.name) {
                if target.metadata_eq(&prev) {
                    let prev_scrape = prev.last_scrape.load(Ordering::SeqCst);
                    target.last_scrape.store(prev_scrape, Ordering::Relaxed);
                }
            }
            let ptr = Arc::new(target);
            new_map.insert(ptr.name.clone(), ptr.clone());
            new_list.push(ptr);
        }

        // Update ScrapeList stored data
        *map_ptr = new_map;
        self.list.store(Arc::new(new_list));

        // Release lock only after updating `list`.
        //
        // This would happen anway, but we do so explicitly to document the intended lock region.
        std::mem::drop(map_ptr);
    }

    pub async fn refresh(&self) -> Result<()> {
        // Get list of pods from prometheus
        let options = kube::api::ListParams::default()
            .timeout(15)
            .labels("telemetry=true");
        let pods = self.api.list(&options).await?;

        // Collect pods scrape configuration
        let mut targets = Vec::new();
        for pod in pods {
            if let Some(target) = ScrapeTarget::from_pod(pod) {
                targets.push(target);
            }
        }

        // Update the collection of targets
        self.update(targets);

        Ok(())
    }

    pub async fn watch(&self) -> Result<()> {
        use kube::api::WatchEvent;

        // Watch for changes to the set of pods
        let options = kube::api::ListParams::default()
            .timeout(15)
            .labels("telemetry=true");
        let informer = kube::runtime::Informer::new(self.api.clone()).params(options);

        // Poll events forever
        let mut events_stream = informer.poll().await?.boxed();
        loop {
            let event = match events_stream.try_next().await {
                Ok(Some(event)) => event,
                Ok(None) => continue,
                Err(err) => {
                    DEBUG.polling_failed();
                    debug_error(err.into());
                    continue;
                }
            };
            match event {
                // When a pod was added or modified, check if it should be present in the list
                WatchEvent::Added(pod) | WatchEvent::Modified(pod) => {
                    if let Some(target_name) =
                        pod.metadata.as_ref().and_then(|meta| meta.name.clone())
                    {
                        // If the pod is a scrape target add it to the scrape target list
                        if let Some(target) = ScrapeTarget::from_pod(pod) {
                            let target = Arc::new(target);
                            let mut map = self.map.lock();
                            match map.get(&target_name) {
                                // Do nothing if the metadata hasn't changed
                                Some(prev) if target.metadata_eq(prev) => (),
                                // Otherwise, update the internal map and list state
                                _ => {
                                    map.insert(target_name, target);
                                    self.list.store(Arc::new(map.values().cloned().collect()));
                                }
                            }
                        }
                        // Otherwise, if it was previously in the list, remove it
                        else {
                            let mut map = self.map.lock();
                            if map.contains_key(&target_name) {
                                map.remove(&target_name);
                                self.list.store(Arc::new(map.values().cloned().collect()));
                            }
                        }
                    }
                }
                // When a pod is removed, remove it from the list
                WatchEvent::Deleted(pod) => {
                    if let Some(target_name) =
                        pod.metadata.as_ref().and_then(|meta| meta.name.clone())
                    {
                        let mut map = self.map.lock();
                        if map.contains_key(&target_name) {
                            map.remove(&target_name);
                            self.list.store(Arc::new(map.values().cloned().collect()));
                        }
                    }
                }
                WatchEvent::Bookmark(_) => (),
                WatchEvent::Error(_err) => {
                    // panic!("WatchEvent error: {:?}", e);
                }
            }
        }
    }
}
