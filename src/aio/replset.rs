use crate::aio::client::Client;
use crate::aio::database::Database;
use crate::aio::primitives::{
    Event, GroupEvent, Lock, MaxConcurrentFlow, PriorityLock, ReadWriteLock, ReentrantLock, Semaphore,
    TokenBucketFlow, TreeLock,
};
use crate::error::{Result, SlockError};
use crate::options::ClientOptions;

pub trait IntoNodeList {
    fn into_nodes(self) -> Vec<String>;
}

impl IntoNodeList for &str {
    fn into_nodes(self) -> Vec<String> {
        self.split(',')
            .map(str::trim)
            .filter(|node| !node.is_empty())
            .map(ToOwned::to_owned)
            .collect()
    }
}

impl IntoNodeList for String {
    fn into_nodes(self) -> Vec<String> {
        self.as_str().into_nodes()
    }
}

impl<const N: usize> IntoNodeList for [&str; N] {
    fn into_nodes(self) -> Vec<String> {
        self.into_iter().map(ToOwned::to_owned).collect()
    }
}

impl IntoNodeList for Vec<String> {
    fn into_nodes(self) -> Vec<String> {
        self
    }
}

#[derive(Clone, Debug)]
pub struct ReplsetClient {
    nodes: Vec<String>,
    options: ClientOptions,
    client: Client,
}

impl ReplsetClient {
    pub fn new<N: IntoNodeList>(nodes: N) -> Self {
        Self::with_options(nodes, ClientOptions::default())
    }

    pub fn with_options<N: IntoNodeList>(nodes: N, options: ClientOptions) -> Self {
        let nodes = nodes.into_nodes();
        let first = nodes.first().cloned().unwrap_or_default();
        Self {
            nodes,
            client: Client::with_options(first, options.clone()),
            options,
        }
    }

    pub async fn connect<N: IntoNodeList>(nodes: N) -> Result<Self> {
        let client = Self::new(nodes);
        client.open().await?;
        Ok(client)
    }

    pub fn nodes(&self) -> &[String] {
        &self.nodes
    }

    pub async fn open(&self) -> Result<()> {
        if self.nodes.is_empty() {
            return Err(SlockError::NotConnected);
        }
        self.client.open().await
    }

    pub async fn close(&self) {
        self.client.close().await;
    }

    pub async fn ping(&self) -> Result<bool> {
        self.client.ping().await
    }

    pub fn select_database(&self, db_id: u8) -> Database {
        self.client.select_database(db_id)
    }

    pub fn lock<K: AsRef<[u8]>>(&self, key: K, timeout: u16, expired: u16) -> Lock {
        self.client.lock(key, timeout, expired)
    }

    pub fn event<K: AsRef<[u8]>>(&self, key: K, timeout: u16, expired: u16, default_set: bool) -> Event {
        self.client.event(key, timeout, expired, default_set)
    }

    pub fn group_event<K: AsRef<[u8]>>(
        &self,
        key: K,
        client_id: u64,
        version_id: u64,
        timeout: u16,
        expired: u16,
    ) -> GroupEvent {
        self.client
            .group_event(key, client_id, version_id, timeout, expired)
    }

    pub fn semaphore<K: AsRef<[u8]>>(&self, key: K, count: u16, timeout: u16, expired: u16) -> Semaphore {
        self.client.semaphore(key, count, timeout, expired)
    }

    pub fn reentrant_lock<K: AsRef<[u8]>>(&self, key: K, timeout: u16, expired: u16) -> ReentrantLock {
        self.client.reentrant_lock(key, timeout, expired)
    }

    pub fn read_write_lock<K: AsRef<[u8]>>(&self, key: K, timeout: u16, expired: u16) -> ReadWriteLock {
        self.client.read_write_lock(key, timeout, expired)
    }

    pub fn priority_lock<K: AsRef<[u8]>>(
        &self,
        key: K,
        priority: u8,
        timeout: u16,
        expired: u16,
    ) -> PriorityLock {
        self.client.priority_lock(key, priority, timeout, expired)
    }

    pub fn max_concurrent_flow<K: AsRef<[u8]>>(
        &self,
        key: K,
        max: u16,
        timeout: u16,
        expired: u16,
    ) -> MaxConcurrentFlow {
        self.client.max_concurrent_flow(key, max, timeout, expired)
    }

    pub fn token_bucket_flow<K: AsRef<[u8]>>(
        &self,
        key: K,
        count: u16,
        timeout: u16,
        period: f64,
    ) -> TokenBucketFlow {
        self.client.token_bucket_flow(key, count, timeout, period)
    }

    pub fn tree_lock<K: AsRef<[u8]>>(&self, key: K, timeout: u16, expired: u16) -> TreeLock {
        self.client.tree_lock(key, timeout, expired)
    }

    pub fn options(&self) -> &ClientOptions {
        &self.options
    }
}
