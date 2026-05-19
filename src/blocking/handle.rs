use crate::blocking::api::ClientApi;
use crate::blocking::client::Client;
use crate::blocking::database::Database;
use crate::blocking::primitives::{
    Event, GroupEvent, Lock, MaxConcurrentFlow, PriorityLock, ReadWriteLock, ReentrantLock,
    Semaphore, TokenBucketFlow, TreeLock,
};
use crate::blocking::replset::{IntoNodeList, ReplsetClient};
use crate::error::Result;
use crate::options::ClientOptions;

#[derive(Clone, Debug)]
pub enum ClientHandle {
    Single(Client),
    Replset(ReplsetClient),
}

impl ClientHandle {
    pub fn new<N: IntoNodeList>(nodes: N) -> Self {
        Self::with_options(nodes, ClientOptions::default())
    }

    pub fn with_options<N: IntoNodeList>(nodes: N, options: ClientOptions) -> Self {
        let nodes = nodes.into_nodes();
        if nodes.len() == 1 {
            Self::Single(Client::with_options(nodes[0].clone(), options))
        } else {
            Self::Replset(ReplsetClient::with_options(nodes, options))
        }
    }

    pub fn connect<N: IntoNodeList>(nodes: N) -> Result<Self> {
        let client = Self::new(nodes);
        client.open()?;
        Ok(client)
    }

    pub fn open(&self) -> Result<()> {
        <Self as ClientApi>::open(self)
    }

    pub fn close(&self) {
        <Self as ClientApi>::close(self);
    }

    pub fn ping(&self) -> Result<bool> {
        <Self as ClientApi>::ping(self)
    }

    pub fn select_database(&self, db_id: u8) -> Database {
        <Self as ClientApi>::select_database(self, db_id)
    }

    pub fn lock<K: AsRef<[u8]>>(&self, key: K, timeout: u16, expired: u16) -> Lock {
        <Self as ClientApi>::lock(self, key, timeout, expired)
    }

    pub fn event<K: AsRef<[u8]>>(
        &self,
        key: K,
        timeout: u16,
        expired: u16,
        default_set: bool,
    ) -> Event {
        <Self as ClientApi>::event(self, key, timeout, expired, default_set)
    }

    pub fn group_event<K: AsRef<[u8]>>(
        &self,
        key: K,
        client_id: u64,
        version_id: u64,
        timeout: u16,
        expired: u16,
    ) -> GroupEvent {
        <Self as ClientApi>::group_event(self, key, client_id, version_id, timeout, expired)
    }

    pub fn semaphore<K: AsRef<[u8]>>(
        &self,
        key: K,
        count: u16,
        timeout: u16,
        expired: u16,
    ) -> Semaphore {
        <Self as ClientApi>::semaphore(self, key, count, timeout, expired)
    }

    pub fn reentrant_lock<K: AsRef<[u8]>>(
        &self,
        key: K,
        timeout: u16,
        expired: u16,
    ) -> ReentrantLock {
        <Self as ClientApi>::reentrant_lock(self, key, timeout, expired)
    }

    pub fn read_write_lock<K: AsRef<[u8]>>(
        &self,
        key: K,
        timeout: u16,
        expired: u16,
    ) -> ReadWriteLock {
        <Self as ClientApi>::read_write_lock(self, key, timeout, expired)
    }

    pub fn priority_lock<K: AsRef<[u8]>>(
        &self,
        key: K,
        priority: u8,
        timeout: u16,
        expired: u16,
    ) -> PriorityLock {
        <Self as ClientApi>::priority_lock(self, key, priority, timeout, expired)
    }

    pub fn max_concurrent_flow<K: AsRef<[u8]>>(
        &self,
        key: K,
        max: u16,
        timeout: u16,
        expired: u16,
    ) -> MaxConcurrentFlow {
        <Self as ClientApi>::max_concurrent_flow(self, key, max, timeout, expired)
    }

    pub fn token_bucket_flow<K: AsRef<[u8]>>(
        &self,
        key: K,
        count: u16,
        timeout: u16,
        period: f64,
    ) -> TokenBucketFlow {
        <Self as ClientApi>::token_bucket_flow(self, key, count, timeout, period)
    }

    pub fn tree_lock<K: AsRef<[u8]>>(&self, key: K, timeout: u16, expired: u16) -> TreeLock {
        <Self as ClientApi>::tree_lock(self, key, timeout, expired)
    }
}

impl From<Client> for ClientHandle {
    fn from(client: Client) -> Self {
        Self::Single(client)
    }
}

impl From<ReplsetClient> for ClientHandle {
    fn from(client: ReplsetClient) -> Self {
        Self::Replset(client)
    }
}

impl ClientApi for ClientHandle {
    fn open(&self) -> Result<()> {
        match self {
            Self::Single(client) => client.open(),
            Self::Replset(client) => client.open(),
        }
    }

    fn close(&self) {
        match self {
            Self::Single(client) => client.close(),
            Self::Replset(client) => client.close(),
        }
    }

    fn ping(&self) -> Result<bool> {
        match self {
            Self::Single(client) => client.ping(),
            Self::Replset(client) => client.ping(),
        }
    }

    fn select_database(&self, db_id: u8) -> Database {
        match self {
            Self::Single(client) => client.select_database(db_id),
            Self::Replset(client) => client.select_database(db_id),
        }
    }
}
