use crate::aio::api::ClientApi;
use crate::aio::client::Client;
use crate::aio::database::Database;
use crate::aio::primitives::{
    Event, GroupEvent, Lock, MaxConcurrentFlow, PriorityLock, ReadWriteLock, ReentrantLock,
    Semaphore, TokenBucketFlow, TreeLock,
};
use crate::aio::raw::RawCommand;
use crate::aio::replset::{IntoNodeList, ReplsetClient};
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

    pub async fn connect<N: IntoNodeList>(nodes: N) -> Result<Self> {
        let client = Self::new(nodes);
        client.open().await?;
        Ok(client)
    }

    pub async fn open(&self) -> Result<()> {
        <Self as ClientApi>::open(self).await
    }

    /// Opens without turning the first refusal into an error.
    ///
    /// Delegates to [`Client::try_open`] for a single node and to
    /// [`ReplsetClient::try_open`] for a replica set, so bindings can use one
    /// lifecycle entry point for both shapes.
    pub async fn try_open(&self) -> Result<bool> {
        match self {
            Self::Single(client) => client.try_open().await,
            Self::Replset(client) => client.try_open().await,
        }
    }

    pub async fn close(&self) {
        <Self as ClientApi>::close(self).await;
    }

    pub async fn ping(&self) -> Result<bool> {
        <Self as ClientApi>::ping(self).await
    }

    pub fn select_database(&self, db_id: u8) -> Database {
        <Self as ClientApi>::select_database(self, db_id)
    }

    /// The routed member index. A single node always routes to index `0`.
    pub fn active_index(&self) -> Option<usize> {
        match self {
            Self::Single(_) => Some(0),
            Self::Replset(client) => client.active_index(),
        }
    }

    /// Init flags of the routed member, or `0` when no member is selected.
    pub fn active_init_type(&self) -> u8 {
        match self {
            Self::Single(client) => client.init_type(),
            Self::Replset(client) => client.active_init_type(),
        }
    }

    /// Whether the routed member advertised `INIT_TYPE_FLAG_IS_LEADER`.
    pub fn is_leader(&self) -> bool {
        (self.active_init_type() & crate::protocol::constants::INIT_TYPE_FLAG_IS_LEADER) != 0
    }

    /// Opts a replica-set handle into conservative typed replay safety.
    ///
    /// This is a no-op for a single-node handle. See
    /// [`ReplsetClient::with_conservative_replay`] for the default policy.
    pub fn with_conservative_replay(self) -> Self {
        match self {
            Self::Single(client) => Self::Single(client),
            Self::Replset(client) => Self::Replset(client.with_conservative_replay()),
        }
    }

    /// Sends an already-validated raw command through the routed member.
    pub async fn send_raw(&self, command: RawCommand) -> Result<Vec<u8>> {
        match self {
            Self::Single(client) => client.send_raw(command).await,
            Self::Replset(client) => client.send_raw(command).await,
        }
    }

    /// Writes an already-validated raw command without waiting for a response.
    pub async fn write_raw(&self, command: RawCommand) -> Result<()> {
        match self {
            Self::Single(client) => client.write_raw(command).await,
            Self::Replset(client) => client.write_raw(command).await,
        }
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
    fn open(&self) -> crate::aio::api::BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            match self {
                Self::Single(client) => client.open().await,
                Self::Replset(client) => client.open().await,
            }
        })
    }

    fn close(&self) -> crate::aio::api::BoxFuture<'_, ()> {
        Box::pin(async move {
            match self {
                Self::Single(client) => client.close().await,
                Self::Replset(client) => client.close().await,
            }
        })
    }

    fn ping(&self) -> crate::aio::api::BoxFuture<'_, Result<bool>> {
        Box::pin(async move {
            match self {
                Self::Single(client) => client.ping().await,
                Self::Replset(client) => client.ping().await,
            }
        })
    }

    fn select_database(&self, db_id: u8) -> Database {
        match self {
            Self::Single(client) => client.select_database(db_id),
            Self::Replset(client) => client.select_database(db_id),
        }
    }
}
