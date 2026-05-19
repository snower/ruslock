use std::sync::Arc;

use crate::aio::api::{BoxFuture, ClientApi};
use crate::aio::connection::Connection;
use crate::aio::database::{ClientBackend, Database};
use crate::aio::primitives::{
    Event, GroupEvent, Lock, MaxConcurrentFlow, PriorityLock, ReadWriteLock, ReentrantLock,
    Semaphore, TokenBucketFlow, TreeLock,
};
use crate::error::Result;
use crate::options::ClientOptions;
use crate::protocol::command::{Command, PingCommand};
use crate::protocol::constants::COMMAND_RESULT_SUCCED;
use crate::protocol::id::Id16;
use crate::protocol::result::CommandResult;

#[derive(Clone, Debug)]
pub struct Client {
    inner: Arc<ClientInner>,
}

#[derive(Debug)]
pub(crate) struct ClientInner {
    pub(crate) connection: Connection,
}

impl Client {
    pub fn new<A: ToString>(address: A) -> Self {
        Self::with_options(address, ClientOptions::default())
    }

    pub fn with_options<A: ToString>(address: A, options: ClientOptions) -> Self {
        Self {
            inner: Arc::new(ClientInner {
                connection: Connection::new(address.to_string(), options),
            }),
        }
    }

    pub async fn connect<A: ToString>(address: A) -> Result<Self> {
        let client = Self::new(address);
        client.open().await?;
        Ok(client)
    }

    pub async fn open(&self) -> Result<()> {
        self.inner.connection.open().await
    }

    pub async fn close(&self) {
        self.inner.connection.close().await;
    }

    pub async fn ping(&self) -> Result<bool> {
        let result = self
            .send_command(Command::Ping(PingCommand::new(Id16::new())))
            .await?;
        Ok(result.result_code() == COMMAND_RESULT_SUCCED)
    }

    pub fn select_database(&self, db_id: u8) -> Database {
        Database::new(ClientBackend::Single(self.clone()), db_id)
    }

    pub fn lock<K: AsRef<[u8]>>(&self, key: K, timeout: u16, expired: u16) -> Lock {
        self.select_database(0).lock(key, timeout, expired)
    }

    pub fn event<K: AsRef<[u8]>>(
        &self,
        key: K,
        timeout: u16,
        expired: u16,
        default_set: bool,
    ) -> Event {
        self.select_database(0)
            .event(key, timeout, expired, default_set)
    }

    pub fn group_event<K: AsRef<[u8]>>(
        &self,
        key: K,
        client_id: u64,
        version_id: u64,
        timeout: u16,
        expired: u16,
    ) -> GroupEvent {
        self.select_database(0)
            .group_event(key, client_id, version_id, timeout, expired)
    }

    pub fn semaphore<K: AsRef<[u8]>>(
        &self,
        key: K,
        count: u16,
        timeout: u16,
        expired: u16,
    ) -> Semaphore {
        self.select_database(0)
            .semaphore(key, count, timeout, expired)
    }

    pub fn reentrant_lock<K: AsRef<[u8]>>(
        &self,
        key: K,
        timeout: u16,
        expired: u16,
    ) -> ReentrantLock {
        self.select_database(0)
            .reentrant_lock(key, timeout, expired)
    }

    pub fn read_write_lock<K: AsRef<[u8]>>(
        &self,
        key: K,
        timeout: u16,
        expired: u16,
    ) -> ReadWriteLock {
        self.select_database(0)
            .read_write_lock(key, timeout, expired)
    }

    pub fn priority_lock<K: AsRef<[u8]>>(
        &self,
        key: K,
        priority: u8,
        timeout: u16,
        expired: u16,
    ) -> PriorityLock {
        self.select_database(0)
            .priority_lock(key, priority, timeout, expired)
    }

    pub fn max_concurrent_flow<K: AsRef<[u8]>>(
        &self,
        key: K,
        max: u16,
        timeout: u16,
        expired: u16,
    ) -> MaxConcurrentFlow {
        self.select_database(0)
            .max_concurrent_flow(key, max, timeout, expired)
    }

    pub fn token_bucket_flow<K: AsRef<[u8]>>(
        &self,
        key: K,
        count: u16,
        timeout: u16,
        period: f64,
    ) -> TokenBucketFlow {
        self.select_database(0)
            .token_bucket_flow(key, count, timeout, period)
    }

    pub fn tree_lock<K: AsRef<[u8]>>(&self, key: K, timeout: u16, expired: u16) -> TreeLock {
        self.select_database(0).tree_lock(key, timeout, expired)
    }

    pub async fn pending_len(&self) -> usize {
        self.inner.connection.pending_len().await
    }

    pub(crate) fn init_type(&self) -> u8 {
        self.inner.connection.init_type()
    }

    pub(crate) async fn send_command(&self, command: Command) -> Result<CommandResult> {
        self.inner.connection.send_command(command).await
    }
}

impl ClientApi for Client {
    fn open(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(Self::open(self))
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(Self::close(self))
    }

    fn ping(&self) -> BoxFuture<'_, Result<bool>> {
        Box::pin(Self::ping(self))
    }

    fn select_database(&self, db_id: u8) -> Database {
        Self::select_database(self, db_id)
    }
}
