use crate::callback::client::Client;
use crate::callback::client::RequestHandle;
use crate::callback::database::Database;
use crate::callback::handle::ReplsetNodeClient;
use crate::callback::primitives::{
    Event, GroupEvent, Lock, MaxConcurrentFlow, PriorityLock, ReadWriteLock, ReentrantLock,
    Semaphore, TokenBucketFlow, TreeLock,
};
use crate::error::Result;
use crate::protocol::result::PingCommandResult;

pub trait ClientApi: Clone + Send + Sync + 'static {
    fn close(&self) -> Result<()>;
    fn select_database(&self, db_id: u8) -> Database;
    fn node_clients(&self) -> Vec<ReplsetNodeClient>;
    fn ping<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<PingCommandResult>) + Send + 'static;

    fn lock<K: AsRef<[u8]>>(&self, key: K, timeout: u16, expired: u16) -> Lock {
        self.select_database(0).lock(key, timeout, expired)
    }

    fn event<K: AsRef<[u8]>>(
        &self,
        key: K,
        timeout: u16,
        expired: u16,
        default_set: bool,
    ) -> Event {
        self.select_database(0)
            .event(key, timeout, expired, default_set)
    }

    fn group_event<K: AsRef<[u8]>>(
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

    fn semaphore<K: AsRef<[u8]>>(
        &self,
        key: K,
        count: u16,
        timeout: u16,
        expired: u16,
    ) -> Semaphore {
        self.select_database(0)
            .semaphore(key, count, timeout, expired)
    }

    fn reentrant_lock<K: AsRef<[u8]>>(&self, key: K, timeout: u16, expired: u16) -> ReentrantLock {
        self.select_database(0)
            .reentrant_lock(key, timeout, expired)
    }

    fn read_write_lock<K: AsRef<[u8]>>(&self, key: K, timeout: u16, expired: u16) -> ReadWriteLock {
        self.select_database(0)
            .read_write_lock(key, timeout, expired)
    }

    fn priority_lock<K: AsRef<[u8]>>(
        &self,
        key: K,
        priority: u8,
        timeout: u16,
        expired: u16,
    ) -> PriorityLock {
        self.select_database(0)
            .priority_lock(key, priority, timeout, expired)
    }

    fn max_concurrent_flow<K: AsRef<[u8]>>(
        &self,
        key: K,
        max: u16,
        timeout: u16,
        expired: u16,
    ) -> MaxConcurrentFlow {
        self.select_database(0)
            .max_concurrent_flow(key, max, timeout, expired)
    }

    fn token_bucket_flow<K: AsRef<[u8]>>(
        &self,
        key: K,
        count: u16,
        timeout: u16,
        period: f64,
    ) -> TokenBucketFlow {
        self.select_database(0)
            .token_bucket_flow(key, count, timeout, period)
    }

    fn tree_lock<K: AsRef<[u8]>>(&self, key: K, timeout: u16, expired: u16) -> TreeLock {
        self.select_database(0).tree_lock(key, timeout, expired)
    }
}

impl ClientApi for Client {
    fn close(&self) -> Result<()> {
        Self::close(self)
    }

    fn select_database(&self, db_id: u8) -> Database {
        Self::select_database(self, db_id)
    }

    fn node_clients(&self) -> Vec<ReplsetNodeClient> {
        Self::node_clients(self)
    }

    fn ping<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<PingCommandResult>) + Send + 'static,
    {
        Self::ping(self, callback)
    }
}

impl ClientApi for crate::callback::replset::ReplsetClient {
    fn close(&self) -> Result<()> {
        Self::close(self)
    }

    fn select_database(&self, db_id: u8) -> Database {
        Self::select_database(self, db_id)
    }

    fn node_clients(&self) -> Vec<ReplsetNodeClient> {
        Self::node_clients(self)
    }

    fn ping<F>(&self, callback: F) -> Result<RequestHandle>
    where
        F: FnOnce(Result<PingCommandResult>) + Send + 'static,
    {
        Self::ping(self, callback)
    }
}
