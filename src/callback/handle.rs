use crate::callback::api::ClientApi;
use crate::callback::buffer::{ReaderBuffer, WriterBuffer};
use crate::callback::client::Client;
use crate::callback::database::Database;
use crate::callback::replset::{IntoNodeList, ReplsetClient};
use crate::error::Result;
use crate::protocol::result::PingCommandResult;

#[derive(Clone, Debug)]
pub struct RequestTransport {
    node_index: usize,
    address: String,
    client: Client,
}

impl RequestTransport {
    pub(crate) fn new(node_index: usize, address: String, client: Client) -> Self {
        Self {
            node_index,
            address,
            client,
        }
    }

    pub fn node_index(&self) -> usize {
        self.node_index
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn client(&self) -> Client {
        self.client.clone()
    }

    pub fn reader_buffer(&self) -> ReaderBuffer {
        self.client.reader_buffer()
    }

    pub fn writer_buffer(&self) -> WriterBuffer {
        self.client.writer_buffer()
    }
}

#[derive(Clone, Debug)]
pub struct ReplsetNodeClient {
    index: usize,
    address: String,
    client: Client,
}

#[derive(Clone, Debug)]
pub enum ClientHandle {
    Single(Client),
    Replset(ReplsetClient),
}

impl ClientHandle {
    pub fn new<N: IntoNodeList>(nodes: N) -> Result<Self> {
        let nodes = nodes.into_nodes();
        if nodes.len() == 1 {
            Ok(Self::Single(Client::new()))
        } else {
            Ok(Self::Replset(ReplsetClient::new(nodes)?))
        }
    }

    pub fn close(&self) -> Result<()> {
        <Self as ClientApi>::close(self)
    }

    pub fn select_database(&self, db_id: u8) -> Database {
        <Self as ClientApi>::select_database(self, db_id)
    }

    pub fn node_clients(&self) -> Vec<ReplsetNodeClient> {
        <Self as ClientApi>::node_clients(self)
    }

    pub fn ping<F>(&self, callback: F) -> Result<crate::callback::RequestHandle>
    where
        F: FnOnce(Result<PingCommandResult>) + Send + 'static,
    {
        <Self as ClientApi>::ping(self, callback)
    }

    pub fn lock<K: AsRef<[u8]>>(
        &self,
        key: K,
        timeout: u16,
        expired: u16,
    ) -> crate::callback::Lock {
        <Self as ClientApi>::lock(self, key, timeout, expired)
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
    fn close(&self) -> Result<()> {
        match self {
            Self::Single(client) => client.close(),
            Self::Replset(client) => client.close(),
        }
    }

    fn select_database(&self, db_id: u8) -> Database {
        match self {
            Self::Single(client) => client.select_database(db_id),
            Self::Replset(client) => client.select_database(db_id),
        }
    }

    fn node_clients(&self) -> Vec<ReplsetNodeClient> {
        match self {
            Self::Single(client) => client.node_clients(),
            Self::Replset(client) => client.node_clients(),
        }
    }

    fn ping<F>(&self, callback: F) -> Result<crate::callback::RequestHandle>
    where
        F: FnOnce(Result<PingCommandResult>) + Send + 'static,
    {
        match self {
            Self::Single(client) => client.ping(callback),
            Self::Replset(client) => client.ping(callback),
        }
    }
}

impl ReplsetNodeClient {
    pub(crate) fn new(index: usize, address: String, client: Client) -> Self {
        Self {
            index,
            address,
            client,
        }
    }

    pub fn index(&self) -> usize {
        self.index
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn client(&self) -> Client {
        self.client.clone()
    }

    pub fn reader_buffer(&self) -> ReaderBuffer {
        self.client.reader_buffer()
    }

    pub fn writer_buffer(&self) -> WriterBuffer {
        self.client.writer_buffer()
    }
}
