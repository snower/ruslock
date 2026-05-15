use std::time::Duration;

#[derive(Clone, Debug)]
pub struct ClientOptions {
    pub connect_timeout: Duration,
    pub reconnect_interval: Duration,
    pub command_timeout_grace: Duration,
    pub auto_reconnect: bool,
    pub tcp_nodelay: bool,
    pub tcp_keepalive: bool,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            reconnect_interval: Duration::from_secs(2),
            command_timeout_grace: Duration::from_secs(120),
            auto_reconnect: true,
            tcp_nodelay: true,
            tcp_keepalive: true,
        }
    }
}
