use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use fbthrift_transport::{AsyncTransport, AsyncTransportConfiguration};
use fbthrift_transport_response_handler::ResponseHandler;

use crate::HostAddress;
use crate::{
    graph::{connection::GraphConnection, GraphQuery},
    GraphTransportResponseHandler,
};

use super::{SingleConnSession, SingleConnSessionError};

#[derive(Debug)]
pub struct SingleConnSessionConf {
    /// Set the host addresses of the graphd servers for load balancing
    pub host_addrs: Vec<HostAddress>,
    /// Index of the current host address being used
    host_idx: AtomicUsize,
    /// graphd's username
    pub username: String,
    /// graphd's password
    pub password: String,
    /// The dafault space after connecting.
    /// ## Notice
    /// - If it's set `None`, user has to execute `USE your_space_name;`
    /// manually before operating on a centain space.
    /// - If it's set `Some(...)`, user can also switch space by executing
    /// `USE your_space_name;` manually.
    pub space: Option<String>,
    /// Set fbthrift buf_size
    pub buf_size: Option<usize>,
    /// Set fbthrift max_buf_size
    pub max_buf_size: Option<usize>,
    /// Set fbthrift max_parse_response_bytes_count
    pub max_parse_response_bytes_count: Option<u8>,
    /// Set fbthrift read_timeout
    pub read_timeout: Option<u32>,
}

impl Clone for SingleConnSessionConf {
    fn clone(&self) -> Self {
        Self {
            host_addrs: self.host_addrs.clone(),
            host_idx: AtomicUsize::new(self.host_idx.load(Ordering::Relaxed)),
            username: self.username.clone(),
            password: self.password.clone(),
            space: self.space.clone(),
            buf_size: self.buf_size.clone(),
            max_buf_size: self.max_buf_size.clone(),
            max_parse_response_bytes_count: self.max_parse_response_bytes_count.clone(),
            read_timeout: self.read_timeout.clone(),
        }
    }
}
impl SingleConnSessionConf {
    pub fn new(
        host_addrs: Vec<HostAddress>,
        username: String,
        password: String,
        space: Option<String>,
    ) -> Self {
        Self {
            host_addrs,
            host_idx: AtomicUsize::new(0),
            username,
            password,
            space,
            buf_size: None,
            max_buf_size: None,
            max_parse_response_bytes_count: None,
            read_timeout: None,
        }
    }

    pub fn set_buf_size(&mut self, size: usize) {
        self.buf_size = Some(size)
    }
    pub fn set_max_buf_size(&mut self, size: usize) {
        self.max_buf_size = Some(size);
    }
    pub fn set_max_parse_response_bytes_count(&mut self, size: u8) {
        self.max_parse_response_bytes_count = Some(size);
    }
    pub fn set_read_timeout(&mut self, timeout_ms: u32) {
        self.read_timeout = Some(timeout_ms);
    }
}

impl SingleConnSessionConf {
    pub fn get_next_addr(&self) -> HostAddress {
        if self.host_idx.load(Ordering::Relaxed) >= self.host_addrs.len() {
            self.host_idx.store(0, Ordering::Relaxed)
        }
        let host = self.host_addrs[self.host_idx.load(Ordering::Relaxed)].clone();
        self.host_idx.fetch_add(1, Ordering::Relaxed);
        host
    }
}

//
#[derive(Clone)]
pub struct SingleConnSessionManager<H = GraphTransportResponseHandler>
where
    H: ResponseHandler,
{
    pub config: SingleConnSessionConf,
    pub transport_config: AsyncTransportConfiguration<H>,
}

impl<H> SingleConnSessionManager<H>
where
    H: ResponseHandler,
{
    pub fn new_with_response_handler(config: SingleConnSessionConf, response_handler: H) -> Self {
        let mut transport_config = AsyncTransportConfiguration::new(response_handler);
        if let Some(size) = config.max_buf_size {
            transport_config.set_max_buf_size(size);
        }
        if let Some(size) = config.buf_size {
            transport_config.set_buf_size(size);
        }
        if let Some(count) = config.max_parse_response_bytes_count {
            transport_config.set_max_parse_response_bytes_count(count);
        }
        if let Some(timeout_ms) = config.read_timeout {
            transport_config.set_read_timeout(timeout_ms);
        }
        Self {
            config,
            transport_config,
        }
    }
}

impl SingleConnSessionManager {
    pub fn new(config: SingleConnSessionConf) -> Self {
        Self::new_with_response_handler(config, GraphTransportResponseHandler)
    }

    pub async fn get_session(&self) -> Result<SingleConnSession, SingleConnSessionError> {
        let transport = AsyncTransport::with_tokio_tcp_connect(
            self.config.get_next_addr().to_string(),
            self.transport_config.clone(),
        )
        .await
        .map_err(SingleConnSessionError::TransportBuildError)?;
        let conn = GraphConnection::new_with_transport(transport);
        let session_id = conn
            .authenticate(&self.config.username, &self.config.password)
            .await
            .map_err(SingleConnSessionError::AuthenticateError)?;

        let mut session = SingleConnSession::new(conn, session_id);
        if self.config.space.is_some() {
            session
                .execute(&format!("Use {};", self.config.space.clone().unwrap()))
                .await?;
        }

        Ok(session)
    }
}

#[async_trait]
impl bb8::ManageConnection for SingleConnSessionManager {
    type Connection = SingleConnSession;
    type Error = SingleConnSessionError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.get_session().await
    }

    async fn is_valid(&self, _conn: &mut Self::Connection) -> Result<(), Self::Error> {
        Ok(())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.is_close_required()
    }
}
