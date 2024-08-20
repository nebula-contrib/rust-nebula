use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use fbthrift_transport::AsyncTransportConfiguration;
use fbthrift_transport_response_handler::ResponseHandler;
use nebula_fbthrift_graph_v3::graph_service::AuthenticateError;

use crate::HostAddress;
use crate::{
    graph::{connection::GraphConnection, GraphQuery, GraphQueryError},
    GraphTransportResponseHandler,
};

use super::SingleConnSession;

#[derive(Debug)]
pub struct SingleConnSessionConf {
    pub host_addrs: Vec<HostAddress>,
    host_idx: AtomicUsize,
    pub username: String,
    pub password: String,
    pub space: Option<String>,
}

impl Clone for SingleConnSessionConf {
    fn clone(&self) -> Self {
        Self {
            host_addrs: self.host_addrs.clone(),
            host_idx: AtomicUsize::new(self.host_idx.load(Ordering::Relaxed)),
            username: self.username.clone(),
            password: self.password.clone(),
            space: self.space.clone(),
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
        }
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
        Self {
            config,
            transport_config: AsyncTransportConfiguration::new(response_handler),
        }
    }
}

impl SingleConnSessionManager {
    pub fn new(config: SingleConnSessionConf) -> Self {
        Self::new_with_response_handler(config, GraphTransportResponseHandler)
    }

    pub async fn get_session(&self) -> Result<SingleConnSession, SingleConnSessionError> {
        let conn = GraphConnection::new(self.config.get_next_addr())
            .await
            .map_err(SingleConnSessionError::ConnectionError)?;
        let session_id = conn
            .authenticate(&self.config.username, &self.config.password)
            .await
            .map_err(SingleConnSessionError::AuthenticateError)?;

        let mut session = SingleConnSession::new(conn, session_id);
        if self.config.space.is_some() {
            session
                .execute(&format!("Use {};", self.config.space.clone().unwrap()))
                .await
                .map_err(SingleConnSessionError::GraphQueryError)?;
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

#[derive(Debug)]
pub enum SingleConnSessionError {
    ConnectionError(Box<dyn Error>),
    AuthenticateError(AuthenticateError),
    GraphQueryError(GraphQueryError),
}

impl core::fmt::Display for SingleConnSessionError {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Self::ConnectionError(err) => write!(f, "ConnectionError {err}"),
            Self::AuthenticateError(err) => write!(f, "AuthenticateError {err}"),
            Self::GraphQueryError(err) => write!(f, "GraphQueryError {err}"),
        }
    }
}

impl std::error::Error for SingleConnSessionError {}

unsafe impl Send for SingleConnSessionError {}
