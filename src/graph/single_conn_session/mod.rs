use async_trait::async_trait;
use bytes::Bytes;
use fbthrift::{
    BinaryProtocol, BufMutExt, Framing, FramingDecoded, FramingEncodedFinal, ProtocolEncoded,
    Transport,
};
use fbthrift_transport::{
    impl_tokio::{TokioSleep, TokioTcpStream},
    AsyncTransport,
};
use nebula_fbthrift_graph_v3::{
    client::GraphService as _,
    dependencies::common::types::ErrorCode,
    errors::graph_service::{ExecuteError, ExecuteJsonError, SignoutError},
    graph_service::AuthenticateError,
};
use std::io::{Error as IoError, ErrorKind as IoErrorKind};

use crate::TimezoneInfo;
use crate::{
    graph::query::{GraphQueryError, GraphQueryOutput},
    GraphTransportResponseHandler,
};

use super::{connection::GraphConnection, query::GraphQuery};

pub mod single_conn_session_manager;

//
//
//
pub struct SingleConnSession<
    T = AsyncTransport<TokioTcpStream, TokioSleep, GraphTransportResponseHandler>,
> where
    T: Transport + Framing<DecBuf = std::io::Cursor<Bytes>>,
    Bytes: Framing<DecBuf = FramingDecoded<T>>,
    ProtocolEncoded<BinaryProtocol>: BufMutExt<Final = FramingEncodedFinal<T>>,
{
    connection: GraphConnection<T>,
    session_id: i64,
    timezone_info: TimezoneInfo,
    close_required: bool,
}

impl<T> SingleConnSession<T>
where
    T: Transport + Framing<DecBuf = std::io::Cursor<Bytes>>,
    Bytes: Framing<DecBuf = FramingDecoded<T>>,
    ProtocolEncoded<BinaryProtocol>: BufMutExt<Final = FramingEncodedFinal<T>>,
{
    fn new(connection: GraphConnection<T>, session_id: i64) -> Self {
        Self {
            connection,
            session_id,
            close_required: false,
            timezone_info: TimezoneInfo {},
        }
    }

    pub async fn signout(self) -> Result<(), SignoutError> {
        self.connection.service.signout(self.session_id).await
    }

    #[allow(clippy::ptr_arg, unused)]
    async fn execute_json(&mut self, stmt: &Vec<u8>) -> Result<Vec<u8>, ExecuteJsonError> {
        let res = match self
            .connection
            .service
            .executeJson(self.session_id, stmt)
            .await
        {
            Ok(res) => res,
            Err(ExecuteJsonError::ThriftError(err)) => {
                if let Some(io_err) = err.downcast_ref::<IoError>() {
                    // "ExecuteJsonError Broken pipe (os error 32)"
                    if io_err.kind() == IoErrorKind::BrokenPipe {
                        self.close_required = true;
                    }
                }
                return Err(ExecuteJsonError::ThriftError(err));
            }
            Err(err) => return Err(err),
        };

        Ok(res)
    }

    pub fn is_close_required(&self) -> bool {
        self.close_required
    }
}

//
//
//
#[async_trait]
impl<T> GraphQuery for SingleConnSession<T>
where
    T: Transport + Send + Sync + Framing<DecBuf = std::io::Cursor<Bytes>>,
    Bytes: Framing<DecBuf = FramingDecoded<T>>,
    ProtocolEncoded<BinaryProtocol>: BufMutExt<Final = FramingEncodedFinal<T>>,
{
    type Error = SingleConnSessionError;

    async fn query(&mut self, stmt: &str) -> Result<GraphQueryOutput, Self::Error> {
        let stmt = stmt.as_bytes().to_vec();
        let res = match self
            .connection
            .service
            .execute(self.session_id, &stmt)
            .await
        {
            Ok(res) => res,
            Err(ExecuteError::ThriftError(err)) => {
                if let Some(io_err) = err.downcast_ref::<IoError>() {
                    // "ExecuteError Broken pipe (os error 32)"
                    if io_err.kind() == IoErrorKind::BrokenPipe {
                        self.close_required = true;
                    }
                }

                return Err(GraphQueryError::ExecuteError(ExecuteError::ThriftError(err)).into());
            }
            Err(err) => return Err(GraphQueryError::ExecuteError(err).into()),
        };

        match res.error_code {
            ErrorCode::SUCCEEDED => {}
            ErrorCode::E_SESSION_INVALID | ErrorCode::E_SESSION_TIMEOUT => {
                self.close_required = true;
                return Err(GraphQueryError::ResponseError(res.error_code, res.error_msg).into());
            }
            _ => {
                return Err(GraphQueryError::ResponseError(res.error_code, res.error_msg).into());
            }
        }

        Ok(GraphQueryOutput::new(res, self.timezone_info.clone()))
    }
}

#[derive(Debug)]
pub enum SingleConnSessionError {
    TransportBuildError(std::io::Error),
    AuthenticateError(AuthenticateError),
    GraphQueryError(GraphQueryError),
}

impl core::fmt::Display for SingleConnSessionError {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Self::TransportBuildError(err) => write!(f, "TransportBuildError {err}"),
            Self::AuthenticateError(err) => write!(f, "AuthenticateError {err}"),
            Self::GraphQueryError(err) => write!(f, "GraphQueryError {err}"),
        }
    }
}

impl From<GraphQueryError> for SingleConnSessionError {
    fn from(value: GraphQueryError) -> Self {
        Self::GraphQueryError(value)
    }
}

impl std::error::Error for SingleConnSessionError {}

unsafe impl Send for SingleConnSessionError {}
