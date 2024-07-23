use async_trait::async_trait;
use bytes::Bytes;
use fbthrift::{
    ApplicationException, ApplicationExceptionErrorCode, BinaryProtocol, BufMutExt, Framing,
    FramingDecoded, FramingEncodedFinal, ProtocolEncoded, Transport,
};
use fbthrift_transport::{
    impl_tokio::{TokioSleep, TokioTcpStream},
    AsyncTransport, AsyncTransportConfiguration,
};
use nebula_fbthrift_graph_v3::{
    client::{GraphService, GraphServiceImpl},
    dependencies::common::types::ErrorCode,
    errors::graph_service::{AuthenticateError, ExecuteError, ExecuteJsonError, SignoutError},
};
use std::io::{Error as IoError, ErrorKind as IoErrorKind};

use crate::TimezoneInfo;
use crate::{
    graph::query::{GraphQueryError, GraphQueryOutput},
    GraphTransportResponseHandler,
};

use super::query::GraphQuery;

//
//
//
struct GraphConnection<
    T = AsyncTransport<TokioTcpStream, TokioSleep, GraphTransportResponseHandler>,
> where
    T: Transport + Framing<DecBuf = std::io::Cursor<Bytes>>,
    Bytes: Framing<DecBuf = FramingDecoded<T>>,
    ProtocolEncoded<BinaryProtocol>: BufMutExt<Final = FramingEncodedFinal<T>>,
{
    service: GraphServiceImpl<BinaryProtocol, T>,
}

impl<T> GraphConnection<T>
where
    T: Transport + Framing<DecBuf = std::io::Cursor<Bytes>>,
    Bytes: Framing<DecBuf = FramingDecoded<T>>,
    ProtocolEncoded<BinaryProtocol>: BufMutExt<Final = FramingEncodedFinal<T>>,
{
    #[allow(unused)]
    fn new_with_transport(transport: T) -> Self {
        Self {
            service: GraphServiceImpl::<BinaryProtocol, _>::new(transport),
        }
    }
}

impl GraphConnection {
    async fn new(addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let transport = AsyncTransport::with_tokio_tcp_connect(
            addr,
            AsyncTransportConfiguration::new(GraphTransportResponseHandler),
        )
        .await?;
        Ok(Self {
            service: GraphServiceImpl::<BinaryProtocol, _>::new(transport),
        })
    }
}

//
//
//
pub struct GraphClient<
    T = AsyncTransport<TokioTcpStream, TokioSleep, GraphTransportResponseHandler>,
> where
    T: Transport + Framing<DecBuf = std::io::Cursor<Bytes>>,
    Bytes: Framing<DecBuf = FramingDecoded<T>>,
    ProtocolEncoded<BinaryProtocol>: BufMutExt<Final = FramingEncodedFinal<T>>,
{
    connection: GraphConnection<T>,
}

impl<T> GraphClient<T>
where
    T: Transport + Framing<DecBuf = std::io::Cursor<Bytes>>,
    Bytes: Framing<DecBuf = FramingDecoded<T>>,
    ProtocolEncoded<BinaryProtocol>: BufMutExt<Final = FramingEncodedFinal<T>>,
{
    pub fn new_with_transport(transport: T) -> Self {
        Self {
            connection: GraphConnection::new_with_transport(transport),
        }
    }

    #[allow(clippy::ptr_arg)]
    pub async fn authenticate(
        self,
        username: &str,
        password: &str,
    ) -> Result<GraphSession<T>, AuthenticateError> {
        let res = self
            .connection
            .service
            .authenticate(&username.as_bytes().to_vec(), &password.as_bytes().to_vec())
            .await?;

        if res.error_code != ErrorCode::SUCCEEDED {
            return Err(ApplicationException::new(
                ApplicationExceptionErrorCode::Unknown,
                res.error_msg
                    .map(|x| String::from_utf8_lossy(&x).to_string())
                    .unwrap_or_else(|| "Unknown".to_owned()),
            )
            .into());
        }
        let session_id = res.session_id.ok_or_else(|| {
            ApplicationException::new(
                ApplicationExceptionErrorCode::InternalError,
                "Missing session_id".to_owned(),
            )
        })?;

        Ok(GraphSession::new(self.connection, session_id))
    }
}

impl GraphClient {
    pub async fn new(addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            connection: GraphConnection::new(addr).await?,
        })
    }
}

//
//
//
pub struct GraphSession<T>
where
    T: Transport + Framing<DecBuf = std::io::Cursor<Bytes>>,
    Bytes: Framing<DecBuf = FramingDecoded<T>>,
    ProtocolEncoded<BinaryProtocol>: BufMutExt<Final = FramingEncodedFinal<T>>,
{
    connection: GraphConnection<T>,
    session_id: i64,
    timezone_info: TimezoneInfo,
    close_required: bool,
}

impl<T> GraphSession<T>
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
impl<T> GraphQuery for GraphSession<T>
where
    T: Transport + Send + Sync + Framing<DecBuf = std::io::Cursor<Bytes>>,
    Bytes: Framing<DecBuf = FramingDecoded<T>>,
    ProtocolEncoded<BinaryProtocol>: BufMutExt<Final = FramingEncodedFinal<T>>,
{
    async fn query(&mut self, stmt: &str) -> Result<GraphQueryOutput, GraphQueryError> {
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

                return Err(GraphQueryError::ExecuteError(ExecuteError::ThriftError(
                    err,
                )));
            }
            Err(err) => return Err(GraphQueryError::ExecuteError(err)),
        };

        match res.error_code {
            ErrorCode::SUCCEEDED => {}
            ErrorCode::E_SESSION_INVALID | ErrorCode::E_SESSION_TIMEOUT => {
                self.close_required = true;
                return Err(GraphQueryError::ResponseError(
                    res.error_code,
                    res.error_msg,
                ));
            }
            _ => {
                return Err(GraphQueryError::ResponseError(
                    res.error_code,
                    res.error_msg,
                ));
            }
        }

        Ok(GraphQueryOutput::new(res, self.timezone_info.clone()))
    }
}
