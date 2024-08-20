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
    errors::graph_service::AuthenticateError,
};

use crate::GraphTransportResponseHandler;
use crate::HostAddress;

//
//
//
pub(super) struct GraphConnection<
    T = AsyncTransport<TokioTcpStream, TokioSleep, GraphTransportResponseHandler>,
> where
    T: Transport + Framing<DecBuf = std::io::Cursor<Bytes>>,
    Bytes: Framing<DecBuf = FramingDecoded<T>>,
    ProtocolEncoded<BinaryProtocol>: BufMutExt<Final = FramingEncodedFinal<T>>,
{
    pub(super) service: GraphServiceImpl<BinaryProtocol, T>,
}

impl<T> GraphConnection<T>
where
    T: Transport + Framing<DecBuf = std::io::Cursor<Bytes>>,
    Bytes: Framing<DecBuf = FramingDecoded<T>>,
    ProtocolEncoded<BinaryProtocol>: BufMutExt<Final = FramingEncodedFinal<T>>,
{
    #[allow(unused)]
    pub(super) fn new_with_transport(transport: T) -> Self {
        Self {
            service: GraphServiceImpl::<BinaryProtocol, _>::new(transport),
        }
    }

    pub(super) async fn authenticate(
        &self,
        username: &str,
        password: &str,
    ) -> Result<i64, AuthenticateError> {
        let res = self
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

        Ok(session_id)
    }
}

impl GraphConnection {
    pub(super) async fn new(addr: HostAddress) -> Result<Self, Box<dyn std::error::Error>> {
        let transport = AsyncTransport::with_tokio_tcp_connect(
            addr.to_string(),
            AsyncTransportConfiguration::new(GraphTransportResponseHandler),
        )
        .await?;
        Ok(Self {
            service: GraphServiceImpl::<BinaryProtocol, _>::new(transport),
        })
    }
}
