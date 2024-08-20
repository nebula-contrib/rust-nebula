use core::fmt;
use std::collections::HashMap;

use bytes::Bytes;
use fbthrift::{
    BinaryProtocol, BufMutExt, Framing, FramingDecoded, FramingEncodedFinal, ProtocolEncoded,
    Transport,
};
use fbthrift_transport::{
    impl_tokio::{TokioSleep, TokioTcpStream},
    AsyncTransport, AsyncTransportConfiguration,
};
use nebula_fbthrift_storage_v3::{
    client::{GraphStorageService, GraphStorageServiceImpl},
    errors::graph_storage_service::{ScanEdgeError, ScanVertexError},
    types::{ScanEdgeRequest, ScanResponse, ScanVertexRequest},
    EdgeProp, VertexProp,
};

use super::{
    query::{StorageQueryError, StorageScanEdgeOutput, StorageScanVertexOutput},
    StorageTransportResponseHandler,
};
use crate::{common::types::HostAddr, meta::client::MetaClientError};
use crate::{storage::query::StorageQueryOutput, MetaTransportResponseHandler};
use crate::{MetaClient, TimezoneInfo};

pub(super) struct StorageConnection<
    T = AsyncTransport<TokioTcpStream, TokioSleep, StorageTransportResponseHandler>,
> where
    T: Transport + Framing<DecBuf = std::io::Cursor<Bytes>>,
    Bytes: Framing<DecBuf = FramingDecoded<T>>,
    ProtocolEncoded<BinaryProtocol>: BufMutExt<Final = FramingEncodedFinal<T>>,
{
    service: GraphStorageServiceImpl<BinaryProtocol, T>,
}

impl<T> StorageConnection<T>
where
    T: Transport + Framing<DecBuf = std::io::Cursor<Bytes>>,
    Bytes: Framing<DecBuf = FramingDecoded<T>>,
    ProtocolEncoded<BinaryProtocol>: BufMutExt<Final = FramingEncodedFinal<T>>,
{
    #[allow(unused)]
    pub fn new_with_transport(transport: T) -> Self {
        Self {
            service: GraphStorageServiceImpl::<BinaryProtocol, _>::new(transport),
        }
    }

    pub(super) async fn scan_vertex(
        &self,
        req: &ScanVertexRequest,
    ) -> Result<ScanResponse, ScanVertexError> {
        let res = self.service.scanVertex(req).await?;
        Ok(res)
    }

    pub(super) async fn scan_edge(
        &self,
        req: &ScanEdgeRequest,
    ) -> Result<ScanResponse, ScanEdgeError> {
        let res = self.service.scanEdge(req).await?;
        Ok(res)
    }
}

impl StorageConnection {
    async fn new(addr: &str) -> Result<Self, StorageClientError> {
        let transport = AsyncTransport::with_tokio_tcp_connect(
            addr,
            AsyncTransportConfiguration::new(StorageTransportResponseHandler),
        )
        .await
        .map_err(StorageClientError::CreateTransportError)?;
        Ok(Self {
            service: GraphStorageServiceImpl::<BinaryProtocol, _>::new(transport),
        })
    }
}

//
//
//
pub struct StorageClient<
    MT = AsyncTransport<TokioTcpStream, TokioSleep, MetaTransportResponseHandler>,
    ST = AsyncTransport<TokioTcpStream, TokioSleep, StorageTransportResponseHandler>,
> where
    MT: Transport + Framing<DecBuf = std::io::Cursor<Bytes>, EncBuf = bytes::BytesMut>,
    ST: Transport + Framing<DecBuf = std::io::Cursor<Bytes>, EncBuf = bytes::BytesMut>,
    Bytes: Framing<DecBuf = FramingDecoded<MT>> + Framing<DecBuf = FramingDecoded<ST>>,
    ProtocolEncoded<BinaryProtocol<MT>>: BufMutExt<Final = FramingEncodedFinal<MT>>,
    ProtocolEncoded<BinaryProtocol<ST>>: BufMutExt<Final = FramingEncodedFinal<ST>>,
{
    pub(super) connection_map: HashMap<HostAddr, StorageConnection<ST>>,
    mclient: MetaClient<MT>,
    pub(super) timezone_info: TimezoneInfo,
}

const K_VID: &str = "_vid";
const K_SRC: &str = "_src";
const K_TYPE: &str = "_type";
const K_RANK: &str = "_rank";
const K_DST: &str = "_dst";

impl<MT> StorageClient<MT>
where
    MT: Transport + Framing<DecBuf = std::io::Cursor<Bytes>, EncBuf = bytes::BytesMut>,
    Bytes: Framing<DecBuf = FramingDecoded<MT>>,
    ProtocolEncoded<BinaryProtocol<MT>>: BufMutExt<Final = FramingEncodedFinal<MT>>,
{
    pub async fn new(mclient: MetaClient<MT>) -> Self {
        Self {
            connection_map: HashMap::new(),
            mclient,
            timezone_info: TimezoneInfo {},
        }
    }

    /// `prop_names` is None means return all properties
    pub async fn scan_vertex(
        &mut self,
        space_name: &str,
        tag_name: &str,
        prop_names: Option<Vec<&str>>,
    ) -> Result<Vec<StorageQueryOutput>, StorageClientError> {
        let space_id = self
            .mclient
            .get_space_id(&space_name)
            .await
            .map_err(StorageClientError::MetaClientError)?;
        let tag_id = self
            .mclient
            .get_tag_id(&space_name, &tag_name)
            .await
            .map_err(StorageClientError::MetaClientError)?;
        let mut vertex_prop = VertexProp::default();
        vertex_prop.tag = tag_id;
        vertex_prop.props = vec![K_VID.into()];

        if let Some(prop_names) = prop_names {
            for prop_name in prop_names {
                vertex_prop.props.push(prop_name.as_bytes().to_vec())
            }
        } else {
            let schema = self
                .mclient
                .get_tag_schema(&space_name, &tag_name)
                .await
                .map_err(StorageClientError::MetaClientError)?;
            for col in &schema.columns {
                vertex_prop.props.push(col.name.clone())
            }
        }

        let result_map = self
            .mclient
            .get_part_leaders(&space_name)
            .await
            .map_err(StorageClientError::MetaClientError)?;
        for (_, host_addr) in result_map {
            let saddr = format!("{}:{}", host_addr.host, host_addr.port);
            if !self.connection_map.contains_key(host_addr) {
                let conn = StorageConnection::new(&saddr).await?;
                self.connection_map.insert(host_addr.clone(), conn);
            }
        }
        let mut scan_output =
            StorageScanVertexOutput::new(space_id, Some(vertex_prop), result_map.clone(), self);
        Ok(scan_output
            .execute()
            .await
            .map_err(StorageClientError::StorageQueryError)?)
    }

    /// `prop_names` is None means return all properties
    pub async fn scan_edge(
        &mut self,
        space_name: &str,
        edge_name: &str,
        prop_names: Option<Vec<&str>>,
    ) -> Result<Vec<StorageQueryOutput>, StorageClientError> {
        let space_id = self
            .mclient
            .get_space_id(&space_name)
            .await
            .map_err(StorageClientError::MetaClientError)?;
        let edge_type = self
            .mclient
            .get_edge_type(&space_name, &edge_name)
            .await
            .map_err(StorageClientError::MetaClientError)?;
        // create default edge props
        let mut edge_prop = EdgeProp::default();
        edge_prop.r#type = edge_type;
        edge_prop.props = vec![K_SRC.into(), K_TYPE.into(), K_RANK.into(), K_DST.into()];

        if let Some(prop_names) = prop_names {
            // if prop_names is given value, just return these props
            for prop_name in prop_names {
                edge_prop.props.push(prop_name.as_bytes().to_vec())
            }
        } else {
            // if prop_names is None, it should ask meta client for all properties
            let schema = self
                .mclient
                .get_edge_schema(&space_name, &edge_name)
                .await
                .map_err(StorageClientError::MetaClientError)?;
            for col in &schema.columns {
                edge_prop.props.push(col.name.clone())
            }
        }

        let result_map = self
            .mclient
            .get_part_leaders(&space_name)
            .await
            .map_err(StorageClientError::MetaClientError)?;
        for (_, host_addr) in result_map {
            let saddr = format!("{}:{}", host_addr.host, host_addr.port);
            if !self.connection_map.contains_key(host_addr) {
                let conn = StorageConnection::new(&saddr).await?;
                self.connection_map.insert(host_addr.clone(), conn);
            }
        }
        let mut scan_output =
            StorageScanEdgeOutput::new(space_id, Some(edge_prop), result_map.clone(), self);
        Ok(scan_output
            .execute()
            .await
            .map_err(StorageClientError::StorageQueryError)?)
    }
}

#[derive(Debug)]
pub enum StorageClientError {
    CreateTransportError(std::io::Error),
    MetaClientError(MetaClientError),
    StorageQueryError(StorageQueryError),
}

impl fmt::Display for StorageClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CreateTransportError(e) => write!(f, "CreateTransportError: {}", e),
            Self::MetaClientError(e) => write!(f, "{}", e),
            Self::StorageQueryError(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for StorageClientError {}
