use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;
use fbthrift::{
    BinaryProtocol, BufMutExt, Framing, FramingDecoded, FramingEncodedFinal, ProtocolEncoded,
    Transport,
};
use fbthrift_transport::{
    impl_tokio::{TokioSleep, TokioTcpStream},
    AsyncTransport,
};
use nebula_fbthrift_storage_v3::{
    errors::graph_storage_service::{ScanEdgeError, ScanVertexError},
    types::{ScanEdgeRequest, ScanResponse, ScanVertexRequest},
    EdgeProp, ScanCursor, VertexProp,
};
use serde::de::DeserializeOwned;

use crate::dataset_wrapper::{DataSetError, DataSetWrapper, Record};
use crate::dataset_wrapper_proxy;
use crate::value_wrapper::ValueWrapper;
use crate::TimezoneInfo;
use crate::{
    common::{types::HostAddr, Row},
    MetaTransportResponseHandler,
};

use super::{StorageClient, StorageTransportResponseHandler};

const DEFAULT_START_TIME: i64 = 0;
const DEFAULT_END_TIME: i64 = i64::MAX;
const DEFAULT_LIMIT: i64 = 1000;

pub struct StorageScanVertexOutput<
    'a,
    MT = AsyncTransport<TokioTcpStream, TokioSleep, MetaTransportResponseHandler>,
    ST = AsyncTransport<TokioTcpStream, TokioSleep, StorageTransportResponseHandler>,
> where
    MT: Transport + Framing<DecBuf = std::io::Cursor<bytes::Bytes>, EncBuf = bytes::BytesMut>,
    ST: Transport + Framing<DecBuf = std::io::Cursor<bytes::Bytes>, EncBuf = bytes::BytesMut>,
    Bytes: Framing<DecBuf = FramingDecoded<MT>> + Framing<DecBuf = FramingDecoded<ST>>,
    ProtocolEncoded<BinaryProtocol<MT>>: BufMutExt<Final = FramingEncodedFinal<MT>>,
    ProtocolEncoded<BinaryProtocol<ST>>: BufMutExt<Final = FramingEncodedFinal<ST>>,
{
    space_id: i32,
    vertex_prop: Option<VertexProp>,
    leader_map: HashMap<i32, HostAddr>,
    sclient: &'a StorageClient<MT, ST>,
}

impl<'a, MT, ST> StorageScanVertexOutput<'a, MT, ST>
where
    MT: Transport + Framing<DecBuf = std::io::Cursor<Bytes>, EncBuf = bytes::BytesMut>,
    ST: Transport + Framing<DecBuf = std::io::Cursor<Bytes>, EncBuf = bytes::BytesMut>,
    Bytes: Framing<DecBuf = FramingDecoded<MT>> + Framing<DecBuf = FramingDecoded<ST>>,
    ProtocolEncoded<BinaryProtocol<MT>>: BufMutExt<Final = FramingEncodedFinal<MT>>,
    ProtocolEncoded<BinaryProtocol<ST>>: BufMutExt<Final = FramingEncodedFinal<ST>>,
{
    pub fn new(
        space_id: i32,
        vertex_prop: Option<VertexProp>,
        leader_map: HashMap<i32, HostAddr>,
        sclient: &'a StorageClient<MT, ST>,
    ) -> Self {
        Self {
            space_id,
            vertex_prop,
            leader_map,
            sclient,
        }
    }

    pub async fn execute(&mut self) -> Result<Vec<StorageQueryOutput>, StorageQueryError> {
        let mut data_set = vec![];

        for (part_id, leader) in &self.leader_map {
            println!("Part ID: {}, Leader: {:?}", part_id, leader);

            let cursor = ScanCursor {
                next_cursor: None, // Option 为空
                ..Default::default()
            };

            let mut part: BTreeMap<i32, ScanCursor> = BTreeMap::new();
            part.insert(*part_id, cursor);

            let resp = self.sclient.connection_map[leader]
                .scan_vertex(&ScanVertexRequest {
                    space_id: self.space_id,
                    parts: part,
                    return_columns: vec![self.vertex_prop.clone().unwrap()],
                    limit: DEFAULT_LIMIT,
                    start_time: Some(DEFAULT_START_TIME),
                    end_time: Some(DEFAULT_END_TIME),
                    filter: None,
                    only_latest_version: false,
                    enable_read_from_follower: true,
                    common: None,
                    ..Default::default()
                })
                .await
                .map_err(StorageQueryError::ScanVertexError)?;
            let resp = StorageQueryOutput::new(resp, self.sclient.timezone_info.clone());

            data_set.push(resp);
        }
        Ok(data_set)
    }
}

pub struct StorageScanEdgeOutput<
    'a,
    MT = AsyncTransport<TokioTcpStream, TokioSleep, MetaTransportResponseHandler>,
    ST = AsyncTransport<TokioTcpStream, TokioSleep, StorageTransportResponseHandler>,
> where
    MT: Transport + Framing<DecBuf = std::io::Cursor<bytes::Bytes>, EncBuf = bytes::BytesMut>,
    ST: Transport + Framing<DecBuf = std::io::Cursor<bytes::Bytes>, EncBuf = bytes::BytesMut>,
    Bytes: Framing<DecBuf = FramingDecoded<MT>> + Framing<DecBuf = FramingDecoded<ST>>,
    ProtocolEncoded<BinaryProtocol<MT>>: BufMutExt<Final = FramingEncodedFinal<MT>>,
    ProtocolEncoded<BinaryProtocol<ST>>: BufMutExt<Final = FramingEncodedFinal<ST>>,
{
    space_id: i32,
    edge_prop: Option<EdgeProp>,
    leader_map: HashMap<i32, HostAddr>,
    sclient: &'a StorageClient<MT, ST>,
}

impl<'a, MT, ST> StorageScanEdgeOutput<'a, MT, ST>
where
    MT: Transport + Framing<DecBuf = std::io::Cursor<Bytes>, EncBuf = bytes::BytesMut>,
    ST: Transport + Framing<DecBuf = std::io::Cursor<Bytes>, EncBuf = bytes::BytesMut>,
    Bytes: Framing<DecBuf = FramingDecoded<MT>> + Framing<DecBuf = FramingDecoded<ST>>,
    ProtocolEncoded<BinaryProtocol<MT>>: BufMutExt<Final = FramingEncodedFinal<MT>>,
    ProtocolEncoded<BinaryProtocol<ST>>: BufMutExt<Final = FramingEncodedFinal<ST>>,
{
    pub fn new(
        space_id: i32,
        edge_prop: Option<EdgeProp>,
        leader_map: HashMap<i32, HostAddr>,
        sclient: &'a StorageClient<MT, ST>,
    ) -> Self {
        Self {
            space_id,
            edge_prop,
            leader_map,
            sclient,
        }
    }

    pub async fn execute(&mut self) -> Result<Vec<StorageQueryOutput>, StorageQueryError> {
        let mut data_set = vec![];

        for (part_id, leader) in &self.leader_map {
            println!("Part ID: {}, Leader: {:?}", part_id, leader);

            let cursor = ScanCursor {
                next_cursor: None, // Option 为空
                ..Default::default()
            };

            let mut part: BTreeMap<i32, ScanCursor> = BTreeMap::new();
            part.insert(*part_id, cursor);

            let resp = self.sclient.connection_map[leader]
                .scan_edge(&ScanEdgeRequest {
                    space_id: self.space_id,
                    parts: part,
                    return_columns: vec![self.edge_prop.clone().unwrap()],
                    limit: DEFAULT_LIMIT,
                    start_time: Some(DEFAULT_START_TIME),
                    end_time: Some(DEFAULT_END_TIME),
                    filter: None,
                    only_latest_version: false,
                    enable_read_from_follower: true,
                    common: None,
                    ..Default::default()
                })
                .await
                .map_err(StorageQueryError::ScanEdgeError)?;
            let resp = StorageQueryOutput::new(resp, self.sclient.timezone_info.clone());

            data_set.push(resp);
        }
        Ok(data_set)
    }
}

#[derive(Debug)]
pub struct StorageQueryOutput {
    pub resp: ScanResponse,
    data_set: Option<DataSetWrapper>,
}

impl StorageQueryOutput {
    pub fn new(mut resp: ScanResponse, timezone_info: TimezoneInfo) -> Self {
        let data_set = resp.props.take();
        let data_set = data_set.map(|v| DataSetWrapper::new(v, timezone_info));
        Self { resp, data_set }
    }
}

impl Default for StorageQueryOutput {
    fn default() -> Self {
        Self {
            resp: ScanResponse::default(),
            data_set: None,
        }
    }
}

dataset_wrapper_proxy!(StorageQueryOutput);

//
//
//
#[derive(Debug)]
pub enum StorageQueryError {
    ScanEdgeError(ScanEdgeError),
    ScanVertexError(ScanVertexError),
}

impl core::fmt::Display for StorageQueryError {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Self::ScanEdgeError(err) => write!(f, "ScanEdgeError {err}"),
            Self::ScanVertexError(err) => write!(f, "ScanVertexError {err}"),
        }
    }
}

impl std::error::Error for StorageQueryError {}
