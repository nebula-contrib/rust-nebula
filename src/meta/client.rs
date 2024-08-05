use std::{
    collections::{BTreeMap, HashMap},
    io::Cursor,
};

use bytes::Bytes;
use fbthrift::{
    BinaryProtocol, BufMutExt, Framing, FramingDecoded, FramingEncodedFinal,
    NonthrowingFunctionError, ProtocolEncoded, Transport,
};
use fbthrift_transport::{
    impl_tokio::{TokioSleep, TokioTcpStream},
    AsyncTransport, AsyncTransportConfiguration,
};
use nebula_fbthrift_meta_v3::{
    client::{MetaService, MetaServiceImpl},
    errors::meta_service::{
        GetPartsAllocError, ListEdgesError, ListPartsError, ListSpacesError, ListTagsError,
    },
    meta_service::ListHostsError,
    types::{
        GetPartsAllocReq, GetPartsAllocResp, ListEdgesReq, ListEdgesResp, ListHostType,
        ListHostsReq, ListHostsResp, ListPartsReq, ListPartsResp, ListSpacesReq, ListSpacesResp,
        ListTagsReq, ListTagsResp,
    },
    EdgeItem, HostItem, IdName, PartItem, Schema, TagItem, ID,
};

use crate::common::{HostAddr, PartitionID};
use crate::MetaTransportResponseHandler;

use super::metacache::{MetaCache, SpaceCache};

//
//
//
struct MetaConnection<T = AsyncTransport<TokioTcpStream, TokioSleep, MetaTransportResponseHandler>>
where
    T: Transport + Framing<DecBuf = Cursor<Bytes>>,
    Bytes: Framing<DecBuf = FramingDecoded<T>>,
    ProtocolEncoded<BinaryProtocol>: BufMutExt<Final = FramingEncodedFinal<T>>,
{
    service: MetaServiceImpl<BinaryProtocol, T>,
}

impl<T> MetaConnection<T>
where
    T: Transport + Framing<DecBuf = std::io::Cursor<Bytes>>,
    Bytes: Framing<DecBuf = FramingDecoded<T>>,
    ProtocolEncoded<BinaryProtocol>: BufMutExt<Final = FramingEncodedFinal<T>>,
{
    #[allow(unused)]
    pub fn new_with_transport(transport: T) -> Self {
        Self {
            service: MetaServiceImpl::<BinaryProtocol, _>::new(transport),
        }
    }

    async fn list_spaces(&self) -> Result<ListSpacesResp, ListSpacesError> {
        self.service
            .listSpaces(&ListSpacesReq {
                ..Default::default()
            })
            .await
    }

    async fn list_hosts(&self) -> Result<ListHostsResp, ListHostsError> {
        self.service
            .listHosts(&ListHostsReq {
                r#type: ListHostType::STORAGE,
                ..Default::default()
            })
            .await
    }

    #[allow(unused)]
    async fn list_parts(
        &self,
        space_id: i32,
        part_ids: Vec<i32>,
    ) -> Result<ListPartsResp, ListPartsError> {
        self.service
            .listParts(&ListPartsReq {
                space_id,
                part_ids,
                ..Default::default()
            })
            .await
    }

    #[allow(unused)]
    async fn list_tags(&self, space_id: i32) -> Result<ListTagsResp, ListTagsError> {
        self.service
            .listTags(&ListTagsReq {
                space_id,
                ..Default::default()
            })
            .await
    }

    async fn list_edges(&self, space_id: i32) -> Result<ListEdgesResp, ListEdgesError> {
        self.service
            .listEdges(&ListEdgesReq {
                space_id,
                ..Default::default()
            })
            .await
    }

    async fn get_parts_alloc(
        &self,
        space_id: i32,
    ) -> Result<GetPartsAllocResp, GetPartsAllocError> {
        self.service
            .getPartsAlloc(&GetPartsAllocReq {
                space_id,
                ..Default::default()
            })
            .await
    }
}

impl MetaConnection {
    async fn new(addr: &str) -> Result<Self, MetaClientError> {
        let transport = AsyncTransport::with_tokio_tcp_connect(
            addr,
            AsyncTransportConfiguration::new(MetaTransportResponseHandler),
        )
        .await
        .map_err(MetaClientError::CreateTransportError)?;
        Ok(Self {
            service: MetaServiceImpl::<BinaryProtocol, _>::new(transport),
        })
    }
}

//
//
//
pub struct MetaClient<T = AsyncTransport<TokioTcpStream, TokioSleep, MetaTransportResponseHandler>>
where
    T: Transport + Framing<DecBuf = std::io::Cursor<Bytes>>,
    Bytes: Framing<DecBuf = FramingDecoded<T>>,
    ProtocolEncoded<BinaryProtocol>: BufMutExt<Final = FramingEncodedFinal<T>>,
{
    connection: MetaConnection<T>,
    meta_cache: MetaCache,
    #[allow(unused)]
    maddr: Vec<String>,
}

impl<T> MetaClient<T>
where
    T: Transport + Framing<DecBuf = std::io::Cursor<Bytes>>,
    Bytes: Framing<DecBuf = FramingDecoded<T>>,
    ProtocolEncoded<BinaryProtocol>: BufMutExt<Final = FramingEncodedFinal<T>>,
{
    pub fn new_with_transport(maddr: &Vec<String>, transport: T) -> Self {
        Self {
            maddr: maddr.clone(),
            meta_cache: MetaCache::new(),
            connection: MetaConnection::new_with_transport(transport),
        }
    }

    async fn list_spaces(&self) -> Result<Vec<IdName>, ListSpacesError> {
        match self.connection.list_spaces().await {
            Ok(resp) => Ok(resp.spaces),
            Err(err) => Err(err),
        }
    }

    async fn list_hosts(&self) -> Result<Vec<HostItem>, ListHostsError> {
        match self.connection.list_hosts().await {
            Ok(resp) => Ok(resp.hosts),
            Err(err) => Err(err),
        }
    }

    #[allow(unused)]
    async fn list_parts(
        &self,
        space_id: i32,
        part_ids: Vec<i32>,
    ) -> Result<Vec<PartItem>, ListPartsError> {
        match self.connection.list_parts(space_id, part_ids).await {
            Ok(resp) => Ok(resp.parts),
            Err(err) => Err(err),
        }
    }

    async fn list_tags(&self, space_id: i32) -> Result<Vec<TagItem>, ListTagsError> {
        match self.connection.list_tags(space_id).await {
            Ok(resp) => Ok(resp.tags),
            Err(err) => Err(err),
        }
    }

    async fn list_edges(&self, space_id: i32) -> Result<Vec<EdgeItem>, ListEdgesError> {
        match self.connection.list_edges(space_id).await {
            Ok(resp) => Ok(resp.edges),
            Err(err) => Err(err),
        }
    }

    async fn get_parts_alloc(
        &self,
        space_id: i32,
    ) -> Result<BTreeMap<PartitionID, Vec<HostAddr>>, GetPartsAllocError> {
        match self.connection.get_parts_alloc(space_id).await {
            Ok(resp) => Ok(resp.parts),
            Err(err) => Err(err),
        }
    }
}

impl<T> MetaClient<T>
where
    T: Transport + Framing<DecBuf = std::io::Cursor<Bytes>>,
    Bytes: Framing<DecBuf = FramingDecoded<T>>,
    ProtocolEncoded<BinaryProtocol>: BufMutExt<Final = FramingEncodedFinal<T>>,
{
    async fn load_all(&mut self) -> Result<(), MetaClientError> {
        let spaces = self
            .list_spaces()
            .await
            .map_err(MetaClientError::LoadError)?;
        let mut space_id_names = HashMap::new();
        let mut space_caches = HashMap::new();

        for space in spaces {
            let space_id = if let ID::space_id(space_id) = space.id {
                space_id
            } else {
                0
            };
            let mut space_cache = SpaceCache {
                space_id,
                space_name: space.name,
                tag_items: HashMap::new(),
                edge_items: HashMap::new(),
                parts_alloc: self
                    .get_parts_alloc(space_id)
                    .await
                    .map_err(MetaClientError::LoadError)?,
            };

            let tags = self
                .list_tags(space_id)
                .await
                .map_err(MetaClientError::LoadError)?;
            let edges = self
                .list_edges(space_id)
                .await
                .map_err(MetaClientError::LoadError)?;

            for tag in tags {
                let tag_name = tag.tag_name.to_vec();
                if !space_cache.tag_items.contains_key(&tag_name)
                    || space_cache.tag_items[&tag_name].version < tag.version
                {
                    space_cache.tag_items.insert(tag_name, tag);
                }
            }

            for edge in edges {
                let edge_name = edge.edge_name.to_vec();
                if !space_cache.edge_items.contains_key(&edge_name)
                    || space_cache.edge_items[&edge_name].version < edge.version
                {
                    space_cache.edge_items.insert(edge_name, edge);
                }
            }

            space_id_names.insert(space_id, space_cache.space_name.clone());
            space_caches.insert(space_cache.space_name.clone(), space_cache);
        }

        let hosts = self
            .list_hosts()
            .await
            .map_err(MetaClientError::LoadError)?;
        let mut storage_addrs = vec![];
        for host_item in hosts {
            storage_addrs.push(host_item.hostAddr);
        }

        let mut storage_leader = HashMap::new();
        for (space_name, space_cache) in &space_caches {
            let mut host_addr_map = HashMap::new();
            for (part_id, _) in &space_cache.parts_alloc {
                host_addr_map.insert(*part_id, space_cache.parts_alloc[part_id][0].clone());
            }
            storage_leader.insert(space_name.clone(), host_addr_map);
        }

        self.meta_cache.space_id_names = space_id_names;
        self.meta_cache.space_caches = space_caches;
        self.meta_cache.storage_addrs = Some(storage_addrs);
        self.meta_cache.storage_leader = storage_leader;

        Ok(())
    }

    /// Gets all storage addresses.
    pub async fn get_all_storage_addrs(&mut self) -> Result<&Vec<HostAddr>, MetaClientError> {
        if self.meta_cache.storage_addrs.is_none() {
            self.load_all().await?
        }
        Ok(self.meta_cache.storage_addrs.as_ref().unwrap())
    }

    /// Gets the ID of a tag.
    pub async fn get_tag_id(
        &mut self,
        space_name: &str,
        tag_name: &str,
    ) -> Result<i32, MetaClientError> {
        let tag_item = self
            .get_tag_item(
                &space_name.as_bytes().to_vec(),
                &tag_name.as_bytes().to_vec(),
            )
            .await?;
        Ok(tag_item.tag_id)
    }

    /// Gets the type of an edge.
    pub async fn get_edge_type(
        &mut self,
        space_name: &str,
        edge_name: &str,
    ) -> Result<i32, MetaClientError> {
        let edge_item = self
            .get_edge_item(
                &space_name.as_bytes().to_vec(),
                &edge_name.as_bytes().to_vec(),
            )
            .await?;
        Ok(edge_item.edge_type)
    }

    /// Gets the ID of a space.
    pub async fn get_space_id(&mut self, space_name: &str) -> Result<i32, MetaClientError> {
        let space_name = space_name.as_bytes().to_vec();
        if !self.meta_cache.contains_space(&space_name) {
            let _ = self.load_all().await?;
        }
        let space_cache = self.meta_cache.get_space_cache(&space_name)?;
        Ok(space_cache.space_id)
    }

    /// Gets the schema of a tag.
    pub async fn get_tag_schema(
        &mut self,
        space_name: &str,
        tag_name: &str,
    ) -> Result<&Schema, MetaClientError> {
        let tag_item = self
            .get_tag_item(
                &space_name.as_bytes().to_vec(),
                &tag_name.as_bytes().to_vec(),
            )
            .await?;
        Ok(&tag_item.schema)
    }

    /// Gets the schema of an edge.
    pub async fn get_edge_schema(
        &mut self,
        space_name: &str,
        edge_name: &str,
    ) -> Result<&Schema, MetaClientError> {
        let edge_item = self
            .get_edge_item(
                &space_name.as_bytes().to_vec(),
                &edge_name.as_bytes().to_vec(),
            )
            .await?;
        Ok(&edge_item.schema)
    }

    /// Gets the leader of a partition.
    pub async fn get_part_leader(
        &mut self,
        space_name: &str,
        part_id: i32,
    ) -> Result<&HostAddr, MetaClientError> {
        let part_leaders = self.get_part_leaders(space_name).await?;
        if !part_leaders.contains_key(&part_id) {
            Err(MetaClientError::PartNotFoundError(part_id))
        } else {
            Ok(&part_leaders[&part_id])
        }
    }

    /// Gets all part leaders of a space.
    pub async fn get_part_leaders(
        &mut self,
        space_name: &str,
    ) -> Result<&HashMap<i32, HostAddr>, MetaClientError> {
        let space_name = space_name.as_bytes().to_vec();
        if !self.meta_cache.storage_leader.contains_key(&space_name) {
            let _ = self.load_all().await?;
        }
        if !self.meta_cache.storage_leader.contains_key(&space_name) {
            Err(MetaClientError::SpaceNotFoundError(space_name.to_vec()))
        } else {
            Ok(&self.meta_cache.storage_leader[&space_name])
        }
    }

    /// Gets all part allocations of a space.
    pub async fn get_part_alloc(
        &mut self,
        space_name: &str,
    ) -> Result<&BTreeMap<i32, Vec<HostAddr>>, MetaClientError> {
        let space_name = space_name.as_bytes().to_vec();
        if !self.meta_cache.contains_space(&space_name) {
            let _ = self.load_all().await?;
        }
        let space_cache = self.meta_cache.get_space_cache(&space_name)?;
        Ok(&space_cache.parts_alloc)
    }

    /// Gets a tag item.
    async fn get_tag_item(
        &mut self,
        space_name: &Vec<u8>,
        tag_name: &Vec<u8>,
    ) -> Result<&TagItem, MetaClientError> {
        if !self.meta_cache.contains_tag(space_name, tag_name) {
            let _ = self.load_all().await?;
        }
        Ok(self.meta_cache.get_tag_item(&space_name, tag_name)?)
    }

    /// Gets an edge item.
    async fn get_edge_item(
        &mut self,
        space_name: &Vec<u8>,
        edge_name: &Vec<u8>,
    ) -> Result<&EdgeItem, MetaClientError> {
        if !self.meta_cache.contains_edge(space_name, edge_name) {
            let _ = self.load_all().await?;
        }
        Ok(self.meta_cache.get_edge_item(&space_name, edge_name)?)
    }

    /// Updates the storage leader.
    pub fn update_storage_leader(&self, space_id: i32, part_id: i32, address: Option<HostAddr>) {
        todo!()
    }
}

impl MetaClient {
    pub async fn new(maddr: &Vec<String>) -> Result<Self, MetaClientError> {
        Ok(Self {
            connection: MetaConnection::new(&maddr[0]).await?,
            meta_cache: MetaCache::new(),
            maddr: maddr.clone(),
        })
    }
}

use std::fmt;

/// A custom error type for meta client operations.
#[derive(Debug)]
pub enum MetaClientError {
    CreateTransportError(std::io::Error),
    LoadError(NonthrowingFunctionError),
    SpaceNotFoundError(Vec<u8>),
    TagNotFoundError(Vec<u8>),
    EdgeNotFoundError(Vec<u8>),
    PartNotFoundError(i32),
}

impl fmt::Display for MetaClientError {
    /// Implement the Display trait to provide a human-readable description of the error.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CreateTransportError(e) => write!(f, "CreateTransportError: {}", e),
            Self::LoadError(error) => {
                write!(f, "Space not found: {:?}", error)
            }
            Self::SpaceNotFoundError(space_id) => {
                write!(f, "Space not found: {:?}", space_id)
            }
            Self::TagNotFoundError(tag_name) => {
                write!(f, "Tag not found: {:?}", tag_name)
            }
            Self::EdgeNotFoundError(edge_name) => {
                write!(f, "Edge not found: {:?}", edge_name)
            }
            Self::PartNotFoundError(part_id) => {
                write!(f, "Partition not found: {}", part_id)
            }
        }
    }
}

impl std::error::Error for MetaClientError {}
