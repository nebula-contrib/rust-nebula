use std::collections::{BTreeMap, HashMap};

use nebula_fbthrift_meta_v3::{EdgeItem, TagItem};

use crate::HostAddr;

use super::client::MetaClientError;

pub struct SpaceCache {
    pub space_id: i32,
    pub space_name: Vec<u8>,
    pub tag_items: HashMap<Vec<u8>, TagItem>,
    pub edge_items: HashMap<Vec<u8>, EdgeItem>,
    pub parts_alloc: BTreeMap<i32, Vec<HostAddr>>,
}

pub struct MetaCache {
    pub space_caches: HashMap<Vec<u8>, SpaceCache>,
    pub space_id_names: HashMap<i32, Vec<u8>>,
    pub storage_addrs: Option<Vec<HostAddr>>,
    pub storage_leader: HashMap<Vec<u8>, HashMap<i32, HostAddr>>,
}

impl MetaCache {
    pub fn new() -> Self {
        MetaCache {
            space_caches: HashMap::new(),
            space_id_names: HashMap::new(),
            storage_addrs: None,
            storage_leader: HashMap::new(),
        }
    }

    pub(super) fn contains_space(&self, space_name: &Vec<u8>) -> bool {
        self.space_caches.contains_key(space_name)
    }

    pub(super) fn get_space_cache(
        &self,
        space_name: &Vec<u8>,
    ) -> Result<&SpaceCache, MetaClientError> {
        if !self.contains_space(space_name) {
            Err(MetaClientError::SpaceNotFoundError(space_name.to_vec()))
        } else {
            Ok(&self.space_caches[space_name])
        }
    }

    pub(super) fn contains_tag(&self, space_name: &Vec<u8>, tag_name: &Vec<u8>) -> bool {
        if !self.contains_space(space_name) {
            return false;
        }
        self.space_caches[space_name]
            .tag_items
            .contains_key(tag_name)
    }

    pub(super) fn get_tag_item(
        &self,
        space_name: &Vec<u8>,
        tag_name: &Vec<u8>,
    ) -> Result<&TagItem, MetaClientError> {
        if !self.contains_space(space_name) {
            Err(MetaClientError::SpaceNotFoundError(space_name.to_vec()))
        } else {
            if !self.space_caches[space_name]
                .tag_items
                .contains_key(tag_name)
            {
                Err(MetaClientError::TagNotFoundError(tag_name.to_vec()))
            } else {
                Ok(&self.space_caches[space_name].tag_items[tag_name])
            }
        }
    }

    pub(super) fn contains_edge(&self, space_name: &Vec<u8>, edge_name: &Vec<u8>) -> bool {
        if !self.contains_space(space_name) {
            return false;
        }
        self.space_caches[space_name]
            .edge_items
            .contains_key(edge_name)
    }

    pub(super) fn get_edge_item(
        &self,
        space_name: &Vec<u8>,
        edge_name: &Vec<u8>,
    ) -> Result<&EdgeItem, MetaClientError> {
        if !self.contains_space(space_name) {
            Err(MetaClientError::SpaceNotFoundError(space_name.to_vec()))
        } else {
            if !self.space_caches[space_name]
                .edge_items
                .contains_key(edge_name)
            {
                Err(MetaClientError::EdgeNotFoundError(edge_name.to_vec()))
            } else {
                Ok(&self.space_caches[space_name].edge_items[edge_name])
            }
        }
    }
}
