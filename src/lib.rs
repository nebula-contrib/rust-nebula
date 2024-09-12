#[cfg(feature = "graph")]
pub mod graph;

#[cfg(feature = "graph")]
pub use graph::{
    GraphTransportResponseHandler, SingleConnSession, SingleConnSessionConf,
    SingleConnSessionError, SingleConnSessionManager,
};

#[cfg(feature = "meta")]
pub mod meta;
#[cfg(feature = "meta")]
pub use self::meta::{MetaClient, MetaClientError, MetaTransportResponseHandler};

#[cfg(feature = "storage")]
pub mod storage;
#[cfg(feature = "storage")]
pub use storage::{StorageClient, StorageClientError, StorageTransportResponseHandler};

pub(crate) mod data_deserializer;
pub(crate) mod dataset_wrapper;
pub(crate) mod value_wrapper;

pub use dataset_wrapper::DataSetError;

use nebula_fbthrift_graph_v3::dependencies::common;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HostAddress {
    host: String,
    port: u16,
}

impl HostAddress {
    pub fn new(host: &str, port: u16) -> Self {
        Self {
            host: host.to_string(),
            port,
        }
    }

    pub fn to_string(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
pub struct TimezoneInfo {}
