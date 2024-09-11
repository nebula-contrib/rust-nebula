use async_trait::async_trait;
use nebula_fbthrift_graph_v3::{
    errors::graph_service::ExecuteError, types::ExecutionResponse, PlanDescription,
};
use serde::{de::DeserializeOwned, Deserialize};

use crate::common::types::{ErrorCode, Row};
use crate::data_deserializer::DataDeserializeError;
use crate::dataset_wrapper_proxy;
use crate::{
    dataset_wrapper::{DataSetWrapper, Record},
    value_wrapper::ValueWrapper,
    TimezoneInfo,
};

#[async_trait]
pub trait GraphQuery {
    /// Execute stmt and return the query output.
    /// ## Notice
    /// For operation that doesn't return result, like `CREATE TAG`, `USE space` etc.
    /// it's recommended to use `execute(stmt)`.
    #[allow(clippy::ptr_arg)]
    async fn query(&mut self, stmt: &str) -> Result<GraphQueryOutput, GraphQueryError>;

    /// Execute stmt and doesn't return the execution output.
    #[allow(clippy::ptr_arg)]
    async fn execute(&mut self, stmt: &str) -> Result<(), GraphQueryError> {
        let _ = self.query(stmt).await?;
        Ok(())
    }

    async fn show_hosts(&mut self) -> Result<Vec<Host>, GraphQueryError> {
        let tmp = self.query(STMT_SHOW_HOSTS).await?;
        tmp.scan::<Host>()
            .map_err(GraphQueryError::DataDeserializeError)
    }

    async fn show_spaces(&mut self) -> Result<Vec<Space>, GraphQueryError> {
        let tmp = self.query(STMT_SHOW_SPACES).await?;
        tmp.scan::<Space>()
            .map_err(GraphQueryError::DataDeserializeError)
    }
}

#[derive(Debug)]
pub struct GraphQueryOutput {
    resp: ExecutionResponse,
    data_set: Option<DataSetWrapper>,
}

impl GraphQueryOutput {
    pub(super) fn new(mut resp: ExecutionResponse, timezone_info: TimezoneInfo) -> Self {
        let data_set = resp.data.take();
        let data_set = data_set.map(|v| DataSetWrapper::new(v, timezone_info));
        Self { resp, data_set }
    }
}

impl GraphQueryOutput {
    pub fn get_error_code(&self) -> ErrorCode {
        self.resp.error_code
    }

    pub fn get_latency(&self) -> i64 {
        self.resp.latency_in_us
    }

    pub fn get_latency_in_ms(&self) -> i64 {
        self.resp.latency_in_us / 1000
    }

    pub fn get_space_name(&self) -> Option<String> {
        if let Some(v) = self.resp.space_name.clone() {
            Some(String::from_utf8(v).unwrap())
        } else {
            None
        }
    }

    pub fn get_error_msg(&self) -> Option<String> {
        if let Some(v) = self.resp.error_msg.clone() {
            Some(String::from_utf8(v).unwrap())
        } else {
            None
        }
    }

    pub fn is_set_plan_desc(&self) -> bool {
        self.resp.plan_desc.is_some()
    }

    pub fn get_plan_desc(&self) -> &Option<PlanDescription> {
        &self.resp.plan_desc
    }

    pub fn is_set_comment(&self) -> bool {
        self.resp.comment.is_some()
    }

    pub fn get_comment(&self) -> Option<String> {
        if let Some(v) = self.resp.comment.clone() {
            Some(String::from_utf8(v).unwrap())
        } else {
            None
        }
    }

    pub fn is_succeed(&self) -> bool {
        self.get_error_code() == ErrorCode::SUCCEEDED
    }

    pub fn is_partial_succeed(&self) -> bool {
        self.get_error_code() == ErrorCode::E_PARTIAL_SUCCEEDED
    }
}

dataset_wrapper_proxy!(GraphQueryOutput);

//
//
//
#[derive(Debug)]
pub enum GraphQueryError {
    ExecuteError(ExecuteError),
    ResponseError(ErrorCode, Option<Vec<u8>>),
    DataDeserializeError(DataDeserializeError),
}

impl core::fmt::Display for GraphQueryError {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Self::ExecuteError(err) => write!(f, "ExecuteError {err}"),
            Self::ResponseError(err_code, err_msg) => {
                write!(f, "ResponseError err_code:{err_code} err_msg:{err_msg:?}",)
            }
            Self::DataDeserializeError(err) => write!(f, "DataDeserializeError {err}"),
        }
    }
}

impl std::error::Error for GraphQueryError {}

//
//
//

const STMT_SHOW_HOSTS: &str = "SHOW HOSTS;";
#[derive(Deserialize, Debug)]
pub struct Host {
    #[serde(rename(deserialize = "Host"))]
    pub host: String,
    #[serde(rename(deserialize = "Port"))]
    pub port: u16,
    #[serde(rename(deserialize = "Status"))]
    pub status: String,
    #[serde(rename(deserialize = "Leader count"))]
    pub leader_count: u64,
    #[serde(rename(deserialize = "Leader distribution"))]
    pub leader_distribution: String,
    #[serde(rename(deserialize = "Partition distribution"))]
    pub partition_distribution: String,
    #[serde(rename(deserialize = "Version"))]
    pub version: String,
}

const STMT_SHOW_SPACES: &str = "SHOW SPACES;";
#[derive(Deserialize, Debug)]
pub struct Space {
    #[serde(rename(deserialize = "Name"))]
    pub name: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::{Error as IoError, ErrorKind as IoErrorKind};

    #[test]
    fn impl_std_fmt_display() {
        let err = GraphQueryError::ResponseError(ErrorCode::E_DISCONNECTED, None);
        println!("{err}");
    }

    #[test]
    fn impl_std_error_error() {
        let err = IoError::new(
            IoErrorKind::Other,
            GraphQueryError::ResponseError(ErrorCode::E_DISCONNECTED, None),
        );
        println!("{err}");
    }
}
