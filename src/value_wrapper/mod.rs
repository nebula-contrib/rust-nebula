use std::collections::HashMap;

use crate::common::types::Value;
use crate::common::{Duration, Geography, NullType, Row};

use crate::data_deserializer::{DataDeserializeError, DataDeserializeErrorKind};
use crate::{DataSetError, TimezoneInfo};
use datetime::{DataTimeWrapper, DateWrapper, TimeWrapper};
use relationship::{Node, PathWrapper, Relationship};

pub mod datetime;
pub mod relationship;

fn new_conversion_error(from_type: String, to_type: String) -> DataSetError {
    DataSetError::DataDeserializeError(DataDeserializeError::new(
        None,
        DataDeserializeErrorKind::ConversionError(from_type, to_type),
    ))
}

#[derive(Debug)]
pub struct ValueWrapper<'a> {
    value: &'a Value,
    #[allow(dead_code)]
    timezone_info: &'a TimezoneInfo,
}

impl<'a> ValueWrapper<'a> {
    pub fn new(val: &'a Value, timezone_info: &'a TimezoneInfo) -> Self {
        Self {
            value: &val,
            timezone_info,
        }
    }
}

pub fn gen_val_wraps<'a>(row: &'a Row, timezone_info: &'a TimezoneInfo) -> Vec<ValueWrapper<'a>> {
    row.values
        .iter()
        .map(|v| ValueWrapper::new(v, timezone_info))
        .collect()
}

impl<'a> ValueWrapper<'a> {
    pub fn is_empty(&self) -> bool {
        self.get_type() == "empty"
    }

    pub fn is_null(&self) -> bool {
        matches!(self.value, Value::nVal(_))
    }

    pub fn is_bool(&self) -> bool {
        matches!(self.value, Value::bVal(_))
    }

    pub fn is_int(&self) -> bool {
        matches!(self.value, Value::iVal(_))
    }

    pub fn is_float(&self) -> bool {
        matches!(self.value, Value::fVal(_))
    }

    pub fn is_string(&self) -> bool {
        matches!(self.value, Value::sVal(_))
    }

    pub fn is_time(&self) -> bool {
        matches!(self.value, Value::tVal(_))
    }

    pub fn is_date(&self) -> bool {
        matches!(self.value, Value::dVal(_))
    }

    pub fn is_datetime(&self) -> bool {
        matches!(self.value, Value::dtVal(_))
    }

    pub fn is_list(&self) -> bool {
        matches!(self.value, Value::lVal(_))
    }

    pub fn is_set(&self) -> bool {
        matches!(self.value, Value::uVal(_))
    }

    pub fn is_map(&self) -> bool {
        matches!(self.value, Value::mVal(_))
    }

    pub fn is_vertex(&self) -> bool {
        matches!(self.value, Value::vVal(_))
    }

    pub fn is_edge(&self) -> bool {
        matches!(self.value, Value::eVal(_))
    }

    pub fn is_path(&self) -> bool {
        matches!(self.value, Value::pVal(_))
    }

    pub fn is_geography(&self) -> bool {
        matches!(self.value, Value::ggVal(_))
    }

    pub fn is_duration(&self) -> bool {
        matches!(self.value, Value::duVal(_))
    }
}

impl<'a> ValueWrapper<'a> {
    pub fn as_null(&self) -> Result<&NullType, DataSetError> {
        if let Value::nVal(v) = self.value {
            Ok(v)
        } else {
            Err(new_conversion_error(
                self.get_type().to_string(),
                "Null".to_string(),
            ))
        }
    }

    pub fn as_bool(&self) -> Result<&bool, DataSetError> {
        if let Value::bVal(v) = self.value {
            Ok(v)
        } else {
            Err(new_conversion_error(
                self.get_type().to_string(),
                "bool".to_string(),
            ))
        }
    }

    pub fn as_int(&self) -> Result<&i64, DataSetError> {
        if let Value::iVal(v) = self.value {
            Ok(v)
        } else {
            Err(new_conversion_error(
                self.get_type().to_string(),
                "int".to_string(),
            ))
        }
    }

    pub fn as_float(&self) -> Result<f64, DataSetError> {
        if let Value::fVal(v) = self.value {
            Ok(v.0)
        } else {
            Err(new_conversion_error(
                self.get_type().to_string(),
                "float".to_string(),
            ))
        }
    }

    pub fn as_string(&self) -> Result<String, DataSetError> {
        if let Value::sVal(v) = self.value {
            Ok(String::from_utf8(v.to_vec()).unwrap())
        } else {
            Err(new_conversion_error(
                self.get_type().to_string(),
                "string".to_string(),
            ))
        }
    }

    pub fn as_time(&self) -> Result<TimeWrapper, DataSetError> {
        todo!("Implement conversion to TimeWrapper")
    }

    pub fn as_date(&self) -> Result<DateWrapper, DataSetError> {
        todo!("Implement conversion to DateWrapper")
    }

    pub fn as_date_time(&self) -> Result<DataTimeWrapper, DataSetError> {
        todo!("Implement conversion to DateTimeWrapper")
    }

    pub fn as_list(&self) -> Result<Vec<ValueWrapper>, DataSetError> {
        todo!("Implement conversion to Vec<ValueWrapper>")
    }

    /// as_dedup_list converts the ValueWrapper to a slice of ValueWrapper that has unique elements
    pub fn as_dedup_list(&self) -> Result<Vec<ValueWrapper>, DataSetError> {
        todo!("Implement conversion to deduped Vec<ValueWrapper>")
    }

    pub fn as_map(&self) -> Result<HashMap<String, ValueWrapper>, DataSetError> {
        todo!("Implement conversion to HashMap<String, ValueWrapper>")
    }

    pub fn as_node(&self) -> Result<Node, DataSetError> {
        todo!("Implement conversion to Node")
    }

    pub fn as_relationship(&self) -> Result<Relationship, DataSetError> {
        todo!("Implement conversion to Relationship")
    }

    pub fn as_path(&self) -> Result<PathWrapper, DataSetError> {
        todo!("Implement conversion to PathWrapper")
    }

    pub fn as_geography(&self) -> Result<Geography, DataSetError> {
        todo!("Implement conversion to nebula::Geography")
    }

    pub fn as_duration(&self) -> Result<Duration, DataSetError> {
        todo!("Implement conversion to nebula::Duration")
    }
}

impl<'a> ValueWrapper<'a> {
    pub fn get_type(&self) -> &str {
        match self.value {
            Value::nVal(_) => "null",
            Value::bVal(_) => "bool",
            Value::iVal(_) => "int",
            Value::fVal(_) => "float",
            Value::sVal(_) => "string",
            Value::dVal(_) => "date",
            Value::tVal(_) => "time",
            Value::dtVal(_) => "datetime",
            Value::vVal(_) => "vertex",
            Value::eVal(_) => "edge",
            Value::pVal(_) => "path",
            Value::lVal(_) => "list",
            Value::mVal(_) => "map",
            Value::uVal(_) => "set",
            Value::ggVal(_) => "geography",
            Value::duVal(_) => "duration",
            _ => "empty",
        }
    }

    pub fn to_string(&self) -> String {
        match self.value {
            Value::nVal(v) => v.to_string(),
            Value::bVal(v) => v.to_string(),
            Value::iVal(v) => v.to_string(),
            Value::fVal(v) => v.0.to_string(),
            Value::sVal(v) => {
                let mut s = String::from('"');
                s.extend(String::from_utf8(v.to_vec()));
                s.push('"');
                s
            }
            Value::dVal(v) => format!("{:04}-{:02}-{:02}", v.year, v.month, v.day),
            Value::tVal(v) => format!(
                "{:02}:{:02}:{:02}.{:06}",
                v.hour, v.minute, v.sec, v.microsec
            ),
            Value::dtVal(v) => format!(
                "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
                v.year, v.month, v.day, v.hour, v.minute, v.sec, v.microsec
            ),
            Value::vVal(_) => todo!(),
            Value::eVal(_) => todo!(),
            Value::pVal(_) => todo!(),
            Value::lVal(_) => todo!(),
            Value::mVal(_) => todo!(),
            Value::uVal(_) => todo!(),
            Value::ggVal(_) => todo!(),
            Value::duVal(v) => format!(
                "{} months, {} seconds, {} microseconds",
                v.months, v.seconds, v.microseconds
            ),
            _ => "".to_string(),
        }
    }
}

fn to_wkt(geo: Geography) -> String {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_TIMEZONE: &str = "UTC";

    #[test]
    fn test_is_empty() {
        todo!("Implement test for is_empty method");
    }

    #[test]
    fn test_as_null() {
        todo!("Implement test for as_null method");
    }

    #[test]
    fn test_as_bool() {
        todo!("Implement test for as_bool method");
    }

    #[test]
    fn test_as_int() {
        todo!("Implement test for as_int method");
    }

    #[test]
    fn test_as_float() {
        todo!("Implement test for as_float method");
    }

    #[test]
    fn test_as_string() {
        todo!("Implement test for as_string method");
    }

    #[test]
    fn test_as_list() {
        todo!("Implement test for as_list method");
    }

    #[test]
    fn test_as_dedup_list() {
        todo!("Implement test for as_dedup_list method");
    }

    #[test]
    fn test_as_map() {
        todo!("Implement test for as_map method");
    }

    #[test]
    fn test_as_date() {
        todo!("Implement test for as_date method");
    }

    #[test]
    fn test_as_time() {
        todo!("Implement test for as_time method");
    }

    #[test]
    fn test_as_datetime() {
        todo!("Implement test for as_datetime method");
    }

    #[test]
    fn test_as_node() {
        todo!("Implement test for as_node method");
    }

    #[test]
    fn test_as_relationship() {
        todo!("Implement test for as_relationship method");
    }

    #[test]
    fn test_as_pathwrapper() {
        todo!("Implement test for as_path method");
    }

    #[test]
    fn test_as_geography() {
        todo!("Implement test for as_geography method");
    }

    #[test]
    fn test_as_duration() {
        todo!("Implement test for as_duration method");
    }
}
