use core::fmt;
use std::collections::HashMap;

use crate::common::{DataSet, Row};
use crate::data_deserializer::{DataDeserializeError, DataDeserializer};
use serde::de::DeserializeOwned;

use crate::value_wrapper::{gen_val_wraps, ValueWrapper};
use crate::TimezoneInfo;

#[derive(Debug)]
pub struct DataSetWrapper {
    dataset: DataSet,
    col_name_index_map: HashMap<Vec<u8>, usize>,
    timezone_info: TimezoneInfo,
}

#[derive(Debug)]
pub struct Record<'a> {
    column_names: &'a Vec<Vec<u8>>,
    records: Vec<ValueWrapper<'a>>,
    col_name_index_map: &'a HashMap<Vec<u8>, usize>,
    timezone_info: &'a TimezoneInfo,
}

impl DataSetWrapper {
    pub(crate) fn new(dataset: DataSet, timezone_info: TimezoneInfo) -> Self {
        let mut col_name_index_map: HashMap<Vec<u8>, usize> = Default::default();
        dataset
            .column_names
            .iter()
            .enumerate()
            .for_each(|(i, name)| {
                col_name_index_map.insert(name.to_vec(), i);
            });
        Self {
            dataset,
            col_name_index_map,
            timezone_info,
        }
    }
}

impl DataSetWrapper {
    /// Returns a 2D array of strings representing the query result
    /// # example
    /// +-------------+------------+------------+-----------+-----------------+---------------+
    /// |serve._src   |serve._type |serve._rank |serve._dst |serve.start_year |serve.end_year |
    /// +-------------+------------+------------+-----------+-----------------+---------------+
    /// |"player112"  |5           |0           |"team204"  |2015             |2017           |
    /// |"player112"  |5           |0           |"team219"  |2019             |2019           |
    /// +-------------+------------+------------+-----------+-----------------+---------------+
    /// The data above will be returned as
    /// vec![
    ///     vec!["serve._s rc", "serve._type", "serve._rank", "serve._dst", "serve.start_year", "serve.end_year"]
    ///     vec!["player112", 5, 0, "team204", 2015, 2017]
    ///     vec!["player112", 5, 0, "team219", 2019, 2019]
    /// ]
    pub fn as_string_table(&self) -> Vec<Vec<String>> {
        let mut res_table = vec![];
        let col_names = self
            .get_col_names()
            .iter()
            .map(|v| String::from_utf8(v.to_vec()).unwrap())
            .collect();
        res_table.push(col_names);
        let rows = self.get_rows();
        let mut rows_table = rows
            .iter()
            .map(|row| {
                let temp_row = row
                    .values
                    .iter()
                    .map(|v| ValueWrapper::new(v, &self.timezone_info).to_string())
                    .collect();
                temp_row
            })
            .collect();
        res_table.append(&mut rows_table);
        res_table
    }

    // Returns all values in the given column
    pub fn get_values_by_col_name(&self, col_name: &str) -> Result<Vec<ValueWrapper>, ()> {
        if !self.has_col_name(col_name) {
            return Err(());
        }
        let col_name = col_name.as_bytes().to_vec();
        let index = self.col_name_index_map[&col_name];
        let rows = self.get_rows();
        let val_list = rows
            .iter()
            .map(|row| ValueWrapper::new(&row.values[index], &self.timezone_info))
            .collect();
        Ok(val_list)
    }

    pub fn get_row_values_by_index<'a>(&'a self, index: usize) -> Result<Record<'a>, ()> {
        if index >= self.get_row_size() {
            return Err(());
        }
        let rows = self.get_rows();
        let val_wrap = gen_val_wraps(&rows[index as usize], &self.timezone_info)?;
        Ok(Record {
            column_names: &self.get_col_names(),
            records: val_wrap,
            col_name_index_map: &self.col_name_index_map,
            timezone_info: &self.timezone_info,
        })
    }

    pub fn scan<D>(&self) -> Result<Vec<D>, DataDeserializeError>
    where
        D: DeserializeOwned,
    {
        let mut data_set = vec![];
        if self.is_empty() {
            return Ok(data_set);
        }
        let names = self.get_col_names();
        let rows = self.get_rows();
        for row in rows.iter() {
            let mut data_deserializer = DataDeserializer::new(names, &row.values);
            let data = D::deserialize(&mut data_deserializer)?;
            data_set.push(data);
        }
        Ok(data_set)
    }
}

impl DataSetWrapper {
    pub fn get_row_size(&self) -> usize {
        self.get_rows().len()
    }

    pub fn get_col_size(&self) -> usize {
        self.get_col_names().len()
    }

    pub fn get_rows(&self) -> &Vec<Row> {
        &self.dataset.rows
    }

    pub fn get_col_names(&self) -> &Vec<Vec<u8>> {
        &self.dataset.column_names
    }

    pub fn is_empty(&self) -> bool {
        self.get_row_size() == 0
    }

    fn has_col_name(&self, col_name: &str) -> bool {
        let col_name = col_name.as_bytes().to_vec();
        self.col_name_index_map.contains_key(&col_name)
    }
}

impl fmt::Display for DataSetWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let table = self.as_string_table();
        let mut column_widths = vec![0; table[0].len()];
        for row in &table {
            for (i, cell) in row.iter().enumerate() {
                let adjusted_width = match i {
                    0 => cell.len() + 1,
                    _ => cell.len(),
                } + 2;
                column_widths[i] = column_widths[i].max(adjusted_width);
            }
        }

        let top_border = "+".to_string()
            + &column_widths
                .iter()
                .map(|&w| format!("{}+", "-".repeat(w - 1)))
                .collect::<Vec<String>>()
                .join("");

        let header_row: &Vec<String> = &table[0]
            .iter()
            .enumerate()
            .map(|(i, cell)| format!("{:width$}|", cell, width = column_widths[i] - 1))
            .collect();

        let separator = "+".to_string()
            + &column_widths
                .iter()
                .map(|&w| format!("{}+", "-".repeat(w - 1)))
                .collect::<Vec<String>>()
                .join("");

        let mut data_rows = String::new();
        for row in &table[1..] {
            let data_row: Vec<String> = row
                .iter()
                .enumerate()
                .map(|(i, cell)| format!("{:width$}|", cell, width = column_widths[i] - 1))
                .collect();
            data_rows.push_str(&("|".to_string() + &data_row.join("") + &"\n".to_string()));
        }

        let table_str = format!(
            "{}\n{}\n{}\n{}{}",
            top_border,
            "|".to_string() + &header_row.join(""),
            separator,
            data_rows,
            top_border
        );

        write!(f, "{}", table_str)
    }
}

#[macro_export]
macro_rules! dataset_wrapper_proxy {
    ($type_name:ident) => {
        impl $type_name {
            pub fn dataset(&self) -> Option<&DataSetWrapper> {
                self.data_set.as_ref()
            }
            pub fn mut_dataset(&mut self) -> Option<&mut DataSetWrapper> {
                self.data_set.as_mut()
            }

            // Returns a 2D array of strings representing the query result
            // If resultSet.resp.data is nil, returns an empty 2D array
            pub fn as_string_table(&self) -> Option<Vec<Vec<String>>> {
                self.dataset().map(|v| v.as_string_table())
            }

            // Returns all values in the given column
            pub fn get_values_by_col_name(&self, col_name: &str) -> Result<Vec<ValueWrapper>, ()> {
                if let Some(data_set) = self.dataset() {
                    data_set.get_values_by_col_name(col_name).map_err(|_| ())
                } else {
                    Err(())
                }
            }

            pub fn get_row_values_by_index(&self, index: usize) -> Result<Record, ()> {
                if let Some(data_set) = self.dataset() {
                    data_set.get_row_values_by_index(index).map_err(|_| ())
                } else {
                    Err(())
                }
            }

            pub fn scan<D>(&self) -> Result<Vec<D>, DataDeserializeError>
            where
                D: DeserializeOwned,
            {
                if let Some(data_set) = self.dataset() {
                    data_set.scan::<D>()
                } else {
                    Err(DataDeserializeError {
                        field: None,
                        kind: crate::data_deserializer::DataDeserializeErrorKind::Custom(
                            "DataSet doesn't exist!".to_string(),
                        ),
                    })
                }
            }

            pub fn get_row_size(&self) -> usize {
                self.dataset().map_or(0, |v| v.get_row_size())
            }

            pub fn get_col_size(&self) -> usize {
                self.dataset().map_or(0, |v| v.get_col_size())
            }

            pub fn get_rows(&self) -> Option<&Vec<Row>> {
                self.dataset().map(|v| v.get_rows())
            }

            pub fn get_col_names(&self) -> Option<&Vec<Vec<u8>>> {
                self.dataset().map(|v| v.get_col_names())
            }

            pub fn is_empty(&self) -> bool {
                self.get_row_size() == 0
            }
        }
    };
}

impl<'a> Record<'a> {
    pub fn get_value_by_index(&self, index: i32) -> Result<&ValueWrapper, ()> {
        if index < 0 || index as usize > self.records.len() {
            return Err(());
        }
        Ok(&self.records[index as usize])
    }

    pub fn get_value_by_col_name(&self, col_name: &str) -> Result<&ValueWrapper, ()> {
        if !self.has_col_name(col_name) {
            return Err(());
        }
        let col_name = col_name.as_bytes().to_vec();
        let index = self.col_name_index_map[&col_name];
        Ok(&self.records[index])
    }

    pub fn to_string(&self) -> String {
        let str_list: Vec<_> = self.records.iter().map(|v| v.to_string()).collect();
        str_list.join(", ")
    }

    fn has_col_name(&self, col_name: &str) -> bool {
        let col_name = col_name.as_bytes().to_vec();
        self.col_name_index_map.contains_key(&col_name)
    }
}
