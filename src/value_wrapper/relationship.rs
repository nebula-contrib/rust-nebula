use std::collections::HashMap;

use crate::common::{Edge, Path, Vertex};
use crate::TimezoneInfo;

pub struct Node {
    vertex: Vertex,
    tags: Vec<String>,
    tag_name_index_map: HashMap<String, i32>,
    timezone_info: TimezoneInfo,
}

pub struct Relationship {
    edge: Edge,
    timezone_info: TimezoneInfo,
}

struct Segment<'a> {
    start_node: &'a Node,
    relationship: &'a Relationship,
    end_node: &'a Node,
}

pub struct PathWrapper<'a> {
    path: Path,
    node_list: Vec<&'a Node>,
    relationship_list: Vec<&'a Relationship>,
    segments: Vec<Segment<'a>>,
    timezone_info: TimezoneInfo,
}
