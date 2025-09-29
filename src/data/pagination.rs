use serde::{Deserialize, Serialize};
use qlib_rs_derive::{RespDecode, RespEncode};
use crate::data::resp::RespDecode as RespDecodeT;

/// Pagination options for retrieving lists of items
#[derive(Debug, Clone, Serialize, Deserialize, RespEncode, RespDecode)]
pub struct PageOpts {
    /// The maximum number of items to return
    pub limit: usize,
    /// The starting point for pagination
    pub cursor: Option<usize>,
}

impl Default for PageOpts {
    fn default() -> Self {
        PageOpts {
            limit: 100,
            cursor: None,
        }
    }
}

impl PageOpts {
    pub fn new(limit: usize, cursor: Option<usize>) -> Self {
        PageOpts { limit, cursor }
    }
}

/// Result of a paginated query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageResult<T> {
    /// The items returned in this page
    pub items: Vec<T>,
    /// The total number of items available
    pub total: usize,
    /// Cursor for retrieving the next page, if available
    pub next_cursor: Option<usize>,
}

impl<T> PageResult<T> {
    pub fn new(items: Vec<T>, total: usize, next_cursor: Option<usize>) -> Self {
        PageResult {
            items,
            total,
            next_cursor,
        }
    }
}
