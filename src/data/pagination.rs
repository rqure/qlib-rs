use serde::{Deserialize, Serialize};

/// Pagination options for retrieving lists of items
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageOpts {
    /// The maximum number of items to return
    pub limit: usize,
    /// The starting point for pagination
    pub cursor: Option<String>,
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
    pub fn new(limit: usize, cursor: Option<String>) -> Self {
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
    pub next_cursor: Option<String>,
}

impl<T> PageResult<T> {
    pub fn new(items: Vec<T>, total: usize, next_cursor: Option<String>) -> Self {
        PageResult {
            items,
            total,
            next_cursor,
        }
    }
}
