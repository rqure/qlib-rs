use serde::{Deserialize, Serialize};
use qlib_rs_derive::{RespDecode, RespEncode};

use crate::{data::{resp::RespDecode as RespDecodeT, EntityId, FieldType, Timestamp, Value}};

#[derive(Debug, Clone, Serialize, Deserialize, RespEncode, RespDecode)]
pub struct Field {
    pub field_type: FieldType,
    pub value: Value,
    pub write_time: Timestamp,
    pub writer_id: Option<EntityId>,
}
