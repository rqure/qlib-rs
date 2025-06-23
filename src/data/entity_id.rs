use crate::data::EntityType;
use serde::{Deserialize, Serialize};


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EntityId {
    typ: EntityType,
    id: u64,
}

impl EntityId {
    const SEPARATOR: &str = "$";

    pub fn new(typ: &str, id: u64) -> Self {
        EntityId {
            typ: typ.into(),
            id: id.clone(),
        }
    }

    pub fn get_type(&self) -> &EntityType {
        &self.typ
    }

    pub fn get_id(&self) -> String {
        format!("{}{}{}", self.get_type(), EntityId::SEPARATOR, self.id)
    }
}

impl TryFrom<&str> for EntityId {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let parts: Vec<&str> = value.split(&EntityId::SEPARATOR).collect();

        if parts.len() != 2 {
            return Err("Invalid EntityId format, expected 'type&id'".to_string());
        }

        let typ = parts[0].to_string();
        let id = parts[1].parse::<u64>().map_err(|e| format!("Invalid id: {}", e))?;

        Ok(EntityId { typ, id })
    }
}

impl Into<String> for EntityId {
    fn into(self) -> String {
        self.get_id()
    }
}

impl std::fmt::Display for EntityId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get_id())
    }
}