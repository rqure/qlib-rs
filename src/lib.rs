mod data;

pub use data::{Entity, EntitySchema, EntityId, Field, FieldSchema, Request, Snowflake, Value, MapStore, resolve_indirection, INDIRECTION_DELIMITER};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let snowflake = Snowflake::new();
        println!("{}", EntityId::new("Root", snowflake.generate()));

        let store = MapStore::new();
    }
}
