use std::collections::HashMap;
use cel::{Context, Program};

use crate::{sread, to_base64, EntityId, FieldType, Result, Store, Value, INDIRECTION_DELIMITER};

#[derive(Debug)]
pub struct CelExecutor {
    cache: HashMap<String, Program>
}

impl CelExecutor {
    pub fn new() -> Self {
        Self {
            cache: HashMap::new()
        }
    }

    pub fn remove(&mut self, source: &str) {
        self.cache.remove(source);
    }

    pub fn get_or_compile(&mut self, source: &str) -> Result<&Program> {
        if !self.cache.contains_key(source) {
            let program = Program::compile(source)
                .map_err(|e| crate::Error::ExecutionError(e.to_string()))?;
            self.cache.insert(source.to_string(), program);
        }

        Ok(self.cache.get(source).unwrap())
    }

    pub fn execute(&mut self, source: &str, relative_id: &EntityId, store: &mut Store) -> Result<cel::Value> {
        let program = self.get_or_compile(source.replace(INDIRECTION_DELIMITER, "_").as_str())?;
        let mut context = Context::default();
        let references = program.references();
        let fields = references.variables();

        for field in fields {
            // Convert underscore to indirection delimiter for store reading
            let store_field = field.to_string().replace("_", INDIRECTION_DELIMITER);
            let mut reqs = vec![
                sread!(relative_id.clone(), FieldType::from(store_field))
            ];
            store.perform(&mut reqs)?;
            let value = reqs.first().unwrap().value().unwrap().clone();
            // Use the original field name for CEL context (keep underscores)
            let cel_field = field.to_string();

            match value {
                Value::Blob(v) => {
                    context.add_variable_from_value(cel_field, to_base64(v.clone()));
                },
                Value::Bool(v) => {
                    context.add_variable_from_value(cel_field, v);
                },
                Value::Choice(v) => {
                    context.add_variable_from_value(cel_field, v);
                },
                Value::EntityReference(v) => {
                    match v {
                        Some(e) => {
                            context.add_variable_from_value(cel_field.clone(), e.to_string());
                        },
                        None => {
                            context.add_variable_from_value(cel_field.clone(), "");
                        }
                    }
                },
                Value::EntityList(v) => {
                    let list: Vec<String> = v.iter().map(|e| e.to_string()).collect();
                    context.add_variable_from_value(cel_field.clone(), list);
                },
                Value::Float(v) => {
                    context.add_variable_from_value(cel_field.clone(), v);
                },
                Value::String(v) => {
                    context.add_variable_from_value(cel_field.clone(), v.as_str());
                },
                Value::Timestamp(v) => {
                    // Convert SystemTime to chrono::DateTime<chrono::FixedOffset>
                    let duration_since_epoch = v.duration_since(std::time::UNIX_EPOCH)
                        .map_err(|e| crate::Error::ExecutionError(format!("Invalid timestamp: {}", e)))?;
                    let datetime = chrono::DateTime::from_timestamp(
                        duration_since_epoch.as_secs() as i64,
                        duration_since_epoch.subsec_nanos()
                    ).ok_or_else(|| crate::Error::ExecutionError("Failed to convert timestamp".to_string()))?
                        .with_timezone(&chrono::FixedOffset::east_opt(0).unwrap());
                    context.add_variable_from_value(cel_field.clone(), datetime);
                },
                Value::Int(v) => {
                    context.add_variable_from_value(cel_field.clone(), v);
                },
            }
        }

        context.add_variable_from_value("EntityId", relative_id.to_string());

        context.add_variable_from_value("EntityType", relative_id.get_type().to_string());

        match program.execute(&context) {
            Ok(v) => Ok(v),
            Err(e) => Err(crate::Error::ExecutionError(e.to_string()))
        }
    }
}