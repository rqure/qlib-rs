use cel::{Context, Program};
use lru::LruCache;
use std::num::NonZeroUsize;

use crate::{to_base64, EntityId, IndirectFieldType, Result, StoreTrait, Value, INDIRECTION_DELIMITER};

/// CelExecutor with LRU cache for compiled CEL programs
#[derive(Debug)]
pub struct CelExecutor {
    cache: LruCache<String, Program>,
}

impl CelExecutor {
    pub fn new() -> Self {
        Self::with_capacity(100) // Default capacity of 100 compiled programs
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            cache: LruCache::new(NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::new(1).unwrap())),
        }
    }

    pub fn remove(&mut self, source: &str) {
        self.cache.pop(source);
    }

    /// Get the current cache size
    pub fn cache_size(&self) -> usize {
        self.cache.len()
    }

    /// Get the cache capacity
    pub fn cache_capacity(&self) -> usize {
        self.cache.cap().get()
    }

    /// Clear the cache completely
    pub fn clear_cache(&mut self) {
        self.cache.clear();
    }

    /// Resize the cache capacity
    pub fn resize_cache(&mut self, new_capacity: usize) {
        if let Some(capacity) = NonZeroUsize::new(new_capacity) {
            self.cache.resize(capacity);
        }
    }

    pub fn get_or_compile(&mut self, source: &str) -> Result<&Program> {
        // Check if already in cache (this will mark it as recently used)
        if self.cache.contains(source) {
            return Ok(self.cache.get(source).unwrap());
        }

        // Not in cache, compile it
        let program = Program::compile(source)
            .map_err(|e| crate::Error::ExecutionError(e.to_string()))?;
        
        // Insert into cache (LRU will handle eviction if needed)
        self.cache.put(source.to_string(), program);
        
        // Get the reference to the newly inserted program
        Ok(self.cache.get(source).unwrap())
    }

    pub fn execute(&mut self, source: &str, relative_id: EntityId, store: &impl StoreTrait) -> Result<cel::Value> {
        let program = self.get_or_compile(source.replace(INDIRECTION_DELIMITER, "_").as_str())?;
        let mut context = Context::default();
        let references = program.references();
        let fields = references.variables();

        for field in fields {
            // Convert underscore to indirection delimiter for store reading
            let store_field = field.to_string().replace("_", INDIRECTION_DELIMITER);
            
            // Parse indirection: split by delimiter and convert each part to FieldType
            let field_types: Result<IndirectFieldType> = store_field
                .split(INDIRECTION_DELIMITER)
                .map(|field_name| store.get_field_type(field_name))
                .collect();
            let field_types = field_types?;
            
            let (value, _, _) = store.read(relative_id, &field_types)?;
            // Use the original field name for CEL context (keep underscores)
            let cel_field = field.to_string();

            match value {
                Value::Blob(v) => {
                    context.add_variable_from_value(cel_field, to_base64(v.to_vec()));
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
                            // EntityId no longer implements Display, so we use debug formatting
                            // or convert to a meaningful string representation
                            context.add_variable_from_value(cel_field.clone(), format!("{:?}", e));
                        },
                        None => {
                            context.add_variable_from_value(cel_field.clone(), "");
                        }
                    }
                },
                Value::EntityList(v) => {
                    let list: Vec<String> = v.iter().map(|e| format!("{:?}", e)).collect();
                    context.add_variable_from_value(cel_field.clone(), list);
                },
                Value::Float(v) => {
                    context.add_variable_from_value(cel_field.clone(), v);
                },
                Value::String(v) => {
                    context.add_variable_from_value(cel_field.clone(), v.as_str());
                },
                Value::Timestamp(v) => {
                    // Convert time::OffsetDateTime to chrono::DateTime<chrono::FixedOffset>
                    let unix_timestamp = v.unix_timestamp();
                    let nanoseconds = v.nanosecond();
                    let datetime = chrono::DateTime::from_timestamp(
                        unix_timestamp,
                        nanoseconds
                    ).ok_or_else(|| crate::Error::ExecutionError("Failed to convert timestamp".to_string()))?
                        .with_timezone(&chrono::FixedOffset::east_opt(0).unwrap());
                    context.add_variable_from_value(cel_field.clone(), datetime);
                },
                Value::Int(v) => {
                    context.add_variable_from_value(cel_field.clone(), v);
                },
            }
        }

        context.add_variable_from_value("EntityId", format!("{:?}", relative_id));

        context.add_variable_from_value("EntityType", store.resolve_entity_type(relative_id.extract_type()).unwrap_or_else(|_| format!("{:?}", relative_id.extract_type())));

        match program.execute(&context) {
            Ok(v) => Ok(v),
            Err(e) => Err(crate::Error::ExecutionError(e.to_string()))
        }
    }
}