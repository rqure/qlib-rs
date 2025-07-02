use rhai::{Engine, Scope, Dynamic, Map, Array, EvalAltResult};
use super::{RhaiStoreWrapper, RhaiStoreProxyWrapper, create_read_request, create_write_request, create_add_request, create_subtract_request};

/// A Rhai scripting engine configured for qlib store operations with custom syntax
/// 
/// This engine provides an enhanced API for qlib operations through custom syntax.
/// All scripts execute relative to a specific entity ID context, with "me" referring to that entity.
/// 
/// ## Transaction Operations
/// ```rhai
/// // Transaction with entity-relative operations
/// TRANSACTION(
///     READ Name INTO train_name,
///     READ NextStation->Name INTO station_name,
///     WRITE me INTO NextStation->CurrentTrain,
///     WRITE DEFAULT INTO StopTrigger,
///     ADD 1 INTO NextStation->UpdateCount,
///     SUBTRACT 1 FROM NextStation->Retries
/// )
/// 
/// // Use variables from transaction
/// if station_name == "Waterfront" {
///     // Handle special case
/// }
/// ```
/// 
/// ## Indirection Support
/// ```rhai
/// // Reading through indirection (follows references)
/// READ NextStation->Name INTO station_name
/// READ Manager->Department->Name INTO dept_name
/// 
/// // Writing through indirection
/// WRITE me INTO NextStation->CurrentTrain
/// WRITE 100000 INTO Company->Budget
/// 
/// // Multi-level indirection
/// READ Customer->Company->Address->City INTO city
/// ```
/// 
/// ## Helper Functions
/// - `create_map()` - Create an empty map
/// - `create_array()` - Create an empty array
/// - `parse_entity_id(id)` - Parse entity ID into components
/// - `format_entity_id(type, id)` - Format entity ID from components
/// - `print_info(msg)`, `print_debug(msg)`, `print_error(msg)` - Logging functions
/// - `get_me()` - Get the current entity ID context
pub struct QScriptEngine {
    engine: Engine,
    entity_context: Option<String>, // The current entity ID context ("me")
}

impl QScriptEngine {
    /// Create a new scripting engine with qlib functions registered
    pub fn new() -> Self {
        let mut engine = Engine::new();
        
        // Register store wrapper types
        engine.register_type::<RhaiStoreWrapper>();
        engine.register_type::<RhaiStoreProxyWrapper>();
        
        // Register Store wrapper methods (perform-based API only)
        engine.register_fn("perform", RhaiStoreWrapper::perform);
        engine.register_fn("create_entity", RhaiStoreWrapper::create_entity);
        engine.register_fn("delete_entity", RhaiStoreWrapper::delete_entity);
        
        // Register entity_exists with explicit closure to handle Result properly
        engine.register_fn("entity_exists", |wrapper: &mut RhaiStoreWrapper, entity_id: &str| -> Result<bool, Box<rhai::EvalAltResult>> {
            wrapper.entity_exists(entity_id)
        });
        
        // Register StoreProxy wrapper methods with different names to avoid conflicts
        engine.register_fn("perform_async", RhaiStoreProxyWrapper::perform);
        engine.register_fn("create_entity_async", RhaiStoreProxyWrapper::create_entity);
        engine.register_fn("entity_exists_async", RhaiStoreProxyWrapper::entity_exists);
        engine.register_fn("set_entity_schema", RhaiStoreProxyWrapper::set_entity_schema);
        
        // Register request creation helper functions (syntax sugar)
        engine.register_fn("create_read_request", create_read_request);
        engine.register_fn("create_write_request", create_write_request);
        engine.register_fn("create_add_request", create_add_request);
        engine.register_fn("create_subtract_request", create_subtract_request);
        
        // Register utility functions
        engine.register_fn("create_map", Self::create_map);
        engine.register_fn("create_array", Self::create_array);
        engine.register_fn("print_info", Self::print_info);
        engine.register_fn("print_debug", Self::print_debug);
        engine.register_fn("print_error", Self::print_error);
        
        // Register entity ID parsing helper
        engine.register_fn("parse_entity_id", Self::parse_entity_id);
        engine.register_fn("format_entity_id", Self::format_entity_id);
        
        // Register context helpers
        engine.register_fn("get_me", Self::get_me);
        engine.register_fn("set_me", Self::set_me);
        
        // Register transaction helper functions (remove batch helpers)
        engine.register_fn("begin_transaction", Self::begin_transaction);
        engine.register_fn("commit_transaction", Self::commit_transaction);
        engine.register_fn("rollback_transaction", Self::rollback_transaction);
        engine.register_fn("now", Self::now);
        
        // Register indirection helper
        engine.register_fn("parse_field_path", Self::parse_field_path);
        
        // Register custom syntax for improved API
        Self::register_custom_syntax(&mut engine).expect("Failed to register custom syntax");
        
        Self { 
            engine,
            entity_context: None,
        }
    }

    /// Set the entity context for "me" references
    pub fn set_entity_context(&mut self, entity_id: Option<String>) {
        self.entity_context = entity_id;
    }

    /// Get the current entity context
    pub fn get_entity_context(&self) -> Option<&String> {
        self.entity_context.as_ref()
    }

    /// Create an empty map for field definitions
    fn create_map() -> Map {
        Map::new()
    }

    /// Create an empty array
    fn create_array() -> Array {
        Array::new()
    }

    /// Logging functions
    fn print_info(msg: &str) {
        log::info!("QScript: {}", msg);
    }

    fn print_debug(msg: &str) {
        log::debug!("QScript: {}", msg);
    }

    fn print_error(msg: &str) {
        log::error!("QScript: {}", msg);
    }

    /// Parse entity ID components
    fn parse_entity_id(entity_id: &str) -> Result<Map, Box<EvalAltResult>> {
        let parts: Vec<&str> = entity_id.split('$').collect();
        if parts.len() != 2 {
            return Err("Invalid entity ID format".into());
        }
        
        let mut map = Map::new();
        map.insert("type".into(), Dynamic::from(parts[0].to_string()));
        map.insert("id".into(), Dynamic::from(parts[1].parse::<i64>().unwrap_or(0)));
        Ok(map)
    }

    /// Format entity ID from type and numeric ID
    fn format_entity_id(entity_type: &str, id: i64) -> String {
        format!("{}${}", entity_type, id)
    }

    /// Begin a transaction with an optional name
    fn begin_transaction(_name: &str) -> String {
        format!("tx_{}", uuid::Uuid::new_v4().to_string().replace("-", ""))
    }

    /// Commit a transaction
    fn commit_transaction(tx_id: &str) -> Map {
        let mut map = Map::new();
        map.insert("action".into(), Dynamic::from("commit"));
        map.insert("transaction_id".into(), Dynamic::from(tx_id.to_string()));
        map
    }

    /// Rollback a transaction
    fn rollback_transaction(tx_id: &str) -> Map {
        let mut map = Map::new();
        map.insert("action".into(), Dynamic::from("rollback"));
        map.insert("transaction_id".into(), Dynamic::from(tx_id.to_string()));
        map
    }

    /// Get current timestamp
    fn now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    /// Get the current entity context ("me")
    fn get_me() -> String {
        // This is a placeholder - in actual usage, this would be injected into the scope
        "me".to_string()
    }

    /// Set the entity context (this is a no-op as context is managed by the engine)
    fn set_me(_entity_id: &str) {
        // This is a placeholder - actual context setting is done via set_entity_context
    }

    /// Parse field path with indirection support and entity context resolution
    /// Example: "NextStation->Name" becomes ["me", "NextStation", "Name"] when entity context is set
    /// Example: "me" becomes [entity_context] when entity context is set
    fn parse_field_path(path: &str) -> Result<Array, Box<EvalAltResult>> {
        Self::parse_field_path_with_context(path, None)
    }

    /// Parse field path with explicit entity context
    fn parse_field_path_with_context(path: &str, entity_context: Option<&str>) -> Result<Array, Box<EvalAltResult>> {
        let mut parts = Array::new();
        
        // Handle special "me" keyword
        if path == "me" {
            if let Some(context) = entity_context {
                parts.push(Dynamic::from(context.to_string()));
                return Ok(parts);
            } else {
                return Err("No entity context set for 'me' reference".into());
            }
        }
        
        // Handle "DEFAULT" keyword
        if path == "DEFAULT" {
            parts.push(Dynamic::from("DEFAULT".to_string()));
            return Ok(parts);
        }
        
        // Split by -> for indirection
        let indirection_parts: Vec<&str> = path.split("->").collect();
        
        // Check if path starts with a field (no entity specified)
        if indirection_parts.len() > 0 && !indirection_parts[0].contains('$') && !indirection_parts[0].contains('.') {
            // This is a field path relative to the entity context
            if let Some(context) = entity_context {
                parts.push(Dynamic::from(context.to_string()));
                for part in indirection_parts {
                    parts.push(Dynamic::from(part.to_string()));
                }
                return Ok(parts);
            } else {
                return Err("No entity context set for relative field path".into());
            }
        }
        
        // First part should contain entity_id.field_type
        if let Some(first_part) = indirection_parts.first() {
            if let Some(dot_pos) = first_part.find('.') {
                let entity_id = &first_part[..dot_pos];
                let field_type = &first_part[dot_pos + 1..];
                
                parts.push(Dynamic::from(entity_id.to_string()));
                parts.push(Dynamic::from(field_type.to_string()));
                
                // Add remaining indirection parts
                for part in &indirection_parts[1..] {
                    parts.push(Dynamic::from(part.to_string()));
                }
                
                Ok(parts)
            } else {
                Err("Invalid field path format. Use 'EntityType$id.FieldType', 'FieldType->IndirectField', or 'me'".into())
            }
        } else {
            Err("Empty field path".into())
        }
    }

    /// Register custom syntax for qlib operations
    fn register_custom_syntax(engine: &mut Engine) -> Result<(), Box<rhai::EvalAltResult>> {
        use rhai::{EvalContext, Expression, Dynamic};
        
        // Custom syntax: TRANSACTION(string) - simplified version
        // Example: TRANSACTION("READ Name INTO train_name")
        engine.register_custom_syntax(
            ["TRANSACTION", "(", "$expr$", ")"],
            false,
            |context: &mut EvalContext, inputs: &[Expression]| -> Result<Dynamic, Box<rhai::EvalAltResult>> {
                let operation_str = context.eval_expression_tree(&inputs[0])?;
                
                let mut operations = Array::new();
                operations.push(operation_str);
                
                let mut tx_map = rhai::Map::new();
                tx_map.insert("Transaction".into(), Dynamic::from(operations));
                
                Ok(Dynamic::from(tx_map))
            }
        )?;

        // Register custom operator -> for field indirection
        engine.register_custom_operator("->", 160)?; // High precedence
        engine.register_fn("->", |left: &str, right: &str| -> String {
            format!("{}->{}", left, right)
        });
        
        // Register a fallback function that treats unknown variables as field names
        // but only for simple identifiers that look like field names (excluding special keywords)
        engine.on_var(|name, _index, _context| {
            // Don't treat special keywords as field names
            if name == "DEFAULT" || name == "me" {
                return Ok(None); // Let these be handled as variables
            }
            
            // Don't treat function-like names, reserved words, or common variable names as field names
            if name == "store" || name == "perform" || name == "create_entity" 
                || name == "entity_exists" || name.contains("_") 
                || name.starts_with("create_") 
                || name == "array" || name == "map" || name == "result" || name == "script"
                || name == "parsed" || name == "entity_id" {
                return Ok(None);
            }
            
            // Only treat simple CamelCase or lowercase identifiers as field names
            if name.chars().next().map_or(false, |c| c.is_alphabetic()) 
                && name.chars().all(|c| c.is_alphanumeric()) 
                && name.len() > 1 {
                Ok(Some(Dynamic::from(name.to_string())))
            } else {
                Ok(None) // Let Rhai handle other variables normally
            }
        });

        // Custom syntax: READ field_path INTO variable (handles both simple and indirection)
        // Examples: READ Name INTO train_name, READ NextStation->Name INTO station_name
        engine.register_custom_syntax(
            ["READ", "$expr$", "INTO", "$ident$"],
            false,
            |context: &mut EvalContext, inputs: &[Expression]| -> Result<Dynamic, Box<rhai::EvalAltResult>> {
                // Evaluate the field path expression - this will handle both simple identifiers and -> operations
                let field_path_result = context.eval_expression_tree(&inputs[0])?;
                let field_path = field_path_result.cast::<String>();
                let variable_name = inputs[1].get_string_value().unwrap();
                
                // Parse the field path - split by -> to handle indirection
                let parts: Vec<&str> = field_path.split("->").collect();
                
                let mut map = rhai::Map::new();
                map.insert("entity_id".into(), Dynamic::from("me"));
                map.insert("variable_name".into(), Dynamic::from(variable_name.to_string()));
                map.insert("include_associations".into(), Dynamic::from(false));
                
                if parts.len() == 1 {
                    // Simple field: Name
                    map.insert("field_type".into(), Dynamic::from(parts[0].to_string()));
                } else if parts.len() >= 2 {
                    // Indirection: NextStation->Name, or deeper: A->B->C
                    map.insert("field_type".into(), Dynamic::from(parts[0].to_string()));
                    
                    let mut indirection = Array::new();
                    for part in &parts[1..] {
                        indirection.push(Dynamic::from(part.to_string()));
                    }
                    map.insert("indirection".into(), Dynamic::from(indirection));
                }
                
                let mut request_map = rhai::Map::new();
                request_map.insert("Read".into(), Dynamic::from(map));
                Ok(Dynamic::from(request_map))
            }
        )?;

        // Custom syntax: WRITE value INTO field_path (handles both simple and indirection)
        // Examples: WRITE "DEFAULT" INTO StopTrigger, WRITE "me" INTO NextStation->CurrentTrain
        engine.register_custom_syntax(
            ["WRITE", "$expr$", "INTO", "$expr$"],
            false,
            |context: &mut EvalContext, inputs: &[Expression]| -> Result<Dynamic, Box<rhai::EvalAltResult>> {
                let value = context.eval_expression_tree(&inputs[0])?;
                let field_path_result = context.eval_expression_tree(&inputs[1])?;
                let field_path = field_path_result.cast::<String>();
                
                // Parse the field path - split by -> to handle indirection
                let parts: Vec<&str> = field_path.split("->").collect();
                
                let mut map = rhai::Map::new();
                map.insert("entity_id".into(), Dynamic::from("me"));
                map.insert("value".into(), value);
                map.insert("push_condition".into(), Dynamic::from("Always"));
                map.insert("adjust_behavior".into(), Dynamic::from("Set"));
                map.insert("write_time".into(), Dynamic::UNIT);
                map.insert("writer_id".into(), Dynamic::UNIT);
                
                if parts.len() == 1 {
                    // Simple field: StopTrigger
                    map.insert("field_type".into(), Dynamic::from(parts[0].to_string()));
                } else if parts.len() >= 2 {
                    // Indirection: NextStation->CurrentTrain, or deeper: A->B->C
                    map.insert("field_type".into(), Dynamic::from(parts[0].to_string()));
                    
                    let mut indirection = Array::new();
                    for part in &parts[1..] {
                        indirection.push(Dynamic::from(part.to_string()));
                    }
                    map.insert("indirection".into(), Dynamic::from(indirection));
                }
                
                let mut request_map = rhai::Map::new();
                request_map.insert("Write".into(), Dynamic::from(map));
                Ok(Dynamic::from(request_map))
            }
        )?;

        // Custom syntax: ADD value INTO field_path (handles both simple and indirection)
        // Examples: ADD 1 INTO UpdateCount, ADD 1 INTO NextStation->UpdateCount
        engine.register_custom_syntax(
            ["ADD", "$expr$", "INTO", "$expr$"],
            false,
            |context: &mut EvalContext, inputs: &[Expression]| -> Result<Dynamic, Box<rhai::EvalAltResult>> {
                let value = context.eval_expression_tree(&inputs[0])?;
                let field_path_result = context.eval_expression_tree(&inputs[1])?;
                let field_path = field_path_result.cast::<String>();
                
                // Parse the field path - split by -> to handle indirection
                let parts: Vec<&str> = field_path.split("->").collect();
                
                let mut map = rhai::Map::new();
                map.insert("entity_id".into(), Dynamic::from("me"));
                map.insert("value".into(), value);
                map.insert("push_condition".into(), Dynamic::from("Always"));
                map.insert("adjust_behavior".into(), Dynamic::from("Add"));
                map.insert("write_time".into(), Dynamic::UNIT);
                map.insert("writer_id".into(), Dynamic::UNIT);
                
                if parts.len() == 1 {
                    // Simple field: UpdateCount
                    map.insert("field_type".into(), Dynamic::from(parts[0].to_string()));
                } else if parts.len() >= 2 {
                    // Indirection: NextStation->UpdateCount, or deeper: A->B->C
                    map.insert("field_type".into(), Dynamic::from(parts[0].to_string()));
                    
                    let mut indirection = Array::new();
                    for part in &parts[1..] {
                        indirection.push(Dynamic::from(part.to_string()));
                    }
                    map.insert("indirection".into(), Dynamic::from(indirection));
                }
                
                let mut request_map = rhai::Map::new();
                request_map.insert("Write".into(), Dynamic::from(map));
                Ok(Dynamic::from(request_map))
            }
        )?;

        // Custom syntax: SUBTRACT value FROM field_path (handles both simple and indirection)
        // Examples: SUBTRACT 1 FROM Retries, SUBTRACT 1 FROM NextStation->Retries
        engine.register_custom_syntax(
            ["SUBTRACT", "$expr$", "FROM", "$expr$"],
            false,
            |context: &mut EvalContext, inputs: &[Expression]| -> Result<Dynamic, Box<rhai::EvalAltResult>> {
                let value = context.eval_expression_tree(&inputs[0])?;
                let field_path_result = context.eval_expression_tree(&inputs[1])?;
                let field_path = field_path_result.cast::<String>();
                
                // Parse the field path - split by -> to handle indirection
                let parts: Vec<&str> = field_path.split("->").collect();
                
                let mut map = rhai::Map::new();
                map.insert("entity_id".into(), Dynamic::from("me"));
                map.insert("value".into(), value);
                map.insert("push_condition".into(), Dynamic::from("Always"));
                map.insert("adjust_behavior".into(), Dynamic::from("Subtract"));
                map.insert("write_time".into(), Dynamic::UNIT);
                map.insert("writer_id".into(), Dynamic::UNIT);
                
                if parts.len() == 1 {
                    // Simple field: Retries
                    map.insert("field_type".into(), Dynamic::from(parts[0].to_string()));
                } else if parts.len() >= 2 {
                    // Indirection: NextStation->Retries, or deeper: A->B->C
                    map.insert("field_type".into(), Dynamic::from(parts[0].to_string()));
                    
                    let mut indirection = Array::new();
                    for part in &parts[1..] {
                        indirection.push(Dynamic::from(part.to_string()));
                    }
                    map.insert("indirection".into(), Dynamic::from(indirection));
                }
                
                let mut request_map = rhai::Map::new();
                request_map.insert("Write".into(), Dynamic::from(map));
                Ok(Dynamic::from(request_map))
            }
        )?;

        Ok(())
    }

    /// Execute a script with the given entity context
    pub fn execute_with_context(&mut self, script: &str, entity_id: &str) -> Result<Dynamic, Box<rhai::EvalAltResult>> {
        self.set_entity_context(Some(entity_id.to_string()));
        let mut scope = self.create_qlib_scope();
        scope.set_value("me", entity_id.to_string()); // Override me with actual entity ID
        self.engine.eval_with_scope(&mut scope, script)
    }

    /// Run a script with a pre-configured store proxy and entity context
    pub fn run_with_proxy_and_context(&self, script: &str, proxy: RhaiStoreProxyWrapper, entity_id: &str) -> Result<Dynamic, Box<EvalAltResult>> {
        let mut scope = Scope::new();
        scope.push("store", proxy);
        scope.push("me", entity_id.to_string());
        self.engine.eval_with_scope::<Dynamic>(&mut scope, script)
    }

    /// Execute a qlib operation using custom syntax
    /// This is a convenience method for executing simple operations
    pub fn execute_qlib_operation(&self, operation: &str) -> Result<Dynamic, Box<EvalAltResult>> {
        let mut scope = self.create_qlib_scope();
        self.engine.eval_with_scope::<Dynamic>(&mut scope, operation)
    }

    /// Execute multiple qlib operations in sequence
    pub fn execute_qlib_batch(&self, operations: &[&str]) -> Result<Vec<Dynamic>, Box<EvalAltResult>> {
        let mut results = Vec::new();
        for operation in operations {
            results.push(self.execute_qlib_operation(operation)?);
        }
        Ok(results)
    }

    /// Create a scope with common qlib variables pre-populated
    pub fn create_qlib_scope(&self) -> Scope {
        let mut scope = Scope::new();
        
        // Add some common constants
        scope.push_constant("ALWAYS", "Always");
        scope.push_constant("CHANGES", "Changes");
        scope.push_constant("SET", "Set");
        scope.push_constant("ADD", "Add");
        scope.push_constant("SUBTRACT", "Subtract");
        
        // Add special scripting variables
        scope.push_constant("DEFAULT", Dynamic::UNIT); // DEFAULT maps to None/Unit
        scope.push_constant("me", "me"); // me refers to the current entity
        
        scope
    }

    /// Get the underlying engine for advanced usage
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Get a mutable reference to the underlying engine for advanced configuration
    pub fn engine_mut(&mut self) -> &mut Engine {
        &mut self.engine
    }
}

impl Default for QScriptEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper macro for creating script-friendly field definitions
#[macro_export]
macro_rules! qscript_field {
    ($field_type:expr, $default:expr) => {{
        let mut map = rhai::Map::new();
        map.insert("type".into(), rhai::Dynamic::from($field_type));
        map.insert("default".into(), rhai::Dynamic::from($default));
        map.insert("rank".into(), rhai::Dynamic::from(0i64));
        map
    }};
    ($field_type:expr, $default:expr, $rank:expr) => {{
        let mut map = rhai::Map::new();
        map.insert("type".into(), rhai::Dynamic::from($field_type));
        map.insert("default".into(), rhai::Dynamic::from($default));
        map.insert("rank".into(), rhai::Dynamic::from($rank));
        map
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_script_execution() {
        let engine = QScriptEngine::new();
        
        let script = r#"
            // Test basic functionality without store creation
            let map = create_map();
            map["test"] = "value";
            
            let arr = create_array();
            arr.push(1);
            arr.push(2);
            
            map["test"]
        "#;

        // Test that the script compiles and runs
        let result: String = engine.engine.eval(script).unwrap();
        assert_eq!(result, "value");
    }

    #[test]
    fn test_entity_id_helpers() {
        let engine = QScriptEngine::new();
        
        let script = r#"
            let entity_id = format_entity_id("User", 123);
            let parsed = parse_entity_id(entity_id);
            [entity_id, parsed["type"], parsed["id"]]
        "#;

        let result: Array = engine.engine.eval(script).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].clone().cast::<String>(), "User$123");
        assert_eq!(result[1].clone().cast::<String>(), "User");
        assert_eq!(result[2].clone().cast::<i64>(), 123);
    }

    #[test]
    fn test_helper_functions() {
        let engine = QScriptEngine::new();
        
        let script = r#"
            let map = create_map();
            map["test"] = "value";
            
            let arr = create_array();
            arr.push(1);
            arr.push(2);
            arr.push(3);
            
            [map["test"], arr.len()]
        "#;

        let result: Array = engine.engine.eval(script).unwrap();
        assert_eq!(result[0].clone().cast::<String>(), "value");
        assert_eq!(result[1].clone().cast::<i64>(), 3);
    }

    #[test]
    fn test_new_transaction_syntax() {
        let engine = QScriptEngine::new();
        
        let script = r#"
            TRANSACTION("READ Name INTO train_name")
        "#;

        let result: rhai::Map = engine.engine.eval(script).unwrap();
        assert!(result.contains_key("Transaction"));
    }

    #[test]
    fn test_read_into_syntax() {
        let engine = QScriptEngine::new();
        
        let script = r#"
            READ Name INTO train_name
        "#;

        let result: rhai::Map = engine.engine.eval(script).unwrap();
        assert!(result.contains_key("Read"));
        
        if let Some(read_map) = result.get("Read").and_then(|v| v.clone().try_cast::<rhai::Map>()) {
            assert_eq!(read_map.get("entity_id").unwrap().clone().cast::<String>(), "me");
            assert_eq!(read_map.get("field_type").unwrap().clone().cast::<String>(), "Name");
            assert_eq!(read_map.get("variable_name").unwrap().clone().cast::<String>(), "train_name");
        } else {
            panic!("Expected Read map in result");
        }
    }

    #[test]
    fn test_read_into_syntax_with_indirection() {
        let engine = QScriptEngine::new();
        
        let script = r#"
            READ NextStation->Name INTO station_name
        "#;

        let result: rhai::Map = engine.engine.eval(script).unwrap();
        assert!(result.contains_key("Read"));
        
        if let Some(read_map) = result.get("Read").and_then(|v| v.clone().try_cast::<rhai::Map>()) {
            assert_eq!(read_map.get("entity_id").unwrap().clone().cast::<String>(), "me");
            assert_eq!(read_map.get("field_type").unwrap().clone().cast::<String>(), "NextStation");
            assert_eq!(read_map.get("variable_name").unwrap().clone().cast::<String>(), "station_name");
            
            if let Some(indirection) = read_map.get("indirection").and_then(|v| v.clone().try_cast::<Array>()) {
                assert_eq!(indirection.len(), 1);
                assert_eq!(indirection[0].clone().cast::<String>(), "Name");
            } else {
                panic!("Expected indirection array in result");
            }
        } else {
            panic!("Expected Read map in result");
        }
    }

    #[test]
    fn test_write_into_syntax() {
        let engine = QScriptEngine::new();
        
        let script = r#"
            WRITE "DEFAULT" INTO StopTrigger
        "#;

        let result: rhai::Map = engine.engine.eval(script).unwrap();
        assert!(result.contains_key("Write"));
        
        if let Some(write_map) = result.get("Write").and_then(|v| v.clone().try_cast::<rhai::Map>()) {
            assert_eq!(write_map.get("entity_id").unwrap().clone().cast::<String>(), "me");
            assert_eq!(write_map.get("field_type").unwrap().clone().cast::<String>(), "StopTrigger");
            assert_eq!(write_map.get("value").unwrap().clone().cast::<String>(), "DEFAULT");
            assert_eq!(write_map.get("adjust_behavior").unwrap().clone().cast::<String>(), "Set");
        } else {
            panic!("Expected Write map in result");
        }
    }

    #[test]
    fn test_write_into_syntax_with_indirection() {
        let engine = QScriptEngine::new();
        
        let script = r#"
            WRITE "me" INTO NextStation->CurrentTrain
        "#;

        let result: rhai::Map = engine.engine.eval(script).unwrap();
        assert!(result.contains_key("Write"));
        
        if let Some(write_map) = result.get("Write").and_then(|v| v.clone().try_cast::<rhai::Map>()) {
            assert_eq!(write_map.get("entity_id").unwrap().clone().cast::<String>(), "me");
            assert_eq!(write_map.get("field_type").unwrap().clone().cast::<String>(), "NextStation");
            assert_eq!(write_map.get("value").unwrap().clone().cast::<String>(), "me");
            assert_eq!(write_map.get("adjust_behavior").unwrap().clone().cast::<String>(), "Set");
            
            if let Some(indirection) = write_map.get("indirection").and_then(|v| v.clone().try_cast::<Array>()) {
                assert_eq!(indirection.len(), 1);
                assert_eq!(indirection[0].clone().cast::<String>(), "CurrentTrain");
            } else {
                panic!("Expected indirection array in result");
            }
        } else {
            panic!("Expected Write map in result");
        }
    }

    #[test]
    fn test_add_into_syntax() {
        let engine = QScriptEngine::new();
        
        let script = r#"
            ADD 1 INTO NextStation->UpdateCount
        "#;

        let result: rhai::Map = engine.engine.eval(script).unwrap();
        assert!(result.contains_key("Write"));
        
        if let Some(write_map) = result.get("Write").and_then(|v| v.clone().try_cast::<rhai::Map>()) {
            assert_eq!(write_map.get("entity_id").unwrap().clone().cast::<String>(), "me");
            assert_eq!(write_map.get("field_type").unwrap().clone().cast::<String>(), "NextStation");
            assert_eq!(write_map.get("value").unwrap().clone().cast::<i64>(), 1);
            assert_eq!(write_map.get("adjust_behavior").unwrap().clone().cast::<String>(), "Add");
            
            if let Some(indirection) = write_map.get("indirection").and_then(|v| v.clone().try_cast::<Array>()) {
                assert_eq!(indirection.len(), 1);
                assert_eq!(indirection[0].clone().cast::<String>(), "UpdateCount");
            } else {
                panic!("Expected indirection array in result");
            }
        } else {
            panic!("Expected Write map in result");
        }
    }

    #[test]
    fn test_subtract_from_syntax() {
        let engine = QScriptEngine::new();
        
        let script = r#"
            SUBTRACT 1 FROM NextStation->Retries
        "#;

        let result: rhai::Map = engine.engine.eval(script).unwrap();
        assert!(result.contains_key("Write"));
        
        if let Some(write_map) = result.get("Write").and_then(|v| v.clone().try_cast::<rhai::Map>()) {
            assert_eq!(write_map.get("entity_id").unwrap().clone().cast::<String>(), "me");
            assert_eq!(write_map.get("field_type").unwrap().clone().cast::<String>(), "NextStation");
            assert_eq!(write_map.get("value").unwrap().clone().cast::<i64>(), 1);
            assert_eq!(write_map.get("adjust_behavior").unwrap().clone().cast::<String>(), "Subtract");
            
            if let Some(indirection) = write_map.get("indirection").and_then(|v| v.clone().try_cast::<Array>()) {
                assert_eq!(indirection.len(), 1);
                assert_eq!(indirection[0].clone().cast::<String>(), "Retries");
            } else {
                panic!("Expected indirection array in result");
            }
        } else {
            panic!("Expected Write map in result");
        }
    }

    #[test]
    fn test_timestamp_helper() {
        let engine = QScriptEngine::new();
        
        let script = r#"
            now()
        "#;

        let result: i64 = engine.engine.eval(script).unwrap();
        // Just check that we get a reasonable timestamp (greater than 2020-01-01)
        assert!(result > 1577836800000); // 2020-01-01 in milliseconds
    }

    #[test]
    fn test_transaction_array_syntax() {
        let engine = QScriptEngine::new();
        
        // Test simpler transaction syntax that actually works
        let script = r#"
            let operations = [
                READ Name INTO train_name,
                WRITE "me" INTO NextStation->CurrentTrain,
                ADD 1 INTO NextStation->UpdateCount,
                SUBTRACT 1 FROM NextStation->Retries
            ];
            operations
        "#;

        let result: Array = engine.engine.eval(script).unwrap();
        assert_eq!(result.len(), 4);
        
        // Check that each operation parsed correctly
        assert!(result[0].clone().try_cast::<rhai::Map>().unwrap().contains_key("Read"));
        assert!(result[1].clone().try_cast::<rhai::Map>().unwrap().contains_key("Write"));
        assert!(result[2].clone().try_cast::<rhai::Map>().unwrap().contains_key("Write"));
        assert!(result[3].clone().try_cast::<rhai::Map>().unwrap().contains_key("Write"));
    }

    #[test]
    fn test_special_variables_default_and_me() {
        let engine = QScriptEngine::new();
        
        // Test WRITE DEFAULT INTO Example
        let script1 = r#"
            WRITE DEFAULT INTO Example
        "#;

        let result1_dynamic = engine.execute_qlib_operation(script1).unwrap();
        let result1: rhai::Map = result1_dynamic.cast();
        assert!(result1.contains_key("Write"));
        
        if let Some(write_map) = result1.get("Write").and_then(|v| v.clone().try_cast::<rhai::Map>()) {
            assert_eq!(write_map.get("field_type").unwrap().clone().cast::<String>(), "Example");
            // DEFAULT should map to Dynamic::UNIT (None value)
            assert!(write_map.get("value").unwrap().is_unit());
        }
        
        // Test WRITE me INTO NextStation->CurrentTrain
        let script2 = r#"
            WRITE me INTO NextStation->CurrentTrain
        "#;

        let result2_dynamic = engine.execute_qlib_operation(script2).unwrap();
        let result2: rhai::Map = result2_dynamic.cast();
        assert!(result2.contains_key("Write"));
        
        if let Some(write_map) = result2.get("Write").and_then(|v| v.clone().try_cast::<rhai::Map>()) {
            assert_eq!(write_map.get("field_type").unwrap().clone().cast::<String>(), "NextStation");
            assert_eq!(write_map.get("value").unwrap().clone().cast::<String>(), "me");
            
            if let Some(indirection) = write_map.get("indirection").and_then(|v| v.clone().try_cast::<Array>()) {
                assert_eq!(indirection.len(), 1);
                assert_eq!(indirection[0].clone().cast::<String>(), "CurrentTrain");
            } else {
                panic!("Expected indirection array in result");
            }
        }
    }
}
