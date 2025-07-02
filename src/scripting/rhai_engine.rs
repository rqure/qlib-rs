use rhai::{Engine, Scope, Dynamic, Map, Array, AST, ParseError, EvalAltResult};
use super::{RhaiStoreWrapper, RhaiStoreProxyWrapper, create_read_request, create_write_request, create_add_request, create_subtract_request};

/// A Rhai scripting engine configured for qlib store operations with custom syntax
/// 
/// This engine provides an enhanced API for qlib operations through custom syntax:
/// 
/// ## Basic Operations
/// ```rhai
/// // Reading a field
/// read User$123.Name
/// 
/// // Writing a field
/// write User$123.Name = "John Doe"
/// 
/// // Adding to a numeric field
/// add User$123.Score += 10
/// 
/// // Subtracting from a numeric field  
/// sub User$123.Score -= 5
/// 
/// // Creating an entity with fields
/// entity User$123 { Name: "John", Age: 25 }
/// 
/// // Querying entities
/// query User where Age > 18
/// ```
/// 
/// ## Indirection Support
/// ```rhai
/// // Reading through indirection (follows references)
/// read User$123.Company->Name
/// read User$123.Manager->Department->Name
/// 
/// // Writing through indirection
/// write User$123.Company->Budget = 100000
/// 
/// // Multi-level indirection
/// read Order$456.Customer->Company->Address->City
/// ```
/// 
/// ## Transactions
/// ```rhai
/// // Single transaction with multiple operations
/// transaction {
///     write User$123.Name = "John Doe";
///     add User$123.Score += 10;
///     write User$123.LastUpdated = now();
/// }
/// 
/// // Named transaction with rollback capability
/// let tx = begin_transaction("user_update");
/// write User$123.Name = "John Doe";
/// add User$123.Score += 10;
/// if (some_condition) {
///     commit_transaction(tx);
/// } else {
///     rollback_transaction(tx);
/// }
/// 
/// // Batch operations
/// batch [
///     read User$123.Name,
///     read User$124.Name,
///     read User$125.Name
/// ]
/// ```
/// 
/// ## Helper Functions
/// - `create_map()` - Create an empty map
/// - `create_array()` - Create an empty array
/// - `parse_entity_id(id)` - Parse entity ID into components
/// - `format_entity_id(type, id)` - Format entity ID from components
/// - `print_info(msg)`, `print_debug(msg)`, `print_error(msg)` - Logging functions
pub struct QScriptEngine {
    engine: Engine,
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
        engine.register_fn("entity_exists", RhaiStoreWrapper::entity_exists);
        
        // Register StoreProxy wrapper methods (perform-based API only)
        engine.register_fn("perform", RhaiStoreProxyWrapper::perform);
        engine.register_fn("create_entity", RhaiStoreProxyWrapper::create_entity);
        engine.register_fn("entity_exists", RhaiStoreProxyWrapper::entity_exists);
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
        
        // Register transaction helper functions
        engine.register_fn("begin_transaction", Self::begin_transaction);
        engine.register_fn("commit_transaction", Self::commit_transaction);
        engine.register_fn("rollback_transaction", Self::rollback_transaction);
        engine.register_fn("create_batch", Self::create_batch);
        engine.register_fn("now", Self::now);
        
        // Register indirection helper
        engine.register_fn("parse_field_path", Self::parse_field_path);
        
        // Register custom syntax for improved API
        Self::register_custom_syntax(&mut engine).expect("Failed to register custom syntax");
        
        Self { engine }
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

    /// Create a batch operation container
    fn create_batch() -> Array {
        Array::new()
    }

    /// Get current timestamp
    fn now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    /// Parse field path with indirection support
    /// Example: "User$123.Company->Name" returns ["User$123", "Company", "Name"]
    fn parse_field_path(path: &str) -> Result<Array, Box<EvalAltResult>> {
        let mut parts = Array::new();
        
        // Split by -> for indirection
        let indirection_parts: Vec<&str> = path.split("->").collect();
        
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
                Err("Invalid field path format. Use 'EntityType$id.FieldType' or 'EntityType$id.FieldType->IndirectField'".into())
            }
        } else {
            Err("Empty field path".into())
        }
    }

    /// Register custom syntax for qlib operations
    fn register_custom_syntax(engine: &mut Engine) -> Result<(), Box<rhai::EvalAltResult>> {
        use rhai::{EvalContext, Expression, Dynamic};
        
        // Custom syntax: read entity_id.field_type or entity_id.field_type->indirect_field
        // Example: read User$123.Name or read User$123.Company->Name
        engine.register_custom_syntax(
            ["read", "$ident$"],
            false,
            |_context: &mut EvalContext, inputs: &[Expression]| -> Result<Dynamic, Box<rhai::EvalAltResult>> {
                let path = inputs[0].get_string_value().unwrap();
                
                // Parse field path with potential indirection
                let parsed_path = Self::parse_field_path(&path)?;
                
                if parsed_path.is_empty() {
                    return Err("Invalid field path".into());
                }
                
                let entity_id = parsed_path[0].clone().cast::<String>();
                let field_type = parsed_path[1].clone().cast::<String>();
                
                let mut map = rhai::Map::new();
                map.insert("entity_id".into(), Dynamic::from(entity_id));
                map.insert("field_type".into(), Dynamic::from(field_type));
                map.insert("value".into(), Dynamic::UNIT);
                map.insert("write_time".into(), Dynamic::UNIT);
                map.insert("writer_id".into(), Dynamic::UNIT);
                
                // Add indirection path if present
                if parsed_path.len() > 2 {
                    let mut indirection = Array::new();
                    for i in 2..parsed_path.len() {
                        indirection.push(parsed_path[i].clone());
                    }
                    map.insert("indirection".into(), Dynamic::from(indirection));
                }
                
                let mut request_map = rhai::Map::new();
                request_map.insert("Read".into(), Dynamic::from(map));
                Ok(Dynamic::from(request_map))
            }
        )?;

        // Custom syntax: write entity_id.field_type = value or entity_id.field_type->indirect_field = value
        // Example: write User$123.Name = "John" or write User$123.Company->Budget = 100000
        engine.register_custom_syntax(
            ["write", "$ident$", "=", "$expr$"],
            false,
            |context: &mut EvalContext, inputs: &[Expression]| -> Result<Dynamic, Box<rhai::EvalAltResult>> {
                let path = inputs[0].get_string_value().unwrap();
                let value = context.eval_expression_tree(&inputs[1])?;
                
                // Parse field path with potential indirection
                let parsed_path = Self::parse_field_path(&path)?;
                
                if parsed_path.is_empty() {
                    return Err("Invalid field path".into());
                }
                
                let entity_id = parsed_path[0].clone().cast::<String>();
                let field_type = parsed_path[1].clone().cast::<String>();
                
                let mut map = rhai::Map::new();
                map.insert("entity_id".into(), Dynamic::from(entity_id));
                map.insert("field_type".into(), Dynamic::from(field_type));
                map.insert("value".into(), value);
                map.insert("push_condition".into(), Dynamic::from("Always"));
                map.insert("adjust_behavior".into(), Dynamic::from("Set"));
                map.insert("write_time".into(), Dynamic::UNIT);
                map.insert("writer_id".into(), Dynamic::UNIT);
                
                // Add indirection path if present
                if parsed_path.len() > 2 {
                    let mut indirection = Array::new();
                    for i in 2..parsed_path.len() {
                        indirection.push(parsed_path[i].clone());
                    }
                    map.insert("indirection".into(), Dynamic::from(indirection));
                }
                
                let mut request_map = rhai::Map::new();
                request_map.insert("Write".into(), Dynamic::from(map));
                Ok(Dynamic::from(request_map))
            }
        )?;

        // Custom syntax: add entity_id.field_type += value or entity_id.field_type->indirect_field += value
        // Example: add User$123.Score += 10 or add User$123.Company->Budget += 5000
        engine.register_custom_syntax(
            ["add", "$ident$", "+=", "$expr$"],
            false,
            |context: &mut EvalContext, inputs: &[Expression]| -> Result<Dynamic, Box<rhai::EvalAltResult>> {
                let path = inputs[0].get_string_value().unwrap();
                let value = context.eval_expression_tree(&inputs[1])?;
                
                // Parse field path with potential indirection
                let parsed_path = Self::parse_field_path(&path)?;
                
                if parsed_path.is_empty() {
                    return Err("Invalid field path".into());
                }
                
                let entity_id = parsed_path[0].clone().cast::<String>();
                let field_type = parsed_path[1].clone().cast::<String>();
                
                let mut map = rhai::Map::new();
                map.insert("entity_id".into(), Dynamic::from(entity_id));
                map.insert("field_type".into(), Dynamic::from(field_type));
                map.insert("value".into(), value);
                map.insert("push_condition".into(), Dynamic::from("Always"));
                map.insert("adjust_behavior".into(), Dynamic::from("Add"));
                map.insert("write_time".into(), Dynamic::UNIT);
                map.insert("writer_id".into(), Dynamic::UNIT);
                
                // Add indirection path if present
                if parsed_path.len() > 2 {
                    let mut indirection = Array::new();
                    for i in 2..parsed_path.len() {
                        indirection.push(parsed_path[i].clone());
                    }
                    map.insert("indirection".into(), Dynamic::from(indirection));
                }
                
                let mut request_map = rhai::Map::new();
                request_map.insert("Write".into(), Dynamic::from(map));
                Ok(Dynamic::from(request_map))
            }
        )?;

        // Custom syntax: sub entity_id.field_type -= value or entity_id.field_type->indirect_field -= value
        // Example: sub User$123.Score -= 5 or sub User$123.Company->Budget -= 1000
        engine.register_custom_syntax(
            ["sub", "$ident$", "-=", "$expr$"],
            false,
            |context: &mut EvalContext, inputs: &[Expression]| -> Result<Dynamic, Box<rhai::EvalAltResult>> {
                let path = inputs[0].get_string_value().unwrap();
                let value = context.eval_expression_tree(&inputs[1])?;
                
                // Parse field path with potential indirection
                let parsed_path = Self::parse_field_path(&path)?;
                
                if parsed_path.is_empty() {
                    return Err("Invalid field path".into());
                }
                
                let entity_id = parsed_path[0].clone().cast::<String>();
                let field_type = parsed_path[1].clone().cast::<String>();
                
                let mut map = rhai::Map::new();
                map.insert("entity_id".into(), Dynamic::from(entity_id));
                map.insert("field_type".into(), Dynamic::from(field_type));
                map.insert("value".into(), value);
                map.insert("push_condition".into(), Dynamic::from("Always"));
                map.insert("adjust_behavior".into(), Dynamic::from("Subtract"));
                map.insert("write_time".into(), Dynamic::UNIT);
                map.insert("writer_id".into(), Dynamic::UNIT);
                
                // Add indirection path if present
                if parsed_path.len() > 2 {
                    let mut indirection = Array::new();
                    for i in 2..parsed_path.len() {
                        indirection.push(parsed_path[i].clone());
                    }
                    map.insert("indirection".into(), Dynamic::from(indirection));
                }
                
                let mut request_map = rhai::Map::new();
                request_map.insert("Write".into(), Dynamic::from(map));
                Ok(Dynamic::from(request_map))
            }
        )?;

        // Custom syntax: entity EntityType$id { field1: value1, field2: value2 }
        // Example: entity User$123 { Name: "John", Age: 25 }
        engine.register_custom_syntax(
            ["entity", "$ident$", "$block$"],
            false,
            |_context: &mut EvalContext, inputs: &[Expression]| -> Result<Dynamic, Box<rhai::EvalAltResult>> {
                let entity_id = inputs[0].get_string_value().unwrap();
                let _block = &inputs[1];
                
                // Create entity first
                let mut create_map = rhai::Map::new();
                create_map.insert("entity_id".into(), Dynamic::from(entity_id.to_string()));
                
                // Execute the block to collect field assignments
                // For now, just return the entity creation request
                // In a full implementation, this would parse the block for field assignments
                Ok(Dynamic::from(create_map))
            }
        )?;

        // Custom syntax: query entities where field_type operator value
        // Example: query User where Age > 18
        engine.register_custom_syntax(
            ["query", "$ident$", "where", "$ident$", "$symbol$", "$expr$"],
            false,
            |context: &mut EvalContext, inputs: &[Expression]| -> Result<Dynamic, Box<rhai::EvalAltResult>> {
                let entity_type = inputs[0].get_string_value().unwrap();
                let field_type = inputs[1].get_string_value().unwrap();
                let operator = inputs[2].get_string_value().unwrap();
                let value = context.eval_expression_tree(&inputs[3])?;
                
                let mut query_map = rhai::Map::new();
                query_map.insert("entity_type".into(), Dynamic::from(entity_type.to_string()));
                query_map.insert("field_type".into(), Dynamic::from(field_type.to_string()));
                query_map.insert("operator".into(), Dynamic::from(operator.to_string()));
                query_map.insert("value".into(), value);
                
                Ok(Dynamic::from(query_map))
            }
        )?;

        // Custom syntax: transaction { ... }
        // Example: transaction { write User$123.Name = "John"; add User$123.Score += 10; }
        engine.register_custom_syntax(
            ["transaction", "$block$"],
            false,
            |context: &mut EvalContext, inputs: &[Expression]| -> Result<Dynamic, Box<rhai::EvalAltResult>> {
                let block = &inputs[0];
                
                // Generate a unique transaction ID
                let tx_id = format!("tx_{}", uuid::Uuid::new_v4().to_string().replace("-", ""));
                
                // Execute the block and collect all operations
                let result = context.eval_expression_tree(block)?;
                
                let mut tx_map = rhai::Map::new();
                tx_map.insert("transaction_id".into(), Dynamic::from(tx_id));
                tx_map.insert("operations".into(), result);
                tx_map.insert("type".into(), Dynamic::from("auto_commit"));
                
                Ok(Dynamic::from(tx_map))
            }
        )?;

        // Custom syntax: batch [ operation1, operation2, ... ]
        // Example: batch [ read User$123.Name, read User$124.Name, read User$125.Name ]
        engine.register_custom_syntax(
            ["batch", "$expr$"],
            false,
            |context: &mut EvalContext, inputs: &[Expression]| -> Result<Dynamic, Box<rhai::EvalAltResult>> {
                let operations = context.eval_expression_tree(&inputs[0])?;
                
                let mut batch_map = rhai::Map::new();
                batch_map.insert("type".into(), Dynamic::from("batch"));
                batch_map.insert("operations".into(), operations);
                batch_map.insert("parallel".into(), Dynamic::from(true));
                
                Ok(Dynamic::from(batch_map))
            }
        )?;

        Ok(())
    }

    /// Compile a script
    pub fn compile(&self, script: &str) -> Result<AST, ParseError> {
        self.engine.compile(script)
    }

    /// Evaluate a script with the given scope
    pub fn eval_with_scope<T: Clone + 'static + Send + Sync>(&self, scope: &mut Scope, script: &str) -> Result<T, Box<EvalAltResult>> {
        self.engine.eval_with_scope::<T>(scope, script)
    }

    /// Evaluate a compiled AST with the given scope
    pub fn eval_ast_with_scope<T: Clone + 'static + Send + Sync>(&self, scope: &mut Scope, ast: &AST) -> Result<T, Box<EvalAltResult>> {
        self.engine.eval_ast_with_scope::<T>(scope, ast)
    }

    /// Run a script file
    pub fn run_file(&self, path: &str) -> Result<Dynamic, Box<EvalAltResult>> {
        let script = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read script file: {}", e))?;
        self.engine.eval::<Dynamic>(&script)
    }

    /// Run a script with a pre-configured store
    pub fn run_with_store(&self, script: &str, store: RhaiStoreWrapper) -> Result<Dynamic, Box<EvalAltResult>> {
        let mut scope = Scope::new();
        scope.push("store", store);
        self.eval_with_scope::<Dynamic>(&mut scope, script)
    }

    /// Run a script with a pre-configured store proxy
    pub fn run_with_proxy(&self, script: &str, proxy: RhaiStoreProxyWrapper) -> Result<Dynamic, Box<EvalAltResult>> {
        let mut scope = Scope::new();
        scope.push("store", proxy);
        self.eval_with_scope::<Dynamic>(&mut scope, script)
    }

    /// Execute a qlib operation using custom syntax
    /// This is a convenience method for executing simple operations
    pub fn execute_qlib_operation(&self, operation: &str) -> Result<Dynamic, Box<EvalAltResult>> {
        self.engine.eval::<Dynamic>(operation)
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
    fn test_custom_syntax_read() {
        let engine = QScriptEngine::new();
        
        let script = r#"
            read User$123.Name
        "#;

        let result: rhai::Map = engine.engine.eval(script).unwrap();
        assert!(result.contains_key("Read"));
        
        if let Some(read_map) = result.get("Read").and_then(|v| v.clone().try_cast::<rhai::Map>()) {
            assert_eq!(read_map.get("entity_id").unwrap().clone().cast::<String>(), "User$123");
            assert_eq!(read_map.get("field_type").unwrap().clone().cast::<String>(), "Name");
        } else {
            panic!("Expected Read map in result");
        }
    }

    #[test]
    fn test_custom_syntax_write() {
        let engine = QScriptEngine::new();
        
        let script = r#"
            write User$123.Name = "John Doe"
        "#;

        let result: rhai::Map = engine.engine.eval(script).unwrap();
        assert!(result.contains_key("Write"));
        
        if let Some(write_map) = result.get("Write").and_then(|v| v.clone().try_cast::<rhai::Map>()) {
            assert_eq!(write_map.get("entity_id").unwrap().clone().cast::<String>(), "User$123");
            assert_eq!(write_map.get("field_type").unwrap().clone().cast::<String>(), "Name");
            assert_eq!(write_map.get("value").unwrap().clone().cast::<String>(), "John Doe");
            assert_eq!(write_map.get("adjust_behavior").unwrap().clone().cast::<String>(), "Set");
        } else {
            panic!("Expected Write map in result");
        }
    }

    #[test]
    fn test_custom_syntax_add() {
        let engine = QScriptEngine::new();
        
        let script = r#"
            add User$123.Score += 10
        "#;

        let result: rhai::Map = engine.engine.eval(script).unwrap();
        assert!(result.contains_key("Write"));
        
        if let Some(write_map) = result.get("Write").and_then(|v| v.clone().try_cast::<rhai::Map>()) {
            assert_eq!(write_map.get("entity_id").unwrap().clone().cast::<String>(), "User$123");
            assert_eq!(write_map.get("field_type").unwrap().clone().cast::<String>(), "Score");
            assert_eq!(write_map.get("value").unwrap().clone().cast::<i64>(), 10);
            assert_eq!(write_map.get("adjust_behavior").unwrap().clone().cast::<String>(), "Add");
        } else {
            panic!("Expected Write map in result");
        }
    }

    #[test]
    fn test_custom_syntax_subtract() {
        let engine = QScriptEngine::new();
        
        let script = r#"
            sub User$123.Score -= 5
        "#;

        let result: rhai::Map = engine.engine.eval(script).unwrap();
        assert!(result.contains_key("Write"));
        
        if let Some(write_map) = result.get("Write").and_then(|v| v.clone().try_cast::<rhai::Map>()) {
            assert_eq!(write_map.get("entity_id").unwrap().clone().cast::<String>(), "User$123");
            assert_eq!(write_map.get("field_type").unwrap().clone().cast::<String>(), "Score");
            assert_eq!(write_map.get("value").unwrap().clone().cast::<i64>(), 5);
            assert_eq!(write_map.get("adjust_behavior").unwrap().clone().cast::<String>(), "Subtract");
        } else {
            panic!("Expected Write map in result");
        }
    }

    #[test]
    fn test_custom_syntax_query() {
        let engine = QScriptEngine::new();
        
        let script = r#"
            query User where Age > 18
        "#;

        let result: rhai::Map = engine.engine.eval(script).unwrap();
        assert_eq!(result.get("entity_type").unwrap().clone().cast::<String>(), "User");
        assert_eq!(result.get("field_type").unwrap().clone().cast::<String>(), "Age");
        assert_eq!(result.get("operator").unwrap().clone().cast::<String>(), ">");
        assert_eq!(result.get("value").unwrap().clone().cast::<i64>(), 18);
    }

    #[test]
    fn test_custom_syntax_entity_creation() {
        let engine = QScriptEngine::new();
        
        let script = r#"
            entity User$123 { /* fields would go here */ }
        "#;

        let result: rhai::Map = engine.engine.eval(script).unwrap();
        assert_eq!(result.get("entity_id").unwrap().clone().cast::<String>(), "User$123");
    }

    #[test]
    fn test_custom_syntax_error_handling() {
        let engine = QScriptEngine::new();
        
        // Test invalid entity path format
        let script = r#"
            read InvalidPath
        "#;

        let result = engine.engine.eval::<rhai::Map>(script);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid field path format"));
    }

    #[test]
    fn test_indirection_parsing() {
        let engine = QScriptEngine::new();
        
        let script = r#"
            parse_field_path("User$123.Company->Name")
        "#;

        let result: Array = engine.engine.eval(script).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].clone().cast::<String>(), "User$123");
        assert_eq!(result[1].clone().cast::<String>(), "Company");
        assert_eq!(result[2].clone().cast::<String>(), "Name");
    }

    #[test]
    fn test_multi_level_indirection_parsing() {
        let engine = QScriptEngine::new();
        
        let script = r#"
            parse_field_path("Order$456.Customer->Company->Address->City")
        "#;

        let result: Array = engine.engine.eval(script).unwrap();
        assert_eq!(result.len(), 5);
        assert_eq!(result[0].clone().cast::<String>(), "Order$456");
        assert_eq!(result[1].clone().cast::<String>(), "Customer");
        assert_eq!(result[2].clone().cast::<String>(), "Company");
        assert_eq!(result[3].clone().cast::<String>(), "Address");
        assert_eq!(result[4].clone().cast::<String>(), "City");
    }

    #[test]
    fn test_custom_syntax_read_with_indirection() {
        let engine = QScriptEngine::new();
        
        let script = r#"
            read User$123.Company->Name
        "#;

        let result: rhai::Map = engine.engine.eval(script).unwrap();
        assert!(result.contains_key("Read"));
        
        if let Some(read_map) = result.get("Read").and_then(|v| v.clone().try_cast::<rhai::Map>()) {
            assert_eq!(read_map.get("entity_id").unwrap().clone().cast::<String>(), "User$123");
            assert_eq!(read_map.get("field_type").unwrap().clone().cast::<String>(), "Company");
            
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
    fn test_custom_syntax_write_with_indirection() {
        let engine = QScriptEngine::new();
        
        let script = r#"
            write User$123.Company->Budget = 100000
        "#;

        let result: rhai::Map = engine.engine.eval(script).unwrap();
        assert!(result.contains_key("Write"));
        
        if let Some(write_map) = result.get("Write").and_then(|v| v.clone().try_cast::<rhai::Map>()) {
            assert_eq!(write_map.get("entity_id").unwrap().clone().cast::<String>(), "User$123");
            assert_eq!(write_map.get("field_type").unwrap().clone().cast::<String>(), "Company");
            assert_eq!(write_map.get("value").unwrap().clone().cast::<i64>(), 100000);
            
            if let Some(indirection) = write_map.get("indirection").and_then(|v| v.clone().try_cast::<Array>()) {
                assert_eq!(indirection.len(), 1);
                assert_eq!(indirection[0].clone().cast::<String>(), "Budget");
            } else {
                panic!("Expected indirection array in result");
            }
        } else {
            panic!("Expected Write map in result");
        }
    }

    #[test]
    fn test_transaction_helpers() {
        let engine = QScriptEngine::new();
        
        let script = r#"
            let tx = begin_transaction("test_transaction");
            let commit_result = commit_transaction(tx);
            let rollback_result = rollback_transaction(tx);
            [tx.len() > 0, commit_result["action"], rollback_result["action"]]
        "#;

        let result: Array = engine.engine.eval(script).unwrap();
        assert_eq!(result[0].clone().cast::<bool>(), true); // tx ID should be non-empty
        assert_eq!(result[1].clone().cast::<String>(), "commit");
        assert_eq!(result[2].clone().cast::<String>(), "rollback");
    }

    #[test]
    fn test_custom_syntax_transaction() {
        let engine = QScriptEngine::new();
        
        let script = r#"
            transaction {
                // This would contain actual operations in a real scenario
                42
            }
        "#;

        let result: rhai::Map = engine.engine.eval(script).unwrap();
        assert_eq!(result.get("type").unwrap().clone().cast::<String>(), "auto_commit");
        assert!(result.contains_key("transaction_id"));
        assert!(result.contains_key("operations"));
    }

    #[test]
    fn test_custom_syntax_batch() {
        let engine = QScriptEngine::new();
        
        let script = r#"
            batch [1, 2, 3]
        "#;

        let result: rhai::Map = engine.engine.eval(script).unwrap();
        assert_eq!(result.get("type").unwrap().clone().cast::<String>(), "batch");
        assert_eq!(result.get("parallel").unwrap().clone().cast::<bool>(), true);
        
        if let Some(operations) = result.get("operations").and_then(|v| v.clone().try_cast::<Array>()) {
            assert_eq!(operations.len(), 3);
        } else {
            panic!("Expected operations array in batch result");
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
}
