use rhai::{Engine, Scope, Dynamic, Map, Array, AST, ParseError, EvalAltResult};
use super::{RhaiStoreWrapper, RhaiStoreProxyWrapper, create_read_request, create_write_request, create_add_request, create_subtract_request};

/// A Rhai scripting engine configured for qlib store operations
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
}
