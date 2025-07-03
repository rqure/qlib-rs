use rhai::{Engine, Dynamic, Array, Scope, EvalAltResult, AST};
use std::sync::Arc;

use crate::{
    Context, Store, EntityId, Value, Notification
};

/// A Rhai scripting engine wrapper for Store operations
pub struct ScriptEngine {
    engine: Engine,
    store: Arc<Store>,
}

/// Script context that provides access to the current entity and transaction state
#[derive(Debug, Clone)]
pub struct ScriptContext {
    pub entity_id: Option<EntityId>,
    pub context: Context,
}

impl ScriptContext {
    pub fn new(entity_id: Option<EntityId>) -> Self {
        Self {
            entity_id,
            context: Context {},
        }
    }

    pub fn with_entity(entity_id: EntityId) -> Self {
        Self::new(Some(entity_id))
    }
}

impl ScriptEngine {
    pub fn new(store: Arc<Store>) -> Self {
        let mut engine = Engine::new();
        
        // Configure engine for safety and performance
        engine.set_max_operations(10_000);
        engine.set_max_string_size(1_024 * 1024); // 1MB
        engine.set_max_array_size(10_000);
        engine.set_max_map_size(10_000);
        
        // Register basic functions
        engine.register_fn("read", |_field: &str| -> Dynamic { Dynamic::UNIT });
        engine.register_fn("write", |_field: &str, _value: &str| -> bool { true });
        engine.register_fn("write", |_field: &str, _value: i64| -> bool { true });
        engine.register_fn("add", |_field: &str, _value: i64| -> bool { true });
        engine.register_fn("subtract", |_field: &str, _value: i64| -> bool { true });
        engine.register_fn("get_entity_type", || -> String { "".to_string() });
        engine.register_fn("get_entity_id", || -> String { "".to_string() });
        
        Self { engine, store }
    }

    /// Convert a Dynamic result to a boolean
    fn convert_to_bool(&self, result: Dynamic) -> Result<bool, Box<EvalAltResult>> {
        match result {
            // If it's already a boolean, return it
            v if v.is_bool() => Ok(v.as_bool().unwrap_or(false)),
            // If it's an integer, convert to bool (0 = false, non-zero = true)
            v if v.is_int() => Ok(v.as_int().unwrap_or(0) != 0),
            // If it's a float, convert to bool (0.0 = false, non-zero = true)
            v if v.is_float() => Ok(v.as_float().unwrap_or(0.0) != 0.0),
            // If it's a string, check if it's non-empty
            v if v.is_string() => Ok(!v.into_string().unwrap_or_default().is_empty()),
            // If it's an array, check if it's non-empty
            v if v.is_array() => Ok(!v.into_array().unwrap_or_default().is_empty()),
            // If it's UNIT (void), return false
            v if v.is_unit() => Ok(false),
            // For any other type, return true (exists)
            _ => Ok(true),
        }
    }

    /// Execute a script with the given context - returns boolean result
    pub fn execute(&self, script: &str, context: ScriptContext) -> Result<bool, Box<EvalAltResult>> {
        let result = self.execute_raw(script, context)?;
        self.convert_to_bool(result)
    }

    /// Execute a script with the given context - returns raw Dynamic result  
    pub fn execute_raw(&self, script: &str, context: ScriptContext) -> Result<Dynamic, Box<EvalAltResult>> {
        let mut scope = Scope::new();
        
        // Add entity context variables if available
        if let Some(entity_id) = &context.entity_id {
            scope.push("entity_type", entity_id.get_type().to_string());
            scope.push("entity_id", entity_id.to_string());
            scope.push("this", entity_id.to_string());
            scope.push("me", entity_id.to_string());
            
            // Create a modified script that defines the get functions inline
            let entity_type_str = entity_id.get_type().to_string();
            let entity_id_str = entity_id.to_string();
            
            let enhanced_script = format!(r#"
                fn get_entity_type() {{ "{}" }}
                fn get_entity_id() {{ "{}" }}
                {}
            "#, entity_type_str, entity_id_str, script);
            
            self.engine.eval_with_scope(&mut scope, &enhanced_script)
        } else {
            let enhanced_script = format!(r#"
                fn get_entity_type() {{ "" }}
                fn get_entity_id() {{ "" }}
                {}
            "#, script);
            
            self.engine.eval_with_scope(&mut scope, &enhanced_script)
        }
    }

    /// Execute a compiled script with the given context - returns boolean result
    pub fn execute_ast(&self, ast: &AST, context: ScriptContext) -> Result<bool, Box<EvalAltResult>> {
        let result = self.execute_ast_raw(ast, context)?;
        self.convert_to_bool(result)
    }

    /// Execute a compiled script with the given context - returns raw Dynamic result
    pub fn execute_ast_raw(&self, ast: &AST, context: ScriptContext) -> Result<Dynamic, Box<EvalAltResult>> {
        let mut scope = Scope::new();
        
        // Add entity context variables if available
        if let Some(entity_id) = &context.entity_id {
            scope.push("entity_type", entity_id.get_type().to_string());
            scope.push("entity_id", entity_id.to_string());
            scope.push("this", entity_id.to_string());
            scope.push("me", entity_id.to_string());
        }
        
        self.engine.eval_ast_with_scope(&mut scope, ast)
    }

    /// Compile a script for repeated execution
    pub fn compile(&self, script: &str) -> Result<AST, Box<EvalAltResult>> {
        match self.engine.compile(script) {
            Ok(ast) => Ok(ast),
            Err(e) => Err(format!("Compilation error: {}", e).into()),
        }
    }

    /// Check if a notification should trigger by running a script
    pub fn should_trigger_notification(
        &self,
        script: &str,
        notification: &Notification,
    ) -> Result<bool, Box<EvalAltResult>> {
        let mut scope = Scope::new();
        
        scope.push("entity_id", notification.entity_id.to_string());
        scope.push("field_type", notification.field_type.to_string());
        scope.push("current_value", value_to_dynamic(&notification.current_value));
        scope.push("previous_value", value_to_dynamic(&notification.previous_value));
        
        let result = self.engine.eval_with_scope::<Dynamic>(&mut scope, script)?;
        self.convert_to_bool(result)
    }
}

// Helper function for type conversion
fn value_to_dynamic(value: &Value) -> Dynamic {
    match value {
        Value::Bool(b) => (*b).into(),
        Value::Int(i) => (*i).into(),
        Value::Float(f) => (*f).into(),
        Value::String(s) => s.clone().into(),
        Value::Blob(b) => {
            let array: Array = b.iter().map(|&byte| (byte as i64).into()).collect();
            array.into()
        }
        Value::EntityReference(Some(entity_id)) => entity_id.to_string().into(),
        Value::EntityReference(None) => Dynamic::UNIT,
        Value::EntityList(list) => {
            let array: Array = list.iter().map(|id| id.to_string().into()).collect();
            array.into()
        }
        Value::Choice(choice) => (*choice).into(),
        Value::Timestamp(ts) => {
            ts.duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64
        }.into(),
    }
}
