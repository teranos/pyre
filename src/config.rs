//! Plugin configuration types

use std::collections::HashMap;

/// Plugin configuration received during initialization
#[derive(Debug, Clone, Default)]
pub struct PluginConfig {
    /// ATSStore gRPC endpoint
    pub ats_store_endpoint: String,
    /// Queue service gRPC endpoint
    pub queue_endpoint: String,
    /// Schedule service gRPC endpoint
    pub schedule_endpoint: String,
    /// Fetch service gRPC endpoint
    pub fetch_endpoint: String,
    /// Auth token for service calls
    pub auth_token: String,
    /// Custom configuration values
    pub config: HashMap<String, String>,
}

/// Build the configuration schema for the Python plugin
///
/// This defines all configuration fields that the plugin accepts,
/// including their types, descriptions, and validation constraints.
pub fn build_schema() -> HashMap<String, crate::proto::ConfigFieldSchema> {
    use crate::proto::{config_field_schema, ConfigFieldSchema};

    let mut fields = HashMap::new();

    fields.insert(
        "python_paths".to_string(),
        ConfigFieldSchema {
            r#type: "string".to_string(),
            description: "Colon-separated list of Python module search paths".to_string(),
            default_value: String::new(),
            required: false,
            min: None,
            max: None,
            pattern: String::new(),
            element_type: String::new(),
        },
    );

    fields.insert(
        "default_modules".to_string(),
        ConfigFieldSchema {
            r#type: "string".to_string(),
            description: "Comma-separated list of Python modules to auto-import".to_string(),
            default_value: "numpy,pandas,scipy,matplotlib".to_string(),
            required: false,
            min: None,
            max: None,
            pattern: String::new(),
            element_type: String::new(),
        },
    );

    fields.insert(
        "timeout_secs".to_string(),
        ConfigFieldSchema {
            r#type: "number".to_string(),
            description: "Maximum execution time for Python code in seconds".to_string(),
            default_value: "30".to_string(),
            required: false,
            min: Some(config_field_schema::Min::MinValue("1".to_string())),
            max: Some(config_field_schema::Max::MaxValue("300".to_string())),
            pattern: String::new(),
            element_type: String::new(),
        },
    );

    fields.insert(
        "max_workers".to_string(),
        ConfigFieldSchema {
            r#type: "number".to_string(),
            description: "Maximum number of concurrent Python execution workers".to_string(),
            default_value: "4".to_string(),
            required: false,
            min: Some(config_field_schema::Min::MinValue("1".to_string())),
            max: Some(config_field_schema::Max::MaxValue("16".to_string())),
            pattern: String::new(),
            element_type: String::new(),
        },
    );

    fields.insert(
        "enable_debug".to_string(),
        ConfigFieldSchema {
            r#type: "boolean".to_string(),
            description: "Enable debug logging for Python execution".to_string(),
            default_value: "false".to_string(),
            required: false,
            min: None,
            max: None,
            pattern: String::new(),
            element_type: String::new(),
        },
    );

    // Not set in a record: the node hands both when it has a DSN of its own.
    // Naming them is what makes the node hand them (QNTX plugin/grpc).
    fields.insert(
        crate::telemetry::DSN_KEY.to_string(),
        ConfigFieldSchema {
            r#type: "string".to_string(),
            description: "Where logs and errors go: the node's Sentry DSN, handed by the node. Empty ships nothing".to_string(),
            default_value: String::new(),
            required: false,
            min: None,
            max: None,
            pattern: String::new(),
            element_type: String::new(),
        },
    );

    fields.insert(
        crate::telemetry::ENVIRONMENT_KEY.to_string(),
        ConfigFieldSchema {
            r#type: "string".to_string(),
            description: "The Sentry environment events are tagged with: the node's, handed by the node".to_string(),
            default_value: String::new(),
            required: false,
            min: None,
            max: None,
            pattern: String::new(),
            element_type: String::new(),
        },
    );

    fields
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The node hands a plugin the keys its schema names, so pyre names the two
    /// that bind its Sentry client, as the keys it reads them by.
    #[test]
    fn the_schema_names_the_keys_the_node_hands_for_sentry() {
        let schema = build_schema();
        for key in [crate::telemetry::DSN_KEY, crate::telemetry::ENVIRONMENT_KEY] {
            let field = schema
                .get(key)
                .unwrap_or_else(|| panic!("the schema does not name {key}"));
            assert!(!field.required, "{key} must stay optional: no DSN ships nothing");
        }
    }
}
