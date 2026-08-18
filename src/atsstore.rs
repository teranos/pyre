//! ATSStore gRPC client for creating attestations from Python code.
//!
//! Provides a blocking wrapper around the ATSStore gRPC client that can be
//! called from PyO3 functions during Python execution.

use crate::proto::{
    ats_store_service_client::AtsStoreServiceClient, AttestationCommand, AttestationFilter,
    GenerateAttestationRequest, GetAttestationsRequest,
};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::transport::Channel;

// Thread-local storage for the ATSStore client during Python execution
thread_local! {
    static CURRENT_CLIENT: RefCell<Option<SharedAtsStoreClient>> = const { RefCell::new(None) };
    // Glyph ID for actor convention: when set, attest() defaults actor to "glyph:{id}"
    static CURRENT_GLYPH_ID: RefCell<Option<String>> = const { RefCell::new(None) };
    // The handler whose code is running. The runtime knows this at the moment of
    // the write, so attest() can name the handler without the handler saying so.
    static CURRENT_HANDLER: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// ATSStore client configuration
#[derive(Debug, Clone)]
pub struct AtsStoreConfig {
    pub endpoint: String,
    pub auth_token: String,
}

/// ATSStore client wrapper with blocking operations for PyO3 compatibility
///
/// TODO: Implement connection pooling - currently creates fresh connection per operation
/// Each thread spawns its own runtime and connection, which works but is inefficient
pub struct AtsStoreClient {
    config: AtsStoreConfig,
}

/// A failure crossing the ATS boundary. `doing` is why this is a type: a bare
/// message reaches Python saying "gRPC error", never saying which call made it.
#[derive(Debug)]
pub enum AtsError {
    /// No client in this execution context at all.
    NoClient { doing: &'static str },
    /// A client exists but was never initialized.
    NotInitialized { doing: &'static str },
    /// The call never reached the node, or its answer never came back.
    Transport { doing: &'static str, cause: String },
    /// The node answered, and refused.
    Refused { doing: &'static str, cause: String },
    /// Nothing named a context: no handler in scope, and none passed.
    Contextless { doing: &'static str },
}

impl std::fmt::Display for AtsError {
    /// Origin first, because that is what a reader is missing.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoClient { doing } => {
                write!(f, "{doing}: no ATSStore client in this context")
            }
            Self::NotInitialized { doing } => {
                write!(f, "{doing}: ATSStore client not initialized")
            }
            Self::Transport { doing, cause } => write!(f, "{doing}: {cause}"),
            Self::Refused { doing, cause } => write!(f, "{doing}: refused: {cause}"),
            Self::Contextless { doing } => write!(
                f,
                "{doing}: no context: this execution is not a discovered handler, \
                 so there is no handler name to stand in — pass contexts=[...]"
            ),
        }
    }
}

impl std::error::Error for AtsError {}

/// Config carries a bare host:port; tonic refuses a URI without a scheme.
async fn connect(
    endpoint: String,
    doing: &'static str,
) -> Result<AtsStoreServiceClient<Channel>, AtsError> {
    let endpoint_uri = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint
    } else {
        format!("http://{}", endpoint)
    };

    let channel = Channel::from_shared(endpoint_uri)
        .map_err(|e| AtsError::Transport {
            doing,
            cause: format!("invalid endpoint: {e}"),
        })?
        .connect()
        .await
        .map_err(|e| AtsError::Transport {
            doing,
            cause: format!("connection failed: {e}"),
        })?;

    Ok(AtsStoreServiceClient::new(channel))
}

/// Named after attest()'s parameters so a handler reads back on the axes it wrote on.
#[derive(Debug, Clone, Default)]
pub struct LastQuery {
    pub subjects: Vec<String>,
    pub predicates: Vec<String>,
    pub contexts: Vec<String>,
    pub actors: Vec<String>,
}

/// QNTX orders query results newest-first, so a limit of 1 is the newest match.
fn last_filter(query: LastQuery) -> AttestationFilter {
    AttestationFilter {
        subjects: query.subjects,
        predicates: query.predicates,
        contexts: query.contexts,
        actors: query.actors,
        time_start: None,
        time_end: None,
        limit: Some(1),
    }
}

impl AtsStoreClient {
    /// Create a new ATSStore client
    pub fn new(config: AtsStoreConfig) -> Self {
        Self { config }
    }

    /// `Ok(None)` means the store holds no match; transport and query failures stay
    /// errors, so a handler never mistakes an unreadable store for absent history.
    pub fn last_attestation(
        &mut self,
        query: LastQuery,
    ) -> Result<Option<AttestationRecord>, AtsError> {
        const DOING: &str = "last";

        let endpoint = self.config.endpoint.clone();
        let request = GetAttestationsRequest {
            auth_token: self.config.auth_token.clone(),
            filter: Some(last_filter(query)),
        };

        let response = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| AtsError::Transport {
                    doing: DOING,
                    cause: format!("failed to create runtime: {e}"),
                })?;

            rt.block_on(async {
                let mut client = connect(endpoint, DOING).await?;
                client
                    .get_attestations(request)
                    .await
                    .map_err(|e| AtsError::Transport {
                        doing: DOING,
                        cause: format!("gRPC error: {e}"),
                    })
            })
        })
        .join()
        .map_err(|e| AtsError::Transport {
            doing: DOING,
            cause: format!("thread panicked: {e:?}"),
        })??
        .into_inner();

        if !response.success {
            return Err(AtsError::Refused {
                doing: DOING,
                cause: response.error,
            });
        }

        Ok(response
            .attestations
            .into_iter()
            .next()
            .map(|a| AttestationRecord {
                id: a.id,
                subjects: a.subjects,
                predicates: a.predicates,
                contexts: a.contexts,
                actors: a.actors,
                timestamp: a.timestamp,
                source: a.source,
                attributes: a
                    .attributes
                    .as_ref()
                    .map(qntx_proto::serde_struct::struct_to_json_map)
                    .unwrap_or_default(),
            }))
    }

    /// Create an attestation with auto-generated ID
    ///
    /// This is the main function called from Python via `attest()`.
    pub fn create_attestation(
        &mut self,
        subjects: Vec<String>,
        predicates: Vec<String>,
        contexts: Vec<String>,
        actors: Option<Vec<String>>,
        attributes: Option<HashMap<String, serde_json::Value>>,
    ) -> Result<AttestationResult, AtsError> {
        const DOING: &str = "attest";

        // Get endpoint for use in spawned thread
        let endpoint = self.config.endpoint.clone();
        let auth_token = self.config.auth_token.clone();

        // Convert attributes to prost Struct if provided
        let attributes =
            attributes.map(|attrs| qntx_proto::serde_struct::json_map_to_struct(&attrs));

        let command = AttestationCommand {
            subjects,
            predicates,
            contexts,
            actors: actors.unwrap_or_default(),
            timestamp: None, // Server will use current time
            attributes,
            source: "python".to_string(),
            source_version: crate::version::version().to_string(),
        };

        let request = GenerateAttestationRequest {
            auth_token,
            command: Some(command),
        };

        // Spawn a separate OS thread with its own runtime (avoid "runtime within runtime" error)
        let response = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| AtsError::Transport {
                    doing: DOING,
                    cause: format!("failed to create runtime: {e}"),
                })?;

            rt.block_on(async {
                let mut client = connect(endpoint, DOING).await?;
                client
                    .generate_and_create_attestation(request)
                    .await
                    .map_err(|e| AtsError::Transport {
                        doing: DOING,
                        cause: format!("gRPC error: {e}"),
                    })
            })
        })
        .join()
        .map_err(|e| AtsError::Transport {
            doing: DOING,
            cause: format!("thread panicked: {e:?}"),
        })??
        .into_inner();

        if !response.success {
            return Err(AtsError::Refused {
                doing: DOING,
                cause: response.error,
            });
        }

        let attestation = response
            .attestation
            .ok_or(AtsError::Refused {
                doing: DOING,
                cause: "server reported success but returned no attestation".to_string(),
            })?;

        Ok(AttestationResult {
            id: attestation.id,
            subjects: attestation.subjects,
            predicates: attestation.predicates,
            contexts: attestation.contexts,
            actors: attestation.actors,
            timestamp: attestation.timestamp,
            source: attestation.source,
        })
    }
}

/// Result of attestation creation, returned to Python
#[derive(Debug, Clone)]
pub struct AttestationResult {
    pub id: String,
    pub subjects: Vec<String>,
    pub predicates: Vec<String>,
    pub contexts: Vec<String>,
    pub actors: Vec<String>,
    pub timestamp: i64,
    pub source: String,
}

/// A stored attestation as read back. Carries attributes, which AttestationResult
/// omits — attributes are where a handler's observed value actually lives.
#[derive(Debug, Clone)]
pub struct AttestationRecord {
    pub id: String,
    pub subjects: Vec<String>,
    pub predicates: Vec<String>,
    pub contexts: Vec<String>,
    pub actors: Vec<String>,
    pub timestamp: i64,
    pub source: String,
    pub attributes: HashMap<String, serde_json::Value>,
}

/// Shared ATSStore client that can be passed to Python execution context
pub type SharedAtsStoreClient = Arc<parking_lot::Mutex<Option<AtsStoreClient>>>;

/// Create a new shared ATSStore client
pub fn new_shared_client() -> SharedAtsStoreClient {
    Arc::new(parking_lot::Mutex::new(None))
}

/// Initialize the shared client with config
pub fn init_shared_client(shared: &SharedAtsStoreClient, config: AtsStoreConfig) {
    let mut guard = shared.lock();
    *guard = Some(AtsStoreClient::new(config));
}

/// Set the current ATSStore client for the executing thread.
/// Called before Python execution to make attest() available.
pub fn set_current_client(client: SharedAtsStoreClient) {
    CURRENT_CLIENT.with(|c| {
        *c.borrow_mut() = Some(client);
    });
}

/// Clear the current ATSStore client after Python execution.
pub fn clear_current_client() {
    CURRENT_CLIENT.with(|c| {
        *c.borrow_mut() = None;
    });
}

/// Set the current glyph ID for actor convention.
/// When set, attest() defaults actor to "glyph:{id}" if no explicit actors provided.
pub fn set_current_glyph_id(glyph_id: Option<String>) {
    CURRENT_GLYPH_ID.with(|g| {
        *g.borrow_mut() = glyph_id;
    });
}

/// Name the handler whose code is about to run, and clear it after. Nothing that
/// is not a discovered handler may inherit the last one's name.
pub fn set_current_handler(handler: Option<String>) {
    CURRENT_HANDLER.with(|h| {
        *h.borrow_mut() = handler;
    });
}

/// The `of` in "subject is predicate of context", with the handler's name in it —
/// every handler in a plugin wrote the plugin's name and read back identical.
fn contexts_for(written: Vec<String>, handler: Option<&str>) -> Vec<String> {
    // What the handler wrote stays. `@watch(predicate, context=...)` matches on
    // the context its upstream carries, so replacing it cuts every watcher wire.
    let mut contexts = written;
    if let Some(name) = handler {
        if !contexts.iter().any(|c| c == name) {
            contexts.push(name.to_string());
        }
    }
    contexts
}

/// Python-callable attest function.
/// Creates an attestation using the current thread's ATSStore client.
#[pyfunction]
#[pyo3(signature = (subjects, predicates, contexts=None, actors=None, attributes=None))]
pub fn attest(
    py: Python<'_>,
    subjects: Vec<String>,
    predicates: Vec<String>,
    contexts: Option<Vec<String>>,
    actors: Option<Vec<String>>,
    attributes: Option<Bound<'_, PyDict>>,
) -> PyResult<PyObject> {
    // Convert Python dict to Rust HashMap if provided
    let attrs: Option<HashMap<String, serde_json::Value>> = match attributes {
        Some(dict) => {
            let mut map = HashMap::new();
            for (key, value) in dict.iter() {
                let k: String = key.extract()?;
                let v = python_value_to_json(py, &value)?;
                map.insert(k, v);
            }
            Some(map)
        }
        None => None,
    };

    let handler = CURRENT_HANDLER.with(|h| h.borrow().clone());
    let contexts = contexts_for(contexts.unwrap_or_default(), handler.as_deref());
    if contexts.is_empty() {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(
            AtsError::Contextless { doing: "attest" }.to_string(),
        ));
    }

    // Default actors to "glyph:{id}" when glyph_id is set and user didn't pass explicit actors
    let actors = match actors {
        Some(a) if !a.is_empty() => Some(a), // User provided explicit actors — use them
        _ => {
            // Check if a glyph ID is set for this execution
            CURRENT_GLYPH_ID.with(|g| g.borrow().as_ref().map(|id| vec![format!("glyph:{}", id)]))
        }
    };

    // Get the current client from thread-local storage
    let result = CURRENT_CLIENT.with(|c| {
        let client_opt = c.borrow();
        match client_opt.as_ref() {
            Some(shared_client) => {
                let mut guard = shared_client.lock();
                match guard.as_mut() {
                    Some(client) => {
                        client.create_attestation(subjects, predicates, contexts, actors, attrs)
                    }
                    None => Err(AtsError::NotInitialized { doing: "attest" }),
                }
            }
            None => Err(AtsError::NoClient { doing: "attest" }),
        }
    });

    match result {
        Ok(attestation) => {
            // Return a dict with the attestation details
            let dict = PyDict::new(py);
            dict.set_item("id", &attestation.id)?;
            dict.set_item("subjects", &attestation.subjects)?;
            dict.set_item("predicates", &attestation.predicates)?;
            dict.set_item("contexts", &attestation.contexts)?;
            dict.set_item("actors", &attestation.actors)?;
            dict.set_item("timestamp", attestation.timestamp)?;
            dict.set_item("source", &attestation.source)?;
            Ok(dict.into())
        }
        Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
    }
}

/// Python-callable last function: the read counterpart of attest().
#[pyfunction]
#[pyo3(signature = (subjects=None, predicates=None, contexts=None, actors=None))]
pub fn last(
    py: Python<'_>,
    subjects: Option<Vec<String>>,
    predicates: Option<Vec<String>>,
    contexts: Option<Vec<String>>,
    actors: Option<Vec<String>>,
) -> PyResult<PyObject> {
    let query = LastQuery {
        subjects: subjects.unwrap_or_default(),
        predicates: predicates.unwrap_or_default(),
        contexts: contexts.unwrap_or_default(),
        actors: actors.unwrap_or_default(),
    };

    let result = CURRENT_CLIENT.with(|c| {
        let client_opt = c.borrow();
        match client_opt.as_ref() {
            Some(shared_client) => {
                let mut guard = shared_client.lock();
                match guard.as_mut() {
                    Some(client) => client.last_attestation(query),
                    None => Err(AtsError::NotInitialized { doing: "last" }),
                }
            }
            None => Err(AtsError::NoClient { doing: "last" }),
        }
    });

    match result {
        Ok(None) => Ok(py.None()),
        Ok(Some(record)) => {
            let dict = PyDict::new(py);
            dict.set_item("id", &record.id)?;
            dict.set_item("subjects", &record.subjects)?;
            dict.set_item("predicates", &record.predicates)?;
            dict.set_item("contexts", &record.contexts)?;
            dict.set_item("actors", &record.actors)?;
            dict.set_item("timestamp", record.timestamp)?;
            dict.set_item("source", &record.source)?;
            dict.set_item("attributes", json_map_to_python(py, &record.attributes)?)?;
            Ok(dict.into())
        }
        Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e.to_string())),
    }
}

/// Round-trips through json.loads because attribute values are arbitrary JSON;
/// same road execution.rs uses to build the `upstream` global.
fn json_map_to_python(
    py: Python<'_>,
    attributes: &HashMap<String, serde_json::Value>,
) -> PyResult<PyObject> {
    let json_str = serde_json::to_string(attributes)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    let json_module = py.import("json")?;
    Ok(json_module.call_method1("loads", (json_str,))?.into())
}

/// Convert a Python value to serde_json::Value
fn python_value_to_json(_py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    if value.is_none() {
        Ok(serde_json::Value::Null)
    } else if let Ok(b) = value.extract::<bool>() {
        Ok(serde_json::Value::Bool(b))
    } else if let Ok(i) = value.extract::<i64>() {
        Ok(serde_json::Value::Number(i.into()))
    } else if let Ok(f) = value.extract::<f64>() {
        Ok(serde_json::json!(f))
    } else if let Ok(s) = value.extract::<String>() {
        Ok(serde_json::Value::String(s))
    } else if let Ok(list) = value.downcast::<pyo3::types::PyList>() {
        let vec: Result<Vec<_>, _> = list.iter().map(|v| python_value_to_json(_py, &v)).collect();
        Ok(serde_json::Value::Array(vec?))
    } else if let Ok(dict) = value.downcast::<PyDict>() {
        let mut map = serde_json::Map::new();
        for (k, v) in dict.iter() {
            let key: String = k.extract()?;
            map.insert(key, python_value_to_json(_py, &v)?);
        }
        Ok(serde_json::Value::Object(map))
    } else {
        // Fallback: convert to string representation
        Ok(serde_json::Value::String(value.str()?.to_string()))
    }
}

/// Both ride CURRENT_CLIENT, so they are injected together or not at all.
pub fn inject_ats_functions(py: Python<'_>, globals: &Bound<'_, PyDict>) -> PyResult<()> {
    globals.set_item("attest", wrap_pyfunction!(attest, py)?)?;
    globals.set_item("last", wrap_pyfunction!(last, py)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A reader who cannot tell which call failed cannot act on the failure,
    /// which is what a bare "gRPC error" leaves them with.
    #[test]
    fn a_fault_names_the_call_that_caused_it() {
        let reading = AtsError::Transport {
            doing: "last",
            cause: "connection failed: refused".to_string(),
        };
        let writing = AtsError::Transport {
            doing: "attest",
            cause: "connection failed: refused".to_string(),
        };

        assert_eq!(reading.to_string(), "last: connection failed: refused");
        assert_eq!(writing.to_string(), "attest: connection failed: refused");
        assert_ne!(reading.to_string(), writing.to_string());
    }

    /// A refusal by the node and a failure to reach it call for different
    /// actions, so they must not read the same.
    #[test]
    fn a_refusal_reads_differently_from_a_transport_failure() {
        let refused = AtsError::Refused {
            doing: "last",
            cause: "invalid filter".to_string(),
        };
        let unreachable = AtsError::NoClient { doing: "last" };

        assert!(refused.to_string().contains("refused"));
        assert!(unreachable.to_string().contains("no ATSStore client"));
    }

    /// Without limit 1 the store applies no limit and ships the whole history back.
    #[test]
    fn last_filter_asks_for_exactly_one_attestation() {
        let filter = last_filter(LastQuery::default());
        assert_eq!(filter.limit, Some(1));
    }

    /// "What did mp004_request_ad_renders file?" had no answer while every
    /// handler in a plugin wrote the same context.
    #[test]
    fn a_handlers_name_becomes_the_context_it_writes_under() {
        let contexts = contexts_for(Vec::new(), Some("mp004_request_ad_renders"));
        assert_eq!(contexts, vec!["mp004_request_ad_renders".to_string()]);
    }

    /// A watcher matches on the context its upstream carries, so a context the
    /// handler chose is a wire — taking it away disconnects whoever watched it.
    #[test]
    fn a_context_the_handler_chose_survives_beside_its_name() {
        let contexts = contexts_for(vec!["my/ctx".to_string()], Some("render_ads"));
        assert_eq!(
            contexts,
            vec!["my/ctx".to_string(), "render_ads".to_string()]
        );
    }

    #[test]
    fn a_handler_that_already_names_itself_is_not_named_twice() {
        let contexts = contexts_for(vec!["render_ads".to_string()], Some("render_ads"));
        assert_eq!(contexts, vec!["render_ads".to_string()]);
    }

    /// The HTTP and gRPC doors carry code, not a handler, so there is no name to
    /// lend and what was written is all there is.
    #[test]
    fn an_execution_that_is_not_a_handler_adds_nothing() {
        let contexts = contexts_for(vec!["my/ctx".to_string()], None);
        assert_eq!(contexts, vec!["my/ctx".to_string()]);
        assert!(contexts_for(Vec::new(), None).is_empty());
    }

    /// An empty context is not a context, and the caller needs to know what to do.
    #[test]
    fn a_contextless_write_says_what_to_pass() {
        let e = AtsError::Contextless { doing: "attest" };
        assert!(e.to_string().contains("contexts=[...]"));
    }

    #[test]
    fn last_filter_passes_every_axis_through_unbounded_in_time() {
        let filter = last_filter(LastQuery {
            subjects: vec!["price:btc".to_string()],
            predicates: vec!["observed".to_string()],
            contexts: vec!["ticker".to_string()],
            actors: vec!["glyph:abc".to_string()],
        });

        assert_eq!(filter.subjects, vec!["price:btc".to_string()]);
        assert_eq!(filter.predicates, vec!["observed".to_string()]);
        assert_eq!(filter.contexts, vec!["ticker".to_string()]);
        assert_eq!(filter.actors, vec!["glyph:abc".to_string()]);
        assert_eq!(filter.time_start, None);
        assert_eq!(filter.time_end, None);
    }
}
