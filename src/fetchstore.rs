//! Fetch gRPC client for HTTP fetching from Python code.
//!
//! Provides fetch(url, ...) as a Python builtin. QNTX performs the HTTP
//! request and attests the result — Python handlers never make outbound
//! network calls directly.

use crate::proto::fetch_service_client::FetchServiceClient;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::cell::RefCell;
use std::sync::Arc;
use tonic::transport::Channel;

thread_local! {
    static CURRENT_CLIENT: RefCell<Option<SharedFetchClient>> = const { RefCell::new(None) };
}

#[derive(Debug, Clone)]
pub struct FetchConfig {
    pub endpoint: String,
    pub auth_token: String,
}

pub struct FetchClient {
    config: FetchConfig,
}

#[derive(Debug)]
struct FetchResult {
    body: String,
    status_code: i32,
    attestation_id: String,
}

impl FetchClient {
    pub fn new(config: FetchConfig) -> Self {
        Self { config }
    }

    fn fetch(
        &self,
        url: &str,
        subjects: Vec<String>,
        predicate: &str,
        context: &str,
        fresh: bool,
        actor: &str,
        source: &str,
    ) -> Result<FetchResult, String> {
        let endpoint = self.config.endpoint.clone();
        let auth_token = self.config.auth_token.clone();
        let request = qntx_proto::FetchRequest {
            auth_token,
            url: url.to_string(),
            subjects,
            predicate: predicate.to_string(),
            context: context.to_string(),
            fresh,
            actor: actor.to_string(),
            source: source.to_string(),
        };

        let response = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| format!("failed to create runtime: {}", e))?;

            rt.block_on(async {
                let endpoint_uri =
                    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
                        endpoint.clone()
                    } else {
                        format!("http://{}", endpoint)
                    };

                let channel = Channel::from_shared(endpoint_uri)
                    .map_err(|e| format!("invalid endpoint: {}", e))?
                    .connect()
                    .await
                    .map_err(|e| format!("connection failed: {}", e))?;

                let mut client = FetchServiceClient::new(channel);
                client
                    .fetch(request)
                    .await
                    .map_err(|e| format!("gRPC error: {}", e))
            })
        })
        .join()
        .map_err(|e| format!("thread panicked: {:?}", e))??
        .into_inner();

        if !response.success {
            return Err(response.error);
        }

        Ok(FetchResult {
            body: response.body,
            status_code: response.status_code,
            attestation_id: response.attestation_id,
        })
    }
}

pub type SharedFetchClient = Arc<parking_lot::Mutex<Option<FetchClient>>>;

pub fn new_shared_client() -> SharedFetchClient {
    Arc::new(parking_lot::Mutex::new(None))
}

pub fn init_shared_client(shared: &SharedFetchClient, config: FetchConfig) {
    let mut guard = shared.lock();
    *guard = Some(FetchClient::new(config));
}

pub fn set_current_client(client: SharedFetchClient) {
    CURRENT_CLIENT.with(|c| {
        *c.borrow_mut() = Some(client);
    });
}

pub fn clear_current_client() {
    CURRENT_CLIENT.with(|c| {
        *c.borrow_mut() = None;
    });
}

/// Python-callable fetch function.
///
/// Returns a dict with body, status_code, and attestation_id.
#[pyfunction]
#[pyo3(signature = (url, subjects=None, predicate="http:get", context="", fresh=false, actor="", source=""))]
fn fetch(
    py: Python<'_>,
    url: String,
    subjects: Option<Vec<String>>,
    predicate: &str,
    context: &str,
    fresh: bool,
    actor: &str,
    source: &str,
) -> PyResult<PyObject> {
    let subjects = subjects.unwrap_or_default();

    let result = CURRENT_CLIENT.with(|c| {
        let client_opt = c.borrow();
        match client_opt.as_ref() {
            Some(shared) => {
                let mut guard = shared.lock();
                match guard.as_mut() {
                    Some(client) => {
                        client.fetch(&url, subjects, predicate, context, fresh, actor, source)
                    }
                    None => Err("Fetch client not initialized".to_string()),
                }
            }
            None => Err("Fetch client not available in this context".to_string()),
        }
    });

    match result {
        Ok(r) => {
            let dict = PyDict::new(py);
            dict.set_item("body", &r.body)?;
            dict.set_item("status_code", r.status_code)?;
            dict.set_item("attestation_id", &r.attestation_id)?;
            Ok(dict.into())
        }
        Err(e) => Err(pyo3::exceptions::PyRuntimeError::new_err(e)),
    }
}

/// Inject fetch function into Python globals.
pub fn inject_fetch_function(
    py: Python<'_>,
    globals: &pyo3::Bound<'_, pyo3::types::PyDict>,
) -> PyResult<()> {
    globals.set_item("fetch", wrap_pyfunction!(fetch, py)?)?;
    Ok(())
}
