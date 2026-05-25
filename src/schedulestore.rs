//! Schedule gRPC client for managing Pulse schedules from Python code.
//!
//! Provides pause_schedule(), resume_schedule(), delete_schedule() as Python
//! builtins, following the same thread-local pattern as atsstore's attest().

use crate::proto::schedule_service_client::ScheduleServiceClient;
use pyo3::prelude::*;
use std::cell::RefCell;
use std::sync::Arc;
use tonic::transport::Channel;
use tracing::error;

thread_local! {
    static CURRENT_CLIENT: RefCell<Option<SharedScheduleClient>> = const { RefCell::new(None) };
}

/// Schedule client configuration
#[derive(Debug, Clone)]
pub struct ScheduleConfig {
    pub endpoint: String,
    pub auth_token: String,
}

/// Schedule client wrapper with blocking operations for PyO3 compatibility
pub struct ScheduleClient {
    config: ScheduleConfig,
}

impl ScheduleClient {
    pub fn new(config: ScheduleConfig) -> Self {
        Self { config }
    }

    fn call_schedule_rpc<F, R>(&self, op: &str, schedule_id: &str, rpc: F) -> Result<(), String>
    where
        F: FnOnce(ScheduleServiceClient<Channel>, String) -> R + Send + 'static,
        R: std::future::Future<Output = Result<tonic::Response<ScheduleRpcResponse>, tonic::Status>>
            + Send,
    {
        let endpoint = self.config.endpoint.clone();
        let auth_token = self.config.auth_token.clone();
        let schedule_id = schedule_id.to_string();
        let op = op.to_string();

        let result = std::thread::spawn(move || {
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

                let client = ScheduleServiceClient::new(channel);
                rpc(client, auth_token)
                    .await
                    .map_err(|e| format!("gRPC error: {}", e))
            })
        })
        .join()
        .map_err(|e| format!("thread panicked: {:?}", e))??
        .into_inner();

        if !result.success {
            error!("Failed to {} schedule {}: {}", op, schedule_id, result.error);
            return Err(result.error);
        }

        Ok(())
    }

    pub fn pause(&self, schedule_id: &str) -> Result<(), String> {
        let sid = schedule_id.to_string();
        self.call_schedule_rpc("pause", schedule_id, move |mut client, auth_token| async move {
            let resp = client
                .pause_schedule(qntx_proto::PauseScheduleRequest {
                    auth_token,
                    schedule_id: sid,
                })
                .await?;
            let inner = resp.into_inner();
            Ok(tonic::Response::new(ScheduleRpcResponse {
                success: inner.success,
                error: inner.error,
            }))
        })
    }

    pub fn resume(&self, schedule_id: &str) -> Result<(), String> {
        let sid = schedule_id.to_string();
        self.call_schedule_rpc("resume", schedule_id, move |mut client, auth_token| async move {
            let resp = client
                .resume_schedule(qntx_proto::ResumeScheduleRequest {
                    auth_token,
                    schedule_id: sid,
                })
                .await?;
            let inner = resp.into_inner();
            Ok(tonic::Response::new(ScheduleRpcResponse {
                success: inner.success,
                error: inner.error,
            }))
        })
    }

    pub fn delete(&self, schedule_id: &str) -> Result<(), String> {
        let sid = schedule_id.to_string();
        self.call_schedule_rpc("delete", schedule_id, move |mut client, auth_token| async move {
            let resp = client
                .delete_schedule(qntx_proto::DeleteScheduleRequest {
                    auth_token,
                    schedule_id: sid,
                })
                .await?;
            let inner = resp.into_inner();
            Ok(tonic::Response::new(ScheduleRpcResponse {
                success: inner.success,
                error: inner.error,
            }))
        })
    }
}

/// Unified response for schedule RPCs (all have success + error)
struct ScheduleRpcResponse {
    success: bool,
    error: String,
}

pub type SharedScheduleClient = Arc<parking_lot::Mutex<Option<ScheduleClient>>>;

pub fn new_shared_client() -> SharedScheduleClient {
    Arc::new(parking_lot::Mutex::new(None))
}

pub fn init_shared_client(shared: &SharedScheduleClient, config: ScheduleConfig) {
    let mut guard = shared.lock();
    *guard = Some(ScheduleClient::new(config));
}

pub fn set_current_client(client: SharedScheduleClient) {
    CURRENT_CLIENT.with(|c| {
        *c.borrow_mut() = Some(client);
    });
}

pub fn clear_current_client() {
    CURRENT_CLIENT.with(|c| {
        *c.borrow_mut() = None;
    });
}

fn with_client<F, R>(op: &str, f: F) -> PyResult<R>
where
    F: FnOnce(&mut ScheduleClient) -> Result<R, String>,
{
    CURRENT_CLIENT.with(|c| {
        let client_opt = c.borrow();
        match client_opt.as_ref() {
            Some(shared) => {
                let mut guard = shared.lock();
                match guard.as_mut() {
                    Some(client) => f(client).map_err(pyo3::exceptions::PyRuntimeError::new_err),
                    None => Err(pyo3::exceptions::PyRuntimeError::new_err(
                        "Schedule client not initialized",
                    )),
                }
            }
            None => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Schedule client not available (cannot {})",
                op
            ))),
        }
    })
}

#[pyfunction]
pub fn pause_schedule(schedule_id: String) -> PyResult<()> {
    with_client("pause_schedule", |client| client.pause(&schedule_id))
}

#[pyfunction]
pub fn resume_schedule(schedule_id: String) -> PyResult<()> {
    with_client("resume_schedule", |client| client.resume(&schedule_id))
}

#[pyfunction]
pub fn delete_schedule(schedule_id: String) -> PyResult<()> {
    with_client("delete_schedule", |client| client.delete(&schedule_id))
}

/// Inject schedule management functions into Python globals.
pub fn inject_schedule_functions(
    py: Python<'_>,
    globals: &pyo3::Bound<'_, pyo3::types::PyDict>,
) -> PyResult<()> {
    globals.set_item("pause_schedule", wrap_pyfunction!(pause_schedule, py)?)?;
    globals.set_item("resume_schedule", wrap_pyfunction!(resume_schedule, py)?)?;
    globals.set_item("delete_schedule", wrap_pyfunction!(delete_schedule, py)?)?;
    Ok(())
}
