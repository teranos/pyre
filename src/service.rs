//! gRPC service implementation for the Python plugin
//!
//! Implements the DomainPluginService interface for QNTX.
//!
//! Package management uses uv (preferred) with pip fallback.
//! POST /uv/install and GET /uv/check are the primary endpoints;
//! /pip/install and /pip/check are aliases for backward compatibility.

use crate::atsstore;
use crate::config::PluginConfig;
use crate::engine::PythonEngine;
use crate::handlers::{HandlerContext, PluginState};
use crate::proto::{
    domain_plugin_service_server::DomainPluginService, llm_chat_chunk,
    llm_service_server::LlmService, python_service_server::PythonService, LlmChatChunk,
    LlmChatRequest, LlmChatResponse, LlmServed,
    service_endpoint, ConfigSchemaResponse, ElementDefResponse, Empty, ExecuteJobRequest,
    ExecuteJobResponse, HealthResponse, HttpHeader, HttpRequest, HttpResponse, InitializeRequest,
    InitializeResponse, MetadataResponse, PythonExecuteRequest, PythonExecuteResponse,
    ScheduleInfo, ServiceEndpoint, ShutdownResponse, WatcherRegistration, WebSocketMessage,
};
use crate::version::version;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, warn};

/// Default timeout for Python job execution (5 minutes)
const DEFAULT_TIMEOUT_SECS: u64 = 300;

/// The paths handle_http answers itself, by method.
const PYRE_PATHS: &[(&str, &str)] = &[
    ("POST", "/execute"),
    ("POST", "/evaluate"),
    ("POST", "/execute-file"),
    ("POST", "/uv/install"),
    ("POST", "/pip/install"),
    ("GET", "/uv/check"),
    ("GET", "/pip/check"),
    ("GET", "/version"),
    ("GET", "/modules"),
];

/// Where a service the node runs answers, when it does. A service the node
/// runs without says why instead, and that is no endpoint to dial.
fn answering(endpoint: &Option<ServiceEndpoint>) -> Option<String> {
    match endpoint.as_ref()?.state.as_ref()? {
        service_endpoint::State::Address(address) if !address.is_empty() => Some(address.clone()),
        _ => None,
    }
}

/// Python plugin gRPC service
pub struct PythonPluginService {
    pub(crate) handlers: HandlerContext,
    pub(crate) name: String,
}

impl PythonPluginService {
    /// Create a new Python plugin service
    pub fn new(name: impl Into<String>) -> Result<Self, Box<dyn std::error::Error>> {
        let engine = match PythonEngine::new() {
            Ok(e) => e,
            Err(e) => {
                tracing::error!("Failed to create Python engine: {}", e);
                return Err(format!("Python engine creation failed: {}", e).into());
            }
        };
        let state = Arc::new(RwLock::new(PluginState {
            config: None,
            engine,
            initialized: false,
            default_modules: crate::handlers::DEFAULT_MODULES
                .iter()
                .map(|s| s.to_string())
                .collect(),
            ats_client: atsstore::new_shared_client(),
            schedule_client: crate::schedulestore::new_shared_client(),
            fetch_client: crate::fetchstore::new_shared_client(),
            discovered_handlers: HashMap::new(),
            llm_handler: None,
            rooms_handler: None,
            routes: vec![],
            telemetry: None,
        }));

        Ok(Self {
            handlers: HandlerContext::new(state),
            name: name.into(),
        })
    }

    /// Get Python version for health checks
    fn python_version(&self) -> String {
        self.handlers.python_version()
    }

    /// Discover handler scripts from ATS store
    /// Returns a HashMap of handler_name -> Python code
    async fn discover_handlers_from_config(
        &self,
        config: Option<PluginConfig>,
    ) -> HashMap<String, String> {
        use crate::proto::{
            ats_store_service_client::AtsStoreServiceClient, attestation_filter, AttestationFilter,
            GetAttestationsRequest,
        };
        use tonic::transport::Channel;

        // A plugin's own handlers come with its build; a store's are laid over
        // them, the newer word on the same name.
        let mut found = bundled_handlers();

        // Check if we have config with ATS store endpoint
        let config = match config {
            Some(cfg) if !cfg.ats_store_endpoint.is_empty() => cfg,
            _ => {
                info!("No ATS store endpoint configured: the build's own handlers only");
                return found;
            }
        };

        debug!("Discovering Python handlers from ATS store");

        let endpoint = config.ats_store_endpoint.clone();
        let auth_token = config.auth_token.clone();

        // Query ATS store for handler attestations
        // Filter: predicate="handler" AND context="python"
        // Both, in one query. Results come back newest-first, so the first
        // thing seen for a subject is the current truth about it: code means
        // load, a fault means the last attempt to read it failed.
        let filter = AttestationFilter {
            subjects: vec![],
            predicates: vec!["handler".to_string(), "handler:faulted".to_string()],
            contexts: vec![self.name.clone()],
            actors: vec![],
            from: Some(attestation_filter::From::UnboundedStart(true)),
            to: Some(attestation_filter::To::UnboundedEnd(true)),
            limit: 200,
        };

        let request = GetAttestationsRequest {
            auth_token,
            filter: Some(filter),
        };

        // Connect to ATS store and query
        let result: Result<HashMap<String, String>, String> =
            tokio::task::spawn_blocking(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| format!("failed to create runtime: {}", e))?;

                rt.block_on(async {
                    // Ensure endpoint has http:// scheme
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

                    let mut client = AtsStoreServiceClient::new(channel);
                    let response = client
                        .get_attestations(request)
                        .await
                        .map_err(|e| format!("gRPC error: {}", e))?
                        .into_inner();

                    if !response.success {
                        return Err(format!("Query failed: {}", response.error));
                    }

                    // Extract handler names and code from attestations
                    let mut handlers = HashMap::new();
                    let mut settled: HashSet<String> = HashSet::new();

                    for attestation in response.attestations {
                        let Some(handler_name) = attestation.subjects.first() else {
                            continue;
                        };
                        // Newest-first, so the first word on a subject is the
                        // last word, whichever predicate it came under.
                        if settled.contains(handler_name) {
                            continue;
                        }

                        if attestation
                            .predicates
                            .iter()
                            .any(|p| p == "handler:faulted")
                        {
                            settled.insert(handler_name.clone());
                            warn!(
                                "Handler {} is faulted and will not be loaded — stoke it again to clear",
                                handler_name
                            );
                            continue;
                        }

                        // Extract Python code from attributes Struct
                        if let Some(ref attrs_struct) = attestation.attributes {
                            let attrs = qntx_proto::serde_struct::struct_to_json_map(attrs_struct);
                            if let Some(serde_json::Value::String(code)) = attrs.get("code") {
                                settled.insert(handler_name.clone());
                                handlers.insert(handler_name.clone(), code.clone());
                            } else {
                                warn!(
                                    "Handler {} attributes missing 'code' field, skipping",
                                    handler_name
                                );
                            }
                        } else {
                            warn!("Handler {} has no attributes, skipping", handler_name);
                        }
                    }

                    Ok(handlers)
                })
            })
            .await
            .unwrap_or_else(|e| Err(format!("task panicked: {:?}", e)));

        match result {
            Ok(handlers) => {
                debug!(
                    "Discovered {} handler(s) from ATS store: {:?}",
                    handlers.len(),
                    handlers.keys().collect::<Vec<_>>()
                );
                found.extend(handlers);
                found
            }
            Err(e) => {
                warn!("Failed to discover handlers from ATS store: {}", e);
                found
            }
        }
    }
}

/// The handlers a plugin's build carries: each handlers/<name>.py beside the
/// binary, by its name. A plugin that stands in no namespace has these alone.
pub(crate) fn bundled_handlers() -> HashMap<String, String> {
    let Some(dir) = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|d| d.join("handlers")))
    else {
        return HashMap::new();
    };
    handlers_in(&dir)
}

/// Each <name>.py directly in dir, by its name.
pub(crate) fn handlers_in(dir: &std::path::Path) -> HashMap<String, String> {
    let mut handlers = HashMap::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return handlers;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("py") {
            continue;
        }
        let Some(name) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        match std::fs::read_to_string(&path) {
            Ok(code) => {
                info!("Handler {} comes with the build: {}", name, path.display());
                handlers.insert(name.to_string(), code);
            }
            Err(e) => warn!("Handler {} beside the binary did not read: {}", path.display(), e),
        }
    }
    handlers
}

impl Default for PythonPluginService {
    fn default() -> Self {
        Self::new("python").expect("Failed to create PythonPluginService")
    }
}

#[tonic::async_trait]
impl DomainPluginService for PythonPluginService {
    /// Return plugin metadata
    async fn metadata(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<MetadataResponse>, Status> {
        debug!("Metadata request received");
        Ok(Response::new(MetadataResponse {
            name: self.name.clone(),
            version: version().to_string(),
            qntx_version: ">=0.1.0".to_string(),
            description: "Python execution plugin - run Python code within QNTX".to_string(),
            author: "QNTX Contributors".to_string(),
            license: "MIT".to_string(),
        }))
    }

    /// Initialize the plugin with service endpoints
    async fn initialize(
        &self,
        request: Request<InitializeRequest>,
    ) -> Result<Response<InitializeResponse>, Status> {
        let req = request.into_inner();
        debug!(
            "Initializing Python plugin (ATS: {}, Queue: {})",
            req.ats_store_endpoint, req.queue_endpoint
        );

        // Clone config for later use after dropping lock
        let (state_config, py_version) = {
            let mut state = self.handlers.state.write();

            // First, so that everything Initialize says from here on is shipped.
            // A rebind on a second Initialize replaces the client; the old
            // guard drops with the assignment and closes its pipe.
            if let Some(dsn) = req.config.get(crate::telemetry::DSN_KEY).filter(|d| !d.is_empty()) {
                let environment = req.config.get(crate::telemetry::ENVIRONMENT_KEY).map(String::as_str);
                state.telemetry = Some(crate::telemetry::bind(&self.name, dsn, environment));
            }

            // Store configuration
            state.config = Some(PluginConfig {
                ats_store_endpoint: req.ats_store_endpoint.clone(),
                queue_endpoint: req.queue_endpoint,
                schedule_endpoint: req.schedule_endpoint.clone(),
                fetch_endpoint: answering(&req.fetch_endpoint).unwrap_or_default(),
                auth_token: req.auth_token.clone(),
                config: req.config,
            });

            // Initialize ATSStore client if endpoint is provided
            if !req.ats_store_endpoint.is_empty() {
                debug!("Initializing ATSStore client for Python attestation support");
                atsstore::init_shared_client(
                    &state.ats_client,
                    atsstore::AtsStoreConfig {
                        endpoint: req.ats_store_endpoint,
                        auth_token: req.auth_token.clone(),
                    },
                );
            }

            // Initialize Schedule client if endpoint is provided
            if !req.schedule_endpoint.is_empty() {
                debug!("Initializing Schedule client for Python schedule management");
                crate::schedulestore::init_shared_client(
                    &state.schedule_client,
                    crate::schedulestore::ScheduleConfig {
                        endpoint: req.schedule_endpoint,
                        auth_token: req.auth_token.clone(),
                    },
                );
            }

            // Initialize Fetch client if endpoint is provided
            if let Some(endpoint) = answering(&req.fetch_endpoint) {
                debug!("Initializing Fetch client for Python HTTP fetching");
                crate::fetchstore::init_shared_client(
                    &state.fetch_client,
                    crate::fetchstore::FetchConfig {
                        endpoint,
                        auth_token: req.auth_token,
                    },
                );
            }

            // Initialize Python engine with custom paths if provided
            let python_paths: Vec<String> = state
                .config
                .as_ref()
                .and_then(|c| c.config.get("python_paths"))
                .map(|p| p.split(':').map(String::from).collect())
                .unwrap_or_default();

            // Override default modules if provided in config
            if let Some(modules_str) = state
                .config
                .as_ref()
                .and_then(|c| c.config.get("default_modules"))
            {
                state.default_modules = modules_str
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .collect();
                info!(
                    "Using configured default modules: {:?}",
                    state.default_modules
                );
            }

            // Named for the plugin, so two instances of pyre never install
            // into each other. Overridable for hosts where /var/lib is not
            // writable by whoever runs the plugin.
            let site_dir = state
                .config
                .as_ref()
                .and_then(|c| c.config.get("site_dir"))
                .cloned()
                .unwrap_or_else(|| format!("/var/lib/qntx/{}-site", self.name));

            if let Err(e) = state.engine.initialize(python_paths, Some(site_dir)) {
                error!("Failed to initialize Python engine: {}", e);
                return Err(Status::internal(format!(
                    "Failed to initialize Python engine: {}",
                    e
                )));
            }

            let config = state.config.as_ref().map(|c| c.config.clone()).unwrap_or_default();
            state.engine.set_plugin_config(&config);

            state.initialized = true;
            let py_version = state.engine.python_version();

            // Clone config before dropping lock
            (state.config.clone(), py_version)
        }; // Lock automatically dropped here

        // Discover handler scripts from ATS store
        let discovered_handlers = self.discover_handlers_from_config(state_config).await;

        // Store discovered handlers in plugin state
        {
            let mut state = self.handlers.state.write();
            state.discovered_handlers = discovered_handlers.clone();
        }
        let mut faulted = self.declare(&discovered_handlers);

        // Announce async handler capabilities
        // Start with built-in handlers
        let mut handler_names = vec![format!("{}.script", self.name)];

        // Extract @watch and @schedule decorator metadata from discovered handlers
        let mut watchers = vec![];
        let mut schedules = vec![];
        let mut sorted_handlers: Vec<_> = discovered_handlers.keys().collect();
        sorted_handlers.sort();
        for handler_name in &sorted_handlers {
            handler_names.push(format!("{}.{}", self.name, handler_name));

            // Faulted already, by reading what it serves: running the module
            // again would fault it a second time for the same reason.
            if faulted.iter().any(|(n, _)| n == *handler_name) {
                continue;
            }

            if let Some(code) = discovered_handlers.get(*handler_name) {
                let state = self.handlers.state.read();

                // A handler whose decorators cannot be read is faulted, not
                // fatal. Returning here failed Initialize for every handler,
                // and QNTX then pruned every schedule the plugin ever declared.
                let read = state
                    .engine
                    .extract_watchers(code)
                    .and_then(|w| state.engine.extract_schedules(code).map(|s| (w, s)));

                let (handler_watchers, handler_schedules) = match read {
                    Ok(pair) => pair,
                    Err(e) => {
                        error!("handler {handler_name} did not load: {e}");
                        faulted.push((handler_name.to_string(), e.to_string()));
                        continue;
                    }
                };

                for w in handler_watchers {
                    let watcher_id = format!("{}-{}", handler_name, w.handler_fn);
                    let watcher_handler = format!("{}.{}", self.name, handler_name);
                    info!(
                        "Watcher: {} watches {:?} in {:?} via {}",
                        watcher_id, w.predicates, w.contexts, watcher_handler
                    );
                    watchers.push(WatcherRegistration {
                        id: watcher_id,
                        handler_name: watcher_handler,
                        predicates: w.predicates,
                        contexts: w.contexts,
                        subjects: vec![],
                        actors: vec![],
                        max_fires_per_second: 1,
                    });
                }

                for s in handler_schedules {
                    let schedule_handler = format!("{}.{}", self.name, handler_name);
                    info!(
                        "Schedule: {} every {}s via {}",
                        s.handler_fn, s.interval_seconds, schedule_handler
                    );
                    schedules.push(ScheduleInfo {
                        handler_name: schedule_handler,
                        interval_seconds: s.interval_seconds,
                        enabled_by_default: true,
                        description: s.description,
                    });
                }
            }
        }

        self.attest_faults(&faulted);

        // Register a watcher on handler attestations in our own context so that
        // attest.fish hot-deploys take effect without a plugin restart.
        // FIXME(tier-2): when the new handler code changes @watch predicates/contexts,
        // the watcher registrations with QNTX are stale (they were set during initialize).
        // Full hot reload requires either a dynamic UpdateWatchers RPC or a re-initialize.
        let reload_handler = format!("{}.__reload", self.name);
        handler_names.push(reload_handler.clone());
        watchers.push(WatcherRegistration {
            id: format!("{}-handler-reload", self.name),
            handler_name: reload_handler,
            predicates: vec!["handler".to_string()],
            contexts: vec![self.name.clone()],
            subjects: vec![],
            actors: vec![],
            max_fires_per_second: 1,
        });
        info!("Watcher: {}-handler-reload watches [\"handler\"] in [\"{}\"] (hot reload)", self.name, self.name);

        let packages = {
            let state = self.handlers.state.read();
            state.engine.installed_packages()
        };
        if packages.is_empty() {
            info!(
                "Python plugin initialized (Python {}) — {} handlers, {} watchers, {} schedules, no packages",
                py_version,
                handler_names.len(),
                watchers.len(),
                schedules.len()
            );
        } else {
            info!(
                "Python plugin initialized (Python {}) — {} handlers, {} watchers, {} schedules, {} packages: {}",
                py_version,
                handler_names.len(),
                watchers.len(),
                schedules.len(),
                packages.len(),
                packages.join(", ")
            );
        }

        // Said only by a plugin whose Python declares @llm, so the node
        // routes LLM calls to no pyre plugin that cannot answer them.
        let llm_provider = self.handlers.state.read().llm_handler.is_some();
        // "rooms will be plugin provided": said only by a plugin whose Python
        // declares @rooms.
        let rooms_provider = self.handlers.state.read().rooms_handler.is_some();

        Ok(Response::new(InitializeResponse {
            handler_names,
            schedules,
            watchers,
            llm_provider,
            rooms_provider,
            python_provider: true,
            ..Default::default()
        }))
    }

    /// Shutdown the plugin
    async fn shutdown(&self, _request: Request<Empty>) -> Result<Response<ShutdownResponse>, Status> {
        info!("Shutting down Python plugin");
        let mut state = self.handlers.state.write();
        state.initialized = false;
        state.config = None;
        // A stopped plugin serves nothing it declared; Initialize reads the
        // declarations again.
        state.llm_handler = None;
        state.rooms_handler = None;
        state.routes.clear();
        // The config the node resolved, secrets among it, goes with them.
        state.engine.set_plugin_config(&HashMap::new());
        Ok(Response::new(ShutdownResponse { stopped: true }))
    }

    /// Handle HTTP requests - routes to appropriate handler
    async fn handle_http(
        &self,
        request: Request<HttpRequest>,
    ) -> Result<Response<HttpResponse>, Status> {
        let req = request.into_inner();
        // Strip query string from path before routing
        let (path, query) = req.path.split_once('?').unwrap_or((&req.path, ""));
        let method = &req.method;

        debug!("HTTP request: {} {}", method, path);

        // A route a handler declares is answered by it, body untouched:
        // before the JSON parse below, which pyre's own paths need and a
        // declared route's body need not pass.
        if let Some((handler, function)) = self.route_for(method, path) {
            let (path, query) = (path.to_string(), query.to_string());
            return Ok(Response::new(
                self.answer_route(handler, function, &req, &path, &query).await,
            ));
        }

        // Parse request body
        let body: serde_json::Value = if req.body.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&req.body)
                .map_err(|e| Status::invalid_argument(format!("Invalid JSON body: {}", e)))?
        };

        // Route to handler
        let result = match (method.as_str(), path) {
            // Python execution endpoints
            ("POST", "/execute") => self.handlers.handle_execute(body).await,
            ("POST", "/evaluate") => self.handlers.handle_evaluate(body).await,
            ("POST", "/execute-file") => self.handlers.handle_execute_file(body).await,

            // Package management (uv preferred, pip fallback)
            ("POST", "/uv/install") | ("POST", "/pip/install") => {
                self.handlers.handle_pip_install(body).await
            }
            ("GET", "/uv/check") | ("GET", "/pip/check") => {
                self.handlers.handle_pip_check(body).await
            }

            // Info endpoints
            ("GET", "/version") => self.handlers.handle_version().await,
            ("GET", "/modules") => self.handlers.handle_modules(body).await,

            _ => Err(Status::not_found(format!(
                "Unknown endpoint: {} {}",
                method, path
            ))),
        };

        match result {
            Ok(response) => Ok(Response::new(response)),
            Err(status) => {
                let error_body = serde_json::json!({
                    "error": status.message()
                });
                Ok(Response::new(HttpResponse {
                    status_code: match status.code() {
                        tonic::Code::NotFound => 404,
                        tonic::Code::InvalidArgument => 400,
                        tonic::Code::Internal => 500,
                        tonic::Code::Unavailable => 503,
                        _ => 500,
                    },
                    headers: vec![HttpHeader {
                        name: "Content-Type".to_string(),
                        values: vec!["application/json".to_string()],
                    }],
                    // An empty body would turn a described failure into a
                    // blank 500, losing the description just built.
                    body: serde_json::to_vec(&error_body).map_err(|e| {
                        Status::internal(format!("failed to serialize the error body: {e}"))
                    })?,
                }))
            }
        }
    }

    /// Handle WebSocket connections (not supported)
    type HandleWebSocketStream =
        Pin<Box<dyn Stream<Item = Result<WebSocketMessage, Status>> + Send>>;

    async fn handle_web_socket(
        &self,
        _request: Request<Streaming<WebSocketMessage>>,
    ) -> Result<Response<Self::HandleWebSocketStream>, Status> {
        warn!("WebSocket not supported by Python plugin");
        Err(Status::unimplemented(
            "WebSocket not supported by Python plugin",
        ))
    }

    /// Check plugin health
    async fn health(&self, _request: Request<Empty>) -> Result<Response<HealthResponse>, Status> {
        let state = self.handlers.state.read();
        let healthy = state.initialized;

        let mut details = HashMap::new();
        details.insert(self.name.clone(), self.python_version());

        Ok(Response::new(HealthResponse {
            healthy,
            message: if healthy {
                format!("Python {}", self.python_version())
            } else {
                "Not initialized".to_string()
            },
            details,
        }))
    }

    /// Return configuration schema
    async fn config_schema(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<ConfigSchemaResponse>, Status> {
        debug!("ConfigSchema request received");
        Ok(Response::new(ConfigSchemaResponse {
            fields: crate::config::build_schema(),
        }))
    }

    /// Register custom element types (none for Python plugin)
    async fn register_elements(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<ElementDefResponse>, Status> {
        Ok(Response::new(ElementDefResponse { elements: vec![] }))
    }

    /// Execute an async job
    /// Routes to appropriate handler based on handler_name
    async fn execute_job(
        &self,
        request: Request<ExecuteJobRequest>,
    ) -> Result<Response<ExecuteJobResponse>, Status> {
        let req = request.into_inner();

        debug!(
            "ExecuteJob request: job_id={}, handler={}",
            req.job_id, req.handler_name
        );

        // Clone handler name to avoid borrow issues
        let handler_name = req.handler_name.clone();

        // Route to handler based on handler_name
        let script_handler = format!("{}.script", self.name);
        let reload_handler = format!("{}.__reload", self.name);
        let prefix = format!("{}.", self.name);

        if handler_name == reload_handler {
            self.handle_handler_reload().await
        } else if handler_name == script_handler {
            self.execute_python_script_job(req).await
        } else if let Some(stripped) = handler_name.strip_prefix(&prefix) {
            self.execute_discovered_handler_job(req, stripped).await
        } else {
            Err(Status::not_found(format!(
                "Unknown handler: {}",
                handler_name
            )))
        }
    }
}

#[tonic::async_trait]
impl PythonService for PythonPluginService {
    async fn execute(
        &self,
        request: Request<PythonExecuteRequest>,
    ) -> Result<Response<PythonExecuteResponse>, Status> {
        let req = request.into_inner();

        if req.code.is_empty() {
            return Err(Status::invalid_argument("Missing 'code' field"));
        }

        let upstream: Option<serde_json::Value> = if req.upstream_attestation.is_empty() {
            None
        } else {
            // A handler that reads `upstream` would see None and conclude it
            // was not triggered by anything, which is not what happened.
            Some(serde_json::from_slice(&req.upstream_attestation).map_err(|e| {
                Status::invalid_argument(format!("upstream attestation is not valid JSON: {e}"))
            })?)
        };

        let config = crate::engine::ExecutionConfig {
            timeout_secs: 30,
            ..Default::default()
        };

        let shared = self.handlers.state.clone();
        let result = crate::handlers::off_the_workers(move || {
            // 「[Sonnet 5.5] Set element ID for actor convention. It is thread-local, so it」
            // 「[Sonnet 5.5] is set on the thread that runs the code.」
            if !req.element_id.is_empty() {
                crate::atsstore::set_current_glyph_id(Some(req.element_id.clone()));
            }

            let state = shared.read();
            let result = state.engine.execute_with_ats(
                &req.code,
                &config,
                Some(state.ats_client.clone()),
                upstream.as_ref(),
            );

            crate::atsstore::set_current_glyph_id(None);
            result
        })
        .await?;

        // Empty bytes read as "the handler returned nothing", which is a
        // different fact from "the result could not be serialized".
        let result_bytes = serde_json::to_vec(&result).map_err(|e| {
            Status::internal(format!("failed to serialize the execution result: {e}"))
        })?;
        Ok(Response::new(PythonExecuteResponse {
            success: result.success,
            output: result.stdout,
            error: result.error.unwrap_or_default(),
            result: result_bytes,
        }))
    }
}

// Helper methods for PythonPluginService
impl PythonPluginService {
    /// Offer Pulse every `@schedule` the reloaded code declares. A reload never
    /// reaches the path Initialize uses, so a handler stoked after startup had
    /// code and no clock. Creation is idempotent, so this offers all of them.
    /// Put each unreadable handler where something other than the journal can
    /// find it. A fault names the handler and why, in the plugin's own context,
    /// so `predicate=handler:faulted` answers what `crowbar` used to.
    /// Read what each handler declares it serves, and keep it: the one @llm
    /// function, and every @route. A handler whose declarations cannot be
    /// read, or that declares a second @llm, is faulted and serves nothing.
    fn declare(&self, handlers: &HashMap<String, String>) -> Vec<(String, String)> {
        let mut faulted = vec![];
        let mut names: Vec<_> = handlers.keys().collect();
        names.sort();
        let mut read = vec![];
        for name in names {
            let declared = {
                let state = self.handlers.state.read();
                state.engine.extract_declarations(&handlers[name])
            };
            match declared {
                Ok(d) => read.push((name.clone(), d)),
                Err(e) => {
                    error!("handler {name} did not load: {e}");
                    faulted.push((name.clone(), e));
                }
            }
        }

        // One @llm function answers the node's LLM calls: the one answering
        // them now while its handler still declares it, else the first by
        // name. Another is not served, and its handler is not faulted for it:
        // a fault takes the whole handler off every later load.
        let incumbent = self.handlers.state.read().llm_handler.clone();
        let declares_llm = |handler: &str| {
            read.iter()
                .find(|(n, d)| n == handler && d.llm.is_some())
                .map(|(n, d)| (n.clone(), d.llm.clone().unwrap_or_default()))
        };
        let llm_handler = incumbent
            .and_then(|(handler, _)| declares_llm(&handler))
            .or_else(|| {
                read.iter()
                    .find(|(_, d)| d.llm.is_some())
                    .map(|(n, d)| (n.clone(), d.llm.clone().unwrap_or_default()))
            });
        if let Some((handler, function)) = &llm_handler {
            info!("LLM: {}.{} answers the node's LLM calls", handler, function);
        }
        for (name, d) in &read {
            if let (Some(function), Some((serving, _))) = (&d.llm, &llm_handler) {
                if name != serving {
                    error!(
                        "{name}.{function} is not served: {serving} answers the node's LLM calls, and a plugin has one @llm function"
                    );
                }
            }
        }

        // One @rooms function holds the plugin's rooms: the one holding them now
        // while its handler still declares it, else the first by name, as @llm.
        let rooms_incumbent = self.handlers.state.read().rooms_handler.clone();
        let declares_rooms = |handler: &str| {
            read.iter()
                .find(|(n, d)| n == handler && d.rooms.is_some())
                .map(|(n, d)| (n.clone(), d.rooms.clone().unwrap_or_default()))
        };
        let rooms_handler = rooms_incumbent
            .and_then(|(handler, _)| declares_rooms(&handler))
            .or_else(|| {
                read.iter()
                    .find(|(_, d)| d.rooms.is_some())
                    .map(|(n, d)| (n.clone(), d.rooms.clone().unwrap_or_default()))
            });
        if let Some((handler, function)) = &rooms_handler {
            info!("Rooms: {}.{} holds the plugin's rooms", handler, function);
        }

        // A route is one method and path, answered by one function, and pyre's
        // own paths are pyre's. A declaration of either is not kept.
        let mut routes: Vec<(crate::engine::RouteInfo, String)> = vec![];
        for (name, d) in read {
            for route in d.routes {
                let pair = (route.method.as_str(), route.path.as_str());
                if PYRE_PATHS.contains(&pair) {
                    error!("{} {} via {}.{} is not served: it is pyre's own path", route.method, route.path, name, route.handler_fn);
                    continue;
                }
                if let Some((_, first)) = routes
                    .iter()
                    .find(|(r, _)| r.method == route.method && r.path == route.path)
                {
                    error!("{} {} via {}.{} is not served: {} declares it already", route.method, route.path, name, route.handler_fn, first);
                    continue;
                }
                info!("Route: {} {} via {}.{}", route.method, route.path, name, route.handler_fn);
                routes.push((route, name.clone()));
            }
        }

        let mut state = self.handlers.state.write();
        state.llm_handler = llm_handler;
        state.rooms_handler = rooms_handler;
        state.routes = routes;
        faulted
    }

    /// Call a function a handler declares, off the async workers: a call
    /// holds the interpreter for as long as the function runs.
    pub(crate) async fn call_declared(
        &self,
        handler: String,
        function: String,
        argument: serde_json::Value,
        as_route: bool,
    ) -> Result<serde_json::Value, Status> {
        let state = self.handlers.state.clone();
        #[allow(clippy::result_large_err)]
        let call = move || -> Result<serde_json::Value, Status> {
            let state = state.read();
            let code = state.discovered_handlers.get(&handler).cloned().ok_or_else(|| {
                Status::failed_precondition(format!("handler {handler} is no longer discovered"))
            })?;
            crate::atsstore::set_current_handler(Some(handler.clone()));
            crate::schedulestore::set_current_client(state.schedule_client.clone());
            crate::fetchstore::set_current_client(state.fetch_client.clone());
            let ats = Some(state.ats_client.clone());
            let answered = if as_route {
                state.engine.call_route(&code, &function, &argument, ats)
            } else {
                state.engine.call_declared(&code, &function, &argument, ats)
            };
            crate::fetchstore::clear_current_client();
            crate::schedulestore::clear_current_client();
            crate::atsstore::set_current_handler(None);
            answered.map_err(|e| {
                error!(handler = handler.as_str(), error = %e, "Declared function failed");
                Status::internal(format!("{handler}.{function} failed: {e}"))
            })
        };
        tokio::task::spawn_blocking(call)
            .await
            .map_err(|e| Status::internal(format!("the call did not finish: {e}")))?
    }

    /// The node's LLM call, answered by the @llm function.
    async fn answer_llm(&self, request: LlmChatRequest) -> Result<LlmChatResponse, Status> {
        let declared = self.handlers.state.read().llm_handler.clone();
        let (handler, function) = declared.ok_or_else(|| {
            Status::unimplemented(format!("{} declares no @llm function", self.name))
        })?;
        let argument = serde_json::to_value(&request)
            .map_err(|e| Status::internal(format!("the request did not serialize: {e}")))?;
        let answer = self.call_declared(handler, function, argument, false).await?;
        llm_answer(&answer)
    }

    /// The @route function that serves this method and path, when one does.
    fn route_for(&self, method: &str, path: &str) -> Option<(String, String)> {
        self.handlers
            .state
            .read()
            .routes
            .iter()
            .find(|(r, _)| r.method == method && r.path == path)
            .map(|(r, handler)| (handler.clone(), r.handler_fn.clone()))
    }

    /// A request on a declared route, answered by its function. A function
    /// that raises is a 500 whose body says why.
    async fn answer_route(
        &self,
        handler: String,
        function: String,
        req: &HttpRequest,
        path: &str,
        query: &str,
    ) -> HttpResponse {
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD;
        let mut headers = serde_json::Map::new();
        for h in &req.headers {
            headers.insert(
                h.name.to_lowercase(),
                serde_json::Value::String(h.values.join(", ")),
            );
        }
        let request = serde_json::json!({
            "method": req.method,
            "path": path,
            "query": query,
            "headers": headers,
            "body_base64": b64.encode(&req.body),
        });
        let failed = |status: i32, message: String| HttpResponse {
            status_code: status,
            headers: vec![HttpHeader {
                name: "Content-Type".to_string(),
                values: vec!["application/json".to_string()],
            }],
            body: serde_json::to_vec(&serde_json::json!({ "error": message })).unwrap_or_default(),
        };
        let answer = match self.call_declared(handler, function, request, true).await {
            Ok(a) => a,
            Err(status) => return failed(500, status.message().to_string()),
        };
        let body = match b64.decode(answer["body_base64"].as_str().unwrap_or_default()) {
            Ok(b) => b,
            Err(e) => return failed(500, format!("the route's body did not decode: {e}")),
        };
        let headers = answer["headers"]
            .as_object()
            .map(|m| {
                m.iter()
                    .map(|(name, value)| HttpHeader {
                        name: name.clone(),
                        values: vec![value.as_str().unwrap_or_default().to_string()],
                    })
                    .collect()
            })
            .unwrap_or_default();
        HttpResponse {
            status_code: answer["status"].as_i64().unwrap_or(200) as i32,
            headers,
            body,
        }
    }

    fn attest_faults(&self, faulted: &[(String, String)]) {
        if faulted.is_empty() {
            return;
        }

        let client = {
            let state = self.handlers.state.read();
            state.ats_client.clone()
        };

        for (handler_name, reason) in faulted {
            let mut attributes = HashMap::new();
            attributes.insert("plugin".to_string(), self.name.clone().into());
            attributes.insert("handler".to_string(), handler_name.clone().into());
            attributes.insert("reason".to_string(), reason.clone().into());

            let mut guard = client.lock();
            let Some(c) = guard.as_mut() else {
                error!("{handler_name} faulted and there is no ATS client to say so");
                continue;
            };

            // The same subject the code is attested under, so "newest wins"
            // decides whether this handler is loadable without comparing
            // timestamps: a later stoke buries the fault by existing.
            match c.create_attestation(
                vec![handler_name.clone()],
                vec!["handler:faulted".to_string()],
                vec![self.name.clone()],
                None,
                Some(attributes),
            ) {
                Ok(_) => error!("handler {handler_name} faulted — attested"),
                Err(e) => error!("{handler_name} faulted and the fault could not be attested: {e}"),
            }
        }
    }

    fn declare_schedules(&self, handlers: &HashMap<String, String>) {
        let client = {
            let state = self.handlers.state.read();
            state.schedule_client.clone()
        };

        for (handler_name, code) in handlers {
            let extracted = {
                let state = self.handlers.state.read();
                state.engine.extract_schedules(code)
            };

            let schedules = match extracted {
                Ok(s) => s,
                Err(e) => {
                    error!("handler {handler_name}: cannot read its schedules: {e}");
                    continue;
                }
            };

            for s in schedules {
                // What core would have named it. Anything else creates a second
                // schedule beside the declared one.
                let name = format!("{}/{}.{}", self.name, self.name, handler_name);

                let mut metadata = HashMap::new();
                metadata.insert("plugin".to_string(), self.name.clone());
                metadata.insert("description".to_string(), s.description.clone());

                let guard = client.lock();
                let Some(c) = guard.as_ref() else {
                    error!("{name}: no schedule client, so it has no clock");
                    continue;
                };

                let id = match c.create(&name, s.interval_seconds, metadata.clone()) {
                    Ok(id) => id,
                    Err(e) => {
                        error!("{name}: every {}s refused: {e}", s.interval_seconds);
                        continue;
                    }
                };

                // Create hands back what already stands rather than changing
                // it, so a period edited in the decorator lands nowhere unless
                // the old schedule is taken away first.
                let standing = match c.interval_of(&id) {
                    Ok(v) => v,
                    Err(e) => {
                        error!("{name} ({id}): cannot read its interval: {e}");
                        continue;
                    }
                };

                if standing == s.interval_seconds {
                    info!("Schedule: {name} every {}s ({id})", s.interval_seconds);
                    continue;
                }

                if let Err(e) = c.delete(&id) {
                    error!("{name}: still every {standing}s, {id} would not go: {e}");
                    continue;
                }

                match c.create(&name, s.interval_seconds, metadata) {
                    Ok(new) => info!(
                        "Schedule: {name} every {}s was {standing}s ({new})",
                        s.interval_seconds
                    ),
                    Err(e) => error!("{name}: {standing}s taken away and nothing put back: {e}"),
                }
            }
        }
    }

    /// Hot-reload handler code from ATS store.
    ///
    /// Triggered by a watcher on predicate="handler" in our own context.
    /// Re-queries ATS for the latest handler attestations and swaps the
    /// in-memory discovered_handlers map.
    async fn handle_handler_reload(&self) -> Result<Response<ExecuteJobResponse>, Status> {
        let config = {
            let state = self.handlers.state.read();
            state.config.clone()
        };

        let new_handlers = self.discover_handlers_from_config(config).await;
        let count = new_handlers.len();

        self.declare_schedules(&new_handlers);

        {
            let mut state = self.handlers.state.write();
            state.discovered_handlers = new_handlers.clone();
        }
        let faulted = self.declare(&new_handlers);
        self.attest_faults(&faulted);

        info!("Hot-reloaded {} handler(s) from ATS store", count);

        Ok(Response::new(ExecuteJobResponse {
            success: true,
            error: String::new(),
            result: serde_json::to_vec(&serde_json::json!({
                "reloaded": count,
            }))
            .unwrap_or_default(),
            progress_current: 0,
            progress_total: 0,
            log_entries: vec![],
            plugin_version: version().to_string(),
        }))
    }

    /// Execute a python.script job
    async fn execute_python_script_job(
        &self,
        req: ExecuteJobRequest,
    ) -> Result<Response<ExecuteJobResponse>, Status> {
        use crate::engine::ExecutionConfig;

        // Parse payload as JSON containing script_code
        #[derive(serde::Deserialize)]
        struct PythonScriptPayload {
            content: String,
        }

        let payload: PythonScriptPayload = serde_json::from_slice(&req.payload)
            .map_err(|e| Status::invalid_argument(format!("Invalid payload JSON: {}", e)))?;

        if payload.content.is_empty() {
            return Err(Status::invalid_argument("Missing content in payload"));
        }

        // Execute the Python script
        let config = ExecutionConfig {
            timeout_secs: match req.timeout_secs {
                Some(t) if t > 0 => t as u64,
                _ => DEFAULT_TIMEOUT_SECS,
            },
            capture_variables: false,
            python_paths: vec![],
            ..Default::default()
        };

        let shared = self.handlers.state.clone();
        let result = crate::handlers::off_the_workers(move || {
            let state = shared.read();
            state.engine.execute_with_ats(
                &payload.content,
                &config,
                Some(state.ats_client.clone()),
                None,
            )
        })
        .await?;

        // Convert execution result to ExecuteJobResponse
        if result.success {
            // Serialize result as JSON for the result field
            let result_json = serde_json::json!({
                "stdout": result.stdout,
                "stderr": result.stderr,
                "duration_ms": result.duration_ms,
                "result": result.result,
            });

            let result_bytes = serde_json::to_vec(&result_json)
                .map_err(|e| Status::internal(format!("Failed to serialize result: {}", e)))?;

            Ok(Response::new(ExecuteJobResponse {
                success: true,
                error: String::new(),
                result: result_bytes,
                progress_current: 0,
                progress_total: 0,
                    log_entries: vec![],
                plugin_version: version().to_string(),
            }))
        } else {
            // Execution failed
            let error_msg = result.error.unwrap_or_else(|| "Unknown error".to_string());

            Ok(Response::new(ExecuteJobResponse {
                success: false,
                error: error_msg,
                result: vec![],
                progress_current: 0,
                progress_total: 0,
                    log_entries: vec![],
                plugin_version: version().to_string(),
            }))
        }
    }

    /// Execute a dynamically discovered handler job
    async fn execute_discovered_handler_job(
        &self,
        req: ExecuteJobRequest,
        handler_key: &str,
    ) -> Result<Response<ExecuteJobResponse>, Status> {
        use crate::engine::ExecutionConfig;

        // Retrieve handler code from plugin state
        let script_code = {
            let state = self.handlers.state.read();
            state.discovered_handlers.get(handler_key).cloned()
        };

        let script_code = script_code.ok_or_else(|| {
            Status::not_found(format!(
                "Handler {} not found in discovered handlers",
                handler_key
            ))
        })?;

        // Parse upstream attestation from watcher payload
        let upstream: Option<serde_json::Value> = if req.payload.is_empty() {
            None
        } else {
            // Same as the other door: a malformed payload must not read as
            // "no upstream", or the handler runs on a lie about its trigger.
            Some(serde_json::from_slice(&req.payload).map_err(|e| {
                Status::invalid_argument(format!("watcher payload is not valid JSON: {e}"))
            })?)
        };

        // One preparation, shared with the HTTP door, so the same file
        // 「[Sonnet 5.5] behaves the same way whichever way it arrives. Preparing it runs the」
        // 「[Sonnet 5.5] module, so it runs where the handler runs, on the blocking pool.」

        // Execute the Python script with upstream attestation
        let config = ExecutionConfig {
            timeout_secs: match req.timeout_secs {
                Some(t) if t > 0 => t as u64,
                _ => DEFAULT_TIMEOUT_SECS,
            },
            capture_variables: false,
            python_paths: vec![],
            ..Default::default()
        };

        // The runtime knows which handler this is, so attest() can say so and no
        // handler has to pass its own name.
        // Said before and after, because a handler that never came back is
        // the one worth knowing about, and the file showed nothing of it.
        info!(handler = handler_key, "Handler started");
        let shared = self.handlers.state.clone();
        let handler_name = handler_key.to_string();
        let result = crate::handlers::off_the_workers(move || {
            // 「[Sonnet 5.5] All of these are thread-local, so they are set on the thread」
            // 「[Sonnet 5.5] that runs the handler.」
            let state = shared.read();
            let exec_code = state.engine.prepared(&script_code)?;
            crate::atsstore::set_current_handler(Some(handler_name));
            crate::schedulestore::set_current_client(state.schedule_client.clone());
            crate::fetchstore::set_current_client(state.fetch_client.clone());
            let r = state.engine.execute_with_ats(
                &exec_code,
                &config,
                Some(state.ats_client.clone()),
                upstream.as_ref(),
            );
            crate::fetchstore::clear_current_client();
            crate::schedulestore::clear_current_client();
            crate::atsstore::set_current_handler(None);
            Ok::<_, String>(r)
        })
        .await?
        .map_err(Status::internal)?;

        // Convert execution result to ExecuteJobResponse
        if result.success {
            info!(handler = handler_key, duration_ms = result.duration_ms, "Handler finished");
            // Serialize result as JSON for the result field
            let result_json = serde_json::json!({
                "stdout": result.stdout,
                "stderr": result.stderr,
                "duration_ms": result.duration_ms,
                "result": result.result,
            });

            let result_bytes = serde_json::to_vec(&result_json)
                .map_err(|e| Status::internal(format!("Failed to serialize result: {}", e)))?;

            Ok(Response::new(ExecuteJobResponse {
                success: true,
                error: String::new(),
                result: result_bytes,
                progress_current: 0,
                progress_total: 0,
                    log_entries: vec![],
                plugin_version: version().to_string(),
            }))
        } else {
            // Execution failed
            let error_msg = result.error.unwrap_or_else(|| "Unknown error".to_string());
            // The traceback is the whole of what a Python failure has to say,
            // and until now it went back to the node and nowhere else.
            error!(
                handler = handler_key,
                duration_ms = result.duration_ms,
                error = %error_msg,
                "Handler failed"
            );

            Ok(Response::new(ExecuteJobResponse {
                success: false,
                error: error_msg,
                result: vec![],
                progress_current: 0,
                progress_total: 0,
                    log_entries: vec![],
                plugin_version: version().to_string(),
            }))
        }
    }
}

/// The status a refusal the @llm function returned names, by its gRPC name.
pub(crate) fn refused(error: &serde_json::Value) -> Status {
    let message = error["message"].as_str().unwrap_or_default().to_string();
    match error["code"].as_str().unwrap_or_default() {
        "INVALID_ARGUMENT" => Status::invalid_argument(message),
        "UNAUTHENTICATED" => Status::unauthenticated(message),
        "PERMISSION_DENIED" => Status::permission_denied(message),
        "NOT_FOUND" => Status::not_found(message),
        "FAILED_PRECONDITION" => Status::failed_precondition(message),
        "RESOURCE_EXHAUSTED" => Status::resource_exhausted(message),
        "UNAVAILABLE" => Status::unavailable(message),
        "ABORTED" => Status::aborted(message),
        "DEADLINE_EXCEEDED" => Status::deadline_exceeded(message),
        other => Status::internal(format!("refused with code {other:?}: {message}")),
    }
}

fn text(v: &serde_json::Value, key: &str) -> String {
    v[key].as_str().unwrap_or_default().to_string()
}

fn count(v: &serde_json::Value, key: &str) -> i32 {
    v[key].as_i64().unwrap_or(0) as i32
}

/// What the @llm function returned, as the node's LLMChatResponse. It
/// returns `content` and may return `model`, the token counts and `served`,
/// each under its proto field name; or `error` with a gRPC `code` and a
/// `message`, which the node receives as that status.
#[allow(clippy::result_large_err)]
fn llm_answer(answer: &serde_json::Value) -> Result<LlmChatResponse, Status> {
    if !answer["error"].is_null() {
        return Err(refused(&answer["error"]));
    }
    let content = answer["content"]
        .as_str()
        .ok_or_else(|| Status::internal("the @llm function returned no content"))?
        .to_string();
    let s = &answer["served"];
    let served = s.as_object().map(|_| LlmServed {
        requested_provider: text(s, "requested_provider"),
        requested_model: text(s, "requested_model"),
        upstream_provider: text(s, "upstream_provider"),
        executed_model: text(s, "executed_model"),
        response_model: text(s, "response_model"),
        request_id: text(s, "request_id"),
        cost_usd: s["cost_usd"].as_str().map(String::from),
        pricing_source: text(s, "pricing_source"),
        attempt_count: count(s, "attempt_count"),
        fallback: s["fallback"].as_bool().unwrap_or(false),
    });
    Ok(LlmChatResponse {
        content,
        model: text(answer, "model"),
        prompt_tokens: count(answer, "prompt_tokens"),
        completion_tokens: count(answer, "completion_tokens"),
        total_tokens: count(answer, "total_tokens"),
        served,
    })
}

#[tonic::async_trait]
impl LlmService for PythonPluginService {
    async fn chat(
        &self,
        request: Request<LlmChatRequest>,
    ) -> Result<Response<LlmChatResponse>, Status> {
        Ok(Response::new(self.answer_llm(request.into_inner()).await?))
    }

    type StreamChatStream = Pin<Box<dyn Stream<Item = Result<LlmChatChunk, Status>> + Send>>;

    /// The whole answer as one final chunk: the @llm function returns once,
    /// so there is nothing to send before it has. It carries no per-token
    /// signal, and says so.
    async fn stream_chat(
        &self,
        request: Request<LlmChatRequest>,
    ) -> Result<Response<Self::StreamChatStream>, Status> {
        let answer = self.answer_llm(request.into_inner()).await?;
        let chunk = LlmChatChunk {
            token: answer.content,
            done: true,
            model: answer.model,
            prompt_tokens: answer.prompt_tokens,
            completion_tokens: answer.completion_tokens,
            total_tokens: answer.total_tokens,
            signal_of: Some(llm_chat_chunk::SignalOf::NoSignal(true)),
            served: answer.served,
        };
        Ok(Response::new(Box::pin(tokio_stream::once(Ok(chunk)))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A plugin standing in no namespace has the handlers its build carries.
    #[test]
    fn the_handlers_a_build_carries_are_read_by_name() {
        let dir = scratch_dir("bundled");
        std::fs::write(dir.join("rooms.py"), "@rooms()\ndef held(asked):\n    return {}\n").unwrap();
        std::fs::write(dir.join("notes.txt"), "not a handler").unwrap();
        let handlers = handlers_in(&dir);
        assert_eq!(handlers.keys().collect::<Vec<_>>(), vec!["rooms"]);
        assert!(handlers["rooms"].contains("@rooms()"));
    }
    use serde::Deserialize;

    #[tokio::test]
    async fn test_metadata() {
        let service = PythonPluginService::new("python").unwrap();
        let response = service.metadata(Request::new(Empty {})).await.unwrap();
        let meta = response.into_inner();
        assert_eq!(meta.name, "python");
        assert_eq!(meta.version, version());
    }

    #[tokio::test]
    async fn test_http_version_endpoint_reports_plugin_version() {
        let service = PythonPluginService::new("python").unwrap();
        let result = service.handlers.handle_version().await.unwrap();

        #[derive(Deserialize)]
        struct VersionResponse {
            plugin_version: String,
        }

        let response: VersionResponse = serde_json::from_slice(&result.body).unwrap();
        assert_eq!(response.plugin_version, version());
    }

    /// Initialize as the node sends it to a plugin with no services wired:
    /// no ATS, so no handlers are discovered. site_dir is a scratch
    /// directory, because the default under /var/lib is not writable here.
    fn bare_initialize(site_dir: &std::path::Path) -> InitializeRequest {
        let mut config = HashMap::new();
        config.insert("site_dir".to_string(), site_dir.display().to_string());
        InitializeRequest {
            config,
            ..Default::default()
        }
    }

    fn scratch_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("pyre-test-{}-{}", name, std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    // What capy and icpy get today: a plugin whose Python declares no LLM
    // handler is no LLM provider, and the node never routes LLM calls to it.
    #[tokio::test]
    async fn a_plugin_that_declares_no_llm_handler_is_no_llm_provider() {
        let service = PythonPluginService::new("python").unwrap();
        let dir = scratch_dir("no-llm");
        let response = service
            .initialize(Request::new(bare_initialize(&dir)))
            .await
            .unwrap()
            .into_inner();
        assert!(!response.llm_provider);
        assert!(response.python_provider);
    }

    // And a path it does not know is still a 404.
    #[tokio::test]
    async fn a_path_no_route_declares_is_a_404() {
        let service = PythonPluginService::new("python").unwrap();
        let dir = scratch_dir("no-route");
        service
            .initialize(Request::new(bare_initialize(&dir)))
            .await
            .unwrap();
        let response = service
            .handle_http(Request::new(HttpRequest {
                method: "POST".to_string(),
                path: "/v1/chat/completions".to_string(),
                body: b"{}".to_vec(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.status_code, 404);
    }

    /// A service whose discovered handlers are these, as Initialize leaves it
    /// after reading ATS.
    async fn serving(handlers: &[(&str, &str)]) -> PythonPluginService {
        let service = PythonPluginService::new("otaripy").unwrap();
        let dir = scratch_dir(handlers.first().map(|h| h.0).unwrap_or("none"));
        service
            .initialize(Request::new(bare_initialize(&dir)))
            .await
            .unwrap();
        let discovered: HashMap<String, String> = handlers
            .iter()
            .map(|(n, c)| (n.to_string(), c.to_string()))
            .collect();
        service.handlers.state.write().discovered_handlers = discovered.clone();
        assert!(service.declare(&discovered).is_empty());
        service
    }

    const ANSWERS: &str = concat!(
        "@llm()\n",
        "def chat(request):\n",
        "    asked = request['messages'][-1]['content']\n",
        "    return {\n",
        "        'content': 'heard ' + asked,\n",
        "        'model': 'good-model',\n",
        "        'prompt_tokens': 3, 'completion_tokens': 2, 'total_tokens': 5,\n",
        "        'served': {\n",
        "            'requested_provider': request['provider'],\n",
        "            'requested_model': request['model'],\n",
        "            'upstream_provider': 'secondary', 'executed_model': 'good-model',\n",
        "            'request_id': 'req-1', 'cost_usd': '0.000023', 'attempt_count': 2,\n",
        "            'fallback': True,\n",
        "        },\n",
        "    }\n",
    );

    fn asked() -> LlmChatRequest {
        LlmChatRequest {
            model: "fast".to_string(),
            provider: "otaripy".to_string(),
            messages: vec![crate::proto::ChatMessage {
                role: "user".to_string(),
                content: "hi".to_string(),
            }],
            ..Default::default()
        }
    }

    // The node's Chat reaches the @llm function, and what it says of how the
    // call was served reaches the node as LLMServed.
    #[tokio::test]
    async fn chat_is_answered_by_the_llm_function() {
        let service = serving(&[("gateway", ANSWERS)]).await;
        let answer = service.chat(Request::new(asked())).await.unwrap().into_inner();

        assert_eq!(answer.content, "heard hi");
        assert_eq!(answer.model, "good-model");
        assert_eq!(answer.total_tokens, 5);
        let served = answer.served.unwrap();
        assert_eq!(served.requested_provider, "otaripy");
        assert_eq!(served.requested_model, "fast");
        assert_eq!(served.upstream_provider, "secondary");
        assert_eq!(served.request_id, "req-1");
        assert_eq!(served.cost_usd.as_deref(), Some("0.000023"));
        assert_eq!(served.attempt_count, 2);
        assert!(served.fallback);
        assert_eq!(served.pricing_source, "");
    }

    // StreamChat is the same answer in one final chunk, which says it
    // carries no per-token signal, since the node refuses a chunk naming neither.
    #[tokio::test]
    async fn stream_chat_is_the_answer_in_one_final_chunk() {
        use tokio_stream::StreamExt;
        let service = serving(&[("gateway", ANSWERS)]).await;
        let mut stream = service
            .stream_chat(Request::new(asked()))
            .await
            .unwrap()
            .into_inner();

        let chunk = stream.next().await.unwrap().unwrap();
        assert!(stream.next().await.is_none());
        assert!(chunk.done);
        assert_eq!(chunk.token, "heard hi");
        assert_eq!(chunk.signal_of, Some(llm_chat_chunk::SignalOf::NoSignal(true)));
        assert_eq!(chunk.served.unwrap().request_id, "req-1");
    }

    #[tokio::test]
    async fn chat_without_an_llm_function_is_unimplemented() {
        let service = serving(&[]).await;
        let refused = service.chat(Request::new(asked())).await.unwrap_err();

        assert_eq!(refused.code(), tonic::Code::Unimplemented);
    }

    // A refusal the function returns keeps its gRPC code and its words, as
    // the Go plugin's mapping of otari's statuses does.
    #[tokio::test]
    async fn a_refusal_the_llm_function_returns_keeps_its_code() {
        let refuses = "@llm()\ndef chat(request):\n    return {'error': {'code': 'PERMISSION_DENIED', 'message': '403 budget_exceeded'}}\n";
        let service = serving(&[("gateway", refuses)]).await;
        let refused = service.chat(Request::new(asked())).await.unwrap_err();

        assert_eq!(refused.code(), tonic::Code::PermissionDenied);
        assert_eq!(refused.message(), "403 budget_exceeded");
    }

    /// An ATS store holding these handlers, as the node's answers
    /// GetAttestations for predicate=handler, and counting the attestations
    /// it is asked to write. Writes are refused after they are counted.
    struct HeldHandlers(Vec<(String, String)>, Arc<std::sync::atomic::AtomicUsize>);

    #[tonic::async_trait]
    impl crate::proto::ats_store_service_server::AtsStoreService for HeldHandlers {
        async fn create_attestation(
            &self,
            _: Request<crate::proto::CreateAttestationRequest>,
        ) -> Result<Response<crate::proto::CreateAttestationResponse>, Status> {
            Err(Status::unimplemented("not held"))
        }
        async fn attestation_exists(
            &self,
            _: Request<crate::proto::AttestationExistsRequest>,
        ) -> Result<Response<crate::proto::AttestationExistsResponse>, Status> {
            Err(Status::unimplemented("not held"))
        }
        async fn generate_and_create_attestation(
            &self,
            _: Request<crate::proto::GenerateAttestationRequest>,
        ) -> Result<Response<crate::proto::GenerateAttestationResponse>, Status> {
            self.1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(Status::unimplemented("not held"))
        }
        async fn batch_generate_and_create_attestations(
            &self,
            _: Request<crate::proto::BatchGenerateAttestationRequest>,
        ) -> Result<Response<crate::proto::BatchGenerateAttestationResponse>, Status> {
            Err(Status::unimplemented("not held"))
        }
        async fn get_attestations(
            &self,
            _: Request<crate::proto::GetAttestationsRequest>,
        ) -> Result<Response<crate::proto::GetAttestationsResponse>, Status> {
            let attestations = self
                .0
                .iter()
                .map(|(name, code)| {
                    let mut attrs = HashMap::new();
                    attrs.insert("code".to_string(), serde_json::Value::String(code.clone()));
                    crate::proto::Attestation {
                        subjects: vec![name.clone()],
                        predicates: vec!["handler".to_string()],
                        attributes: Some(qntx_proto::serde_struct::json_map_to_struct(&attrs)),
                        ..Default::default()
                    }
                })
                .collect();
            Ok(Response::new(crate::proto::GetAttestationsResponse {
                success: true,
                error: String::new(),
                attestations,
            }))
        }
        type GetAttestationsStreamStream =
            Pin<Box<dyn Stream<Item = Result<crate::proto::Attestation, Status>> + Send>>;
        async fn get_attestations_stream(
            &self,
            _: Request<crate::proto::GetAttestationsRequest>,
        ) -> Result<Response<Self::GetAttestationsStreamStream>, Status> {
            Err(Status::unimplemented("not held"))
        }
    }

    /// Where an ATS store holding these handlers answers.
    async fn ats_holding(handlers: &[(&str, &str)]) -> String {
        ats_counting(handlers).await.0
    }

    /// Where an ATS store holding these handlers answers, and how many
    /// attestations it has been asked to write.
    async fn ats_counting(
        handlers: &[(&str, &str)],
    ) -> (String, Arc<std::sync::atomic::AtomicUsize>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let written = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let held = HeldHandlers(
            handlers
                .iter()
                .map(|(n, c)| (n.to_string(), c.to_string()))
                .collect(),
            written.clone(),
        );
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(crate::proto::ats_store_service_server::AtsStoreServiceServer::new(held))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        (address.to_string(), written)
    }

    // Initialize, with the handlers read from ATS as the node holds them,
    // tells the node it is an LLM provider when one of them declares @llm.
    #[tokio::test]
    async fn a_plugin_that_declares_an_llm_function_is_an_llm_provider() {
        let service = PythonPluginService::new("otaripy").unwrap();
        let dir = scratch_dir("llm");
        let mut request = bare_initialize(&dir);
        request.ats_store_endpoint = ats_holding(&[("gateway", ANSWERS)]).await;

        let response = service
            .initialize(Request::new(request))
            .await
            .unwrap()
            .into_inner();

        assert!(response.llm_provider);
        assert!(response.handler_names.contains(&"otaripy.gateway".to_string()));
        let answer = service.chat(Request::new(asked())).await.unwrap().into_inner();
        assert_eq!(answer.content, "heard hi");
    }

    const ROUTES: &str = concat!(
        "@route('POST', '/v1/chat/completions')\n",
        "def completions(request):\n",
        "    return {\n",
        "        'status': 201,\n",
        "        'headers': {'Content-Type': 'text/plain', 'Otari-Request-ID': 'req-2'},\n",
        "        'body': '|'.join([request['method'], request['path'], request['query'],\n",
        "                          request['headers'].get('x-qntx-asker-did', ''),\n",
        "                          request['body'].decode()]),\n",
        "    }\n",
    );

    fn post(path: &str, body: &[u8]) -> HttpRequest {
        HttpRequest {
            method: "POST".to_string(),
            path: path.to_string(),
            headers: vec![HttpHeader {
                name: "X-Qntx-Asker-Did".to_string(),
                values: vec!["did:key:z6Mke2eCaller".to_string()],
            }],
            body: body.to_vec(),
        }
    }

    // A path a handler declares reaches it whole: method, path, query, the
    // headers the node set (who it admitted among them) and the body as sent,
    // here one that is not JSON. Its status, headers and body come back.
    #[tokio::test]
    async fn a_declared_route_is_answered_by_its_function() {
        let service = serving(&[("gateway", ROUTES)]).await;
        let response = service
            .handle_http(Request::new(post("/v1/chat/completions?x=1", b"not json")))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.status_code, 201);
        assert_eq!(
            String::from_utf8(response.body).unwrap(),
            "POST|/v1/chat/completions|x=1|did:key:z6Mke2eCaller|not json"
        );
        let header = |name: &str| {
            response
                .headers
                .iter()
                .find(|h| h.name == name)
                .map(|h| h.values.clone())
        };
        assert_eq!(header("Otari-Request-ID"), Some(vec!["req-2".to_string()]));
        assert_eq!(header("Content-Type"), Some(vec!["text/plain".to_string()]));
    }

    // A route is its method and its path: the same path asked another way is
    // no route, and pyre's own paths stay pyre's.
    #[tokio::test]
    async fn a_route_is_its_method_and_its_path() {
        let service = serving(&[("gateway", ROUTES)]).await;
        let mut get = post("/v1/chat/completions", b"");
        get.method = "GET".to_string();
        let response = service.handle_http(Request::new(get)).await.unwrap().into_inner();
        assert_eq!(response.status_code, 404);

        let version = HttpRequest {
            method: "GET".to_string(),
            path: "/version".to_string(),
            ..Default::default()
        };
        let response = service.handle_http(Request::new(version)).await.unwrap().into_inner();
        assert_eq!(response.status_code, 200);
    }

    #[tokio::test]
    async fn a_route_function_that_raises_is_a_500_that_says_why() {
        let raises = "@route('POST', '/boom')\ndef boom(request):\n    raise RuntimeError('upstream gone')\n";
        let service = serving(&[("gateway", raises)]).await;
        let response = service
            .handle_http(Request::new(post("/boom", b"{}")))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.status_code, 500);
        assert!(String::from_utf8(response.body).unwrap().contains("upstream gone"));
    }

    // The record's config reaches the plugin's Python as the node handed it:
    // a value the record names as ssm:/// or env: arrives resolved, so a
    // handler needs no code and no credentials of its own for a secret.
    #[tokio::test]
    async fn a_handler_reads_its_plugins_config() {
        let reads = "@llm()\ndef chat(request):\n    return {'content': plugin_config['upstream_key_name']}\n";
        let service = PythonPluginService::new("otaripy").unwrap();
        let dir = scratch_dir("config");
        let mut request = bare_initialize(&dir);
        request
            .config
            .insert("upstream_key_name".to_string(), "as the node resolved it".to_string());
        request.ats_store_endpoint = ats_holding(&[("gateway", reads)]).await;
        service.initialize(Request::new(request)).await.unwrap();

        let answer = service.chat(Request::new(asked())).await.unwrap().into_inner();
        assert_eq!(answer.content, "as the node resolved it");
    }

    fn handlers_of(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(n, c)| (n.to_string(), c.to_string())).collect()
    }

    // A second @llm function is not served, and its handler is not faulted:
    // a fault is attested and takes the whole handler, its watchers and
    // schedules with it, off every later load.
    // A stopped plugin keeps none of the config the node resolved for it:
    // its secrets among them, for anything still reaching /execute.
    #[tokio::test]
    async fn a_stopped_plugin_keeps_no_config() {
        let service = PythonPluginService::new("otaripy").unwrap();
        let dir = scratch_dir("stopped-config");
        let mut request = bare_initialize(&dir);
        request.config.insert("openrouter_api_key".to_string(), "resolved".to_string());
        service.initialize(Request::new(request)).await.unwrap();
        service.shutdown(Request::new(Empty {})).await.unwrap();

        let read = service
            .handlers
            .handle_execute(serde_json::json!({"content": "print(len(plugin_config))"}))
            .await
            .unwrap();
        let said: serde_json::Value = serde_json::from_slice(&read.body).unwrap();
        assert_eq!(said["stdout"], "0\n", "{said}");
    }

    #[tokio::test]
    async fn a_second_llm_function_is_not_served_and_not_faulted() {
        let service = PythonPluginService::new("otaripy").unwrap();
        let faulted = service.declare(&handlers_of(&[("a", ANSWERS), ("b", ANSWERS)]));

        assert!(faulted.is_empty(), "{faulted:?}");
        assert_eq!(
            service.handlers.state.read().llm_handler,
            Some(("a".to_string(), "chat".to_string()))
        );
    }

    // The handler serving the node's LLM calls keeps serving them when a
    // handler whose name sorts before it arrives declaring @llm too.
    #[tokio::test]
    async fn the_serving_llm_function_keeps_serving_when_another_arrives() {
        let service = PythonPluginService::new("otaripy").unwrap();
        service.declare(&handlers_of(&[("gateway", ANSWERS)]));
        service.declare(&handlers_of(&[("aa", ANSWERS), ("gateway", ANSWERS)]));

        assert_eq!(
            service.handlers.state.read().llm_handler,
            Some(("gateway".to_string(), "chat".to_string()))
        );
    }

    // pyre's own paths stay pyre's: a route declaring one is not kept.
    #[tokio::test]
    async fn a_route_on_one_of_pyres_own_paths_is_not_kept() {
        let takes = "@route('GET', '/version')\ndef v(request):\n    return {'status': 418}\n";
        let service = serving(&[("gateway", takes)]).await;
        assert!(service.handlers.state.read().routes.is_empty());

        let version = HttpRequest {
            method: "GET".to_string(),
            path: "/version".to_string(),
            ..Default::default()
        };
        let response = service.handle_http(Request::new(version)).await.unwrap().into_inner();
        assert_eq!(response.status_code, 200);
    }

    // One method and path is one route: a second declaration of it is not
    // kept, so which function answers is the first, said in the log.
    #[tokio::test]
    async fn a_route_declared_twice_is_kept_once() {
        let service = serving(&[("a", ROUTES), ("b", ROUTES)]).await;
        let routes = service.handlers.state.read().routes.clone();

        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].1, "a");
    }

    // A plugin that has been told to stop serves no declared route and no
    // LLM call.
    #[tokio::test]
    async fn a_stopped_plugin_serves_nothing_it_declared() {
        let service = serving(&[("gateway", ROUTES), ("llm", ANSWERS)]).await;
        service.shutdown(Request::new(Empty {})).await.unwrap();

        let response = service
            .handle_http(Request::new(post("/v1/chat/completions", b"{}")))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.status_code, 404);
        let refused = service.chat(Request::new(asked())).await.unwrap_err();
        assert_eq!(refused.code(), tonic::Code::Unimplemented);
    }

    // A handler that does not load is faulted once, not once per reading.
    // Multi-threaded: the fault is written from inside Initialize by a client
    // that blocks for its answer, and the stand-in ATS answers on this runtime.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_broken_handler_is_faulted_once() {
        let service = PythonPluginService::new("otaripy").unwrap();
        let dir = scratch_dir("broken");
        let mut request = bare_initialize(&dir);
        let (endpoint, written) = ats_counting(&[("broken", "raise SystemError('no')\n")]).await;
        request.ats_store_endpoint = endpoint;
        service.initialize(Request::new(request)).await.unwrap();

        assert_eq!(written.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_health_before_init() {
        let service = PythonPluginService::new("python").unwrap();
        let response = service.health(Request::new(Empty {})).await.unwrap();
        let health = response.into_inner();
        assert!(!health.healthy);
    }

    #[tokio::test]
    async fn test_execute_endpoint() {
        let service = PythonPluginService::new("python").unwrap();

        let body = serde_json::json!({
            "content": "print('Hello from test')",
            "timeout_secs": 5
        });

        let result = service.handlers.handle_execute(body).await.unwrap();

        #[derive(Deserialize)]
        struct ExecutionResponse {
            success: bool,
            stdout: String,
            stderr: String,
        }

        let response: ExecutionResponse = serde_json::from_slice(&result.body).unwrap();
        assert!(response.success);
        assert_eq!(response.stdout, "Hello from test\n");
        assert_eq!(response.stderr, "");
    }

    #[tokio::test]
    async fn test_attest_function_available() {
        let service = PythonPluginService::new("python").unwrap();

        // Test that the attest function exists in the Python namespace
        // It will error when called since ATSStore is not initialized,
        // but it should be defined and callable.
        let body = serde_json::json!({
            "content": "result = callable(attest)\nprint('attest is callable:', result)",
            "timeout_secs": 5
        });

        let result = service.handlers.handle_execute(body).await.unwrap();

        #[derive(Deserialize)]
        struct ExecutionResponse {
            success: bool,
            stdout: String,
            error: Option<String>,
        }

        let response: ExecutionResponse = serde_json::from_slice(&result.body).unwrap();
        assert!(
            response.success,
            "Expected success, got error: {:?}",
            response.error
        );
        assert!(response.stdout.contains("attest is callable: True"));
    }

    #[tokio::test]
    async fn test_attest_without_atsstore_errors() {
        let service = PythonPluginService::new("python").unwrap();

        // When ATSStore is not initialized, calling attest should fail gracefully
        let body = serde_json::json!({
            "content": r#"
try:
    attest(['subject'], ['predicate'], ['context'])
    print('ERROR: should have raised')
except RuntimeError as e:
    print('Got expected error:', str(e))
"#,
            "timeout_secs": 5
        });

        let result = service.handlers.handle_execute(body).await.unwrap();

        #[derive(Deserialize)]
        struct ExecutionResponse {
            success: bool,
            stdout: String,
        }

        let response: ExecutionResponse = serde_json::from_slice(&result.body).unwrap();
        assert!(response.success);
        assert!(response.stdout.contains("Got expected error"));
    }

    /// Proves the name reaches Python: with a handler in scope the write gets
    /// past the context check and fails on the store instead.
    #[tokio::test]
    async fn a_handler_supplies_the_context_its_code_never_passed() {
        let service = PythonPluginService::new("pyre").unwrap();
        // 「[Sonnet 5.5] The name is thread-local, so it is set on the thread that runs the」
        // 「[Sonnet 5.5] code, as execute_discovered_handler_job does. The HTTP door runs on」
        // 「[Sonnet 5.5] a thread of its own and would not see it.」
        atsstore::set_current_handler(Some("mp004_request_ad_renders".to_string()));

        let code = r#"
try:
    attest(['ad:7'], ['render:requested'])
    print('ERROR: should have raised')
except RuntimeError as e:
    print('raised:', str(e))
"#;
        let result = {
            let state = service.handlers.state.read();
            state.engine.execute_with_ats(
                code,
                &crate::engine::ExecutionConfig::default(),
                Some(state.ats_client.clone()),
                None,
            )
        };
        atsstore::set_current_handler(None);

        assert!(result.success);
        assert!(result.stdout.contains("not initialized"), "{}", result.stdout);
        assert!(!result.stdout.contains("no context"), "{}", result.stdout);
    }

    /// The HTTP door is not a handler, so it has no name to lend. Writing an
    /// empty context instead would put the attestation nowhere.
    #[tokio::test]
    async fn a_write_with_no_handler_and_no_context_says_so() {
        let service = PythonPluginService::new("pyre").unwrap();
        atsstore::set_current_handler(None);

        let body = serde_json::json!({
            "content": r#"
try:
    attest(['ad:7'], ['render:requested'])
    print('ERROR: should have raised')
except RuntimeError as e:
    print('raised:', str(e))
"#,
            "timeout_secs": 5
        });

        let result = service.handlers.handle_execute(body).await.unwrap();

        #[derive(Deserialize)]
        struct ExecutionResponse {
            success: bool,
            stdout: String,
        }

        let response: ExecutionResponse = serde_json::from_slice(&result.body).unwrap();
        assert!(response.success);
        assert!(response.stdout.contains("no context"), "{}", response.stdout);
    }

    #[tokio::test]
    async fn test_last_function_available() {
        let service = PythonPluginService::new("python").unwrap();

        let body = serde_json::json!({
            "content": "print('last is callable:', callable(last))",
            "timeout_secs": 5
        });

        let result = service.handlers.handle_execute(body).await.unwrap();

        #[derive(Deserialize)]
        struct ExecutionResponse {
            success: bool,
            stdout: String,
            error: Option<String>,
        }

        let response: ExecutionResponse = serde_json::from_slice(&result.body).unwrap();
        assert!(
            response.success,
            "Expected success, got error: {:?}",
            response.error
        );
        assert!(response.stdout.contains("last is callable: True"));
    }

    /// An unreadable store must raise, never return None — None means "no history",
    /// and a guard that cannot tell those apart fires "changed" on every run.
    #[tokio::test]
    async fn test_last_without_atsstore_raises_rather_than_returning_none() {
        let service = PythonPluginService::new("python").unwrap();

        let body = serde_json::json!({
            "content": r#"
try:
    got = last(predicates=['observed'])
    print('ERROR: should have raised, returned', repr(got))
except RuntimeError as e:
    print('Got expected error:', str(e))
"#,
            "timeout_secs": 5
        });

        let result = service.handlers.handle_execute(body).await.unwrap();

        #[derive(Deserialize)]
        struct ExecutionResponse {
            success: bool,
            stdout: String,
        }

        let response: ExecutionResponse = serde_json::from_slice(&result.body).unwrap();
        assert!(response.success);
        assert!(response.stdout.contains("Got expected error"));
    }

    // 「[Sonnet 5.5] Ramps concurrent /execute calls until Health stops answering in time, and」
    // 「[Sonnet 5.5] prints one line per level so the break is observed rather than guessed.」
    // 「[Sonnet 5.5] The runtime is multi-threaded and sized as `#[tokio::main]` sizes the」
    // 「[Sonnet 5.5] plugin: one worker per core. Each call holds the GIL for」
    // 「[Sonnet 5.5] PYRE_STRESS_HOLD_MS (default 1000), which is what a busy handler does.」
    // 「[Sonnet 5.5] The bound is PYRE_STRESS_HEALTH_MS (default 2000).」
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "pushes the runtime until it fails; run on purpose"]
    async fn health_keeps_answering_as_executions_pile_up() {
        let hold_ms: u64 = std::env::var("PYRE_STRESS_HOLD_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000);
        let bound = std::time::Duration::from_millis(
            std::env::var("PYRE_STRESS_HEALTH_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(2000),
        );
        let service = std::sync::Arc::new(PythonPluginService::new("python").unwrap());
        let script = format!(
            "import time\nend = time.time() + {}\nwhile time.time() < end:\n    pass\n",
            hold_ms as f64 / 1000.0
        );

        eprintln!(
            "workers={} hold_ms={hold_ms} health_bound_ms={}",
            std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0),
            bound.as_millis()
        );
        eprintln!("level\tok\tfailed\twall_ms\tmax_health_ms");

        let mut broke_at = None;
        for level in [1usize, 2, 4, 8, 16, 32, 64, 128, 256] {
            let started = std::time::Instant::now();
            let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

            let poller = {
                let (service, done) = (service.clone(), done.clone());
                tokio::spawn(async move {
                    let mut worst = std::time::Duration::ZERO;
                    while !done.load(std::sync::atomic::Ordering::SeqCst) {
                        let asked = std::time::Instant::now();
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        let _ = service.health(Request::new(Empty {})).await;
                        worst = worst.max(asked.elapsed().saturating_sub(std::time::Duration::from_millis(50)));
                    }
                    worst
                })
            };

            let calls: Vec<_> = (0..level)
                .map(|_| {
                    let (service, script) = (service.clone(), script.clone());
                    tokio::spawn(async move {
                        service
                            .handlers
                            .handle_execute(serde_json::json!({ "content": script }))
                            .await
                    })
                })
                .collect();

            let (mut ok, mut failed) = (0, 0);
            for call in calls {
                // 「[Sonnet 5.5] A script that fails comes back as Ok with status 400, so」
                // 「[Sonnet 5.5] only a 200 whose body says success is an execution that ran.」
                match call.await {
                    Ok(Ok(response))
                        if response.status_code == 200
                            && serde_json::from_slice::<serde_json::Value>(&response.body)
                                .map(|body| body["success"] == true)
                                .unwrap_or(false) =>
                    {
                        ok += 1
                    }
                    _ => failed += 1,
                }
            }
            let wall = started.elapsed();
            done.store(true, std::sync::atomic::Ordering::SeqCst);
            let worst = poller.await.unwrap();

            eprintln!("{level}\t{ok}\t{failed}\t{}\t{}", wall.as_millis(), worst.as_millis());
            if failed > 0 || worst > bound {
                broke_at = Some((level, failed, worst));
                break;
            }
        }

        if let Some((level, failed, worst)) = broke_at {
            panic!(
                "broke at {level} concurrent executions: {failed} failed, Health waited {}ms (bound {}ms)",
                worst.as_millis(),
                bound.as_millis()
            );
        }
    }
}
