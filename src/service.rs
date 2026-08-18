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
    domain_plugin_service_server::DomainPluginService, python_service_server::PythonService,
    ConfigSchemaResponse, Empty, ExecuteJobRequest, ExecuteJobResponse, GlyphDefResponse,
    HealthResponse, HttpHeader, HttpRequest, HttpResponse, InitializeRequest, InitializeResponse,
    MetadataResponse, ParseAxQueryRequest, ParseAxQueryResponse, PythonExecuteRequest,
    PythonExecuteResponse, ScheduleInfo, WatcherRegistration, WebSocketMessage,
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

/// Python plugin gRPC service
pub struct PythonPluginService {
    handlers: HandlerContext,
    name: String,
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
            ats_store_service_client::AtsStoreServiceClient, AttestationFilter,
            GetAttestationsRequest,
        };
        use tonic::transport::Channel;

        // Check if we have config with ATS store endpoint
        let config = match config {
            Some(cfg) if !cfg.ats_store_endpoint.is_empty() => cfg,
            _ => {
                info!("No ATS store endpoint configured, skipping handler discovery");
                return HashMap::new();
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
            time_start: None,
            time_end: None,
            limit: Some(200),
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
                handlers
            }
            Err(e) => {
                warn!("Failed to discover handlers from ATS store: {}", e);
                HashMap::new()
            }
        }
    }
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

            // Store configuration
            state.config = Some(PluginConfig {
                ats_store_endpoint: req.ats_store_endpoint.clone(),
                queue_endpoint: req.queue_endpoint,
                schedule_endpoint: req.schedule_endpoint.clone(),
                fetch_endpoint: req.fetch_endpoint.clone(),
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
            if !req.fetch_endpoint.is_empty() {
                debug!("Initializing Fetch client for Python HTTP fetching");
                crate::fetchstore::init_shared_client(
                    &state.fetch_client,
                    crate::fetchstore::FetchConfig {
                        endpoint: req.fetch_endpoint,
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

        // Announce async handler capabilities
        // Start with built-in handlers
        let mut handler_names = vec![format!("{}.script", self.name)];

        // Extract @watch and @schedule decorator metadata from discovered handlers
        let mut watchers = vec![];
        let mut schedules = vec![];
        let mut faulted: Vec<(String, String)> = vec![];
        let mut sorted_handlers: Vec<_> = discovered_handlers.keys().collect();
        sorted_handlers.sort();
        for handler_name in &sorted_handlers {
            handler_names.push(format!("{}.{}", self.name, handler_name));

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
                        ats_code: String::new(),
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

        Ok(Response::new(InitializeResponse {
            handler_names,
            schedules,
            watchers,
            python_provider: true,
            ..Default::default()
        }))
    }

    /// Shutdown the plugin
    async fn shutdown(&self, _request: Request<Empty>) -> Result<Response<Empty>, Status> {
        info!("Shutting down Python plugin");
        let mut state = self.handlers.state.write();
        state.initialized = false;
        state.config = None;
        Ok(Response::new(Empty {}))
    }

    /// Handle HTTP requests - routes to appropriate handler
    async fn handle_http(
        &self,
        request: Request<HttpRequest>,
    ) -> Result<Response<HttpResponse>, Status> {
        let req = request.into_inner();
        // Strip query string from path before routing
        let (path, _query) = req.path.split_once('?').unwrap_or((&req.path, ""));
        let method = &req.method;

        debug!("HTTP request: {} {}", method, path);

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

    /// Register custom glyph types (none for Python plugin)
    async fn register_glyphs(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<GlyphDefResponse>, Status> {
        Ok(Response::new(GlyphDefResponse { glyphs: vec![] }))
    }

    /// Parse an Ax query (not implemented — kern handles parsing)
    async fn parse_ax_query(
        &self,
        _request: Request<ParseAxQueryRequest>,
    ) -> Result<Response<ParseAxQueryResponse>, Status> {
        Err(Status::unimplemented("ParseAxQuery is handled by kern"))
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

        // Set glyph ID for actor convention
        if !req.glyph_id.is_empty() {
            crate::atsstore::set_current_glyph_id(Some(req.glyph_id.clone()));
        }

        let config = crate::engine::ExecutionConfig {
            timeout_secs: 30,
            ..Default::default()
        };

        let result = {
            let state = self.handlers.state.read();
            state.engine.execute_with_ats(
                &req.code,
                &config,
                Some(state.ats_client.clone()),
                upstream.as_ref(),
            )
        };

        crate::atsstore::set_current_glyph_id(None);

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
            state.discovered_handlers = new_handlers;
        }

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
            cost_actual: 0.0,
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

        let result = {
            let state = self.handlers.state.read();
            state.engine.execute_with_ats(
                &payload.content,
                &config,
                Some(state.ats_client.clone()),
                None,
            )
        };

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
                cost_actual: 0.0,
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
                cost_actual: 0.0,
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
        // behaves the same way whichever way it arrives.
        let exec_code = {
            let state = self.handlers.state.read();
            state
                .engine
                .prepared(&script_code)
                .map_err(Status::internal)?
        };

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
        let result = {
            let state = self.handlers.state.read();
            crate::atsstore::set_current_handler(Some(handler_key.to_string()));
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
            r
        };

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
                cost_actual: 0.0,
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
                cost_actual: 0.0,
                log_entries: vec![],
                plugin_version: version().to_string(),
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        atsstore::set_current_handler(Some("mp004_request_ad_renders".to_string()));

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
        atsstore::set_current_handler(None);

        #[derive(Deserialize)]
        struct ExecutionResponse {
            success: bool,
            stdout: String,
        }

        let response: ExecutionResponse = serde_json::from_slice(&result.body).unwrap();
        assert!(response.success);
        assert!(response.stdout.contains("not initialized"), "{}", response.stdout);
        assert!(!response.stdout.contains("no context"), "{}", response.stdout);
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
}
