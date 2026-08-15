//! Python code execution - execute, evaluate, and file execution
//!
//! Provides execution capabilities for the PythonEngine.

use crate::atsstore::{self, SharedAtsStoreClient};
use crate::engine::{ExecutionConfig, ExecutionResult, PythonEngine, WatcherInfo};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use qntx_grpc::error::Error;
use std::collections::HashMap;
use std::ffi::CString;

/// Why an extraction could not answer. Extraction runs the module, so the
/// reason is usually the script's own — and it is the only copy of it.
fn extraction_failed(what: &str, result: &ExecutionResult) -> String {
    let reason = result.error.clone().unwrap_or_else(|| "no reason given".into());
    if result.stderr.trim().is_empty() {
        format!("failed to read {what} from the script: {reason}")
    } else {
        format!(
            "failed to read {what} from the script: {reason} ({})",
            result.stderr.trim()
        )
    }
}

/// Maximum length for captured variable values before truncation
const MAX_VARIABLE_LENGTH: usize = 1000;

/// Suffix appended to truncated variable values
const TRUNCATION_SUFFIX: &str = "...";

impl PythonEngine {
    /// Execute Python code and return the result
    pub fn execute(&self, code: &str, config: &ExecutionConfig) -> ExecutionResult {
        self.execute_with_ats(code, config, None, None)
    }

    /// Execute Python code with optional ATSStore client for attestation support.
    /// When an ATSStore client is provided, the `attest()` function becomes available
    /// in the Python execution context. When `upstream_attestation` is provided, it is
    /// injected as a Python dict global named `upstream` (or `None` when absent).
    pub fn execute_with_ats(
        &self,
        code: &str,
        config: &ExecutionConfig,
        ats_client: Option<SharedAtsStoreClient>,
        upstream_attestation: Option<&serde_json::Value>,
    ) -> ExecutionResult {
        let start = std::time::Instant::now();

        // Set up ATSStore client for this execution if provided
        if let Some(ref client) = ats_client {
            atsstore::set_current_client(client.clone());
        }

        let result = self.execute_inner(code, config, ats_client.is_some(), upstream_attestation);

        // Clean up ATSStore client
        if ats_client.is_some() {
            atsstore::clear_current_client();
        }

        let duration_ms = start.elapsed().as_millis() as u64;

        match result {
            Ok(mut res) => {
                res.duration_ms = duration_ms;
                res
            }
            Err(e) => ExecutionResult {
                success: false,
                stdout: String::new(),
                stderr: String::new(),
                result: None,
                error: Some(e.to_string()),
                duration_ms,
                variables: HashMap::new(),
            },
        }
    }

    fn execute_inner(
        &self,
        code: &str,
        config: &ExecutionConfig,
        inject_attest: bool,
        upstream_attestation: Option<&serde_json::Value>,
    ) -> Result<ExecutionResult, Error> {
        // Create CString for the code
        let code_cstr = CString::new(code)
            .map_err(|e| Error::context("invalid code string contains null bytes", e))?;

        Python::with_gil(|py| {
            // Set up output capture
            let io = py
                .import("io")
                .map_err(|e| Error::context("failed to import io module", e))?;
            let sys = py
                .import("sys")
                .map_err(|e| Error::context("failed to import sys module", e))?;

            // Create StringIO objects for capturing output
            let stdout_capture = io
                .call_method0("StringIO")
                .map_err(|e| Error::context("failed to create stdout StringIO object", e))?;
            let stderr_capture = io
                .call_method0("StringIO")
                .map_err(|e| Error::context("failed to create stderr StringIO object", e))?;

            // Save original stdout/stderr
            let original_stdout = sys
                .getattr("stdout")
                .map_err(|e| Error::context("failed to get original sys.stdout", e))?;
            let original_stderr = sys
                .getattr("stderr")
                .map_err(|e| Error::context("failed to get original sys.stderr", e))?;

            // Redirect stdout/stderr
            sys.setattr("stdout", &stdout_capture)
                .map_err(|e| Error::context("failed to redirect sys.stdout to StringIO", e))?;
            sys.setattr("stderr", &stderr_capture)
                .map_err(|e| Error::context("failed to redirect sys.stderr to StringIO", e))?;

            // Create execution globals
            let globals = PyDict::new(py);

            // Add builtins
            let builtins = py
                .import("builtins")
                .map_err(|e| Error::context("failed to import builtins module", e))?;
            globals.set_item("__builtins__", builtins).map_err(|e| {
                Error::context("failed to set __builtins__ in execution globals", e)
            })?;

            // Add custom paths if specified
            for path in &config.python_paths {
                let path_list: Bound<'_, PyList> = sys
                    .getattr("path")
                    .map_err(|e| Error::context("failed to get sys.path", e))?
                    .extract()
                    .map_err(|e| Error::context("failed to extract sys.path as list", e))?;
                let _ = path_list.insert(0, path);
            }

            // Inject attest()/last() if an ATSStore client is available
            if inject_attest {
                atsstore::inject_ats_functions(py, &globals)
                    .map_err(|e| Error::context("failed to inject ATSStore functions", e))?;
            }

            // Inject schedule management functions if schedule client is available
            crate::schedulestore::inject_schedule_functions(py, &globals)
                .map_err(|e| Error::context("failed to inject schedule functions", e))?;

            // Inject fetch function if fetch client is available
            crate::fetchstore::inject_fetch_function(py, &globals)
                .map_err(|e| Error::context("failed to inject fetch function", e))?;

            // Inject upstream attestation as Python dict (or None)
            match upstream_attestation {
                Some(attestation) => {
                    let json_module = py
                        .import("json")
                        .map_err(|e| Error::context("failed to import json module", e))?;
                    let json_str = serde_json::to_string(attestation).map_err(|e| {
                        Error::context("failed to serialize upstream attestation", e)
                    })?;
                    let upstream = json_module
                        .call_method1("loads", (json_str,))
                        .map_err(|e| {
                            Error::context("failed to parse upstream attestation as Python dict", e)
                        })?;
                    globals
                        .set_item("upstream", upstream)
                        .map_err(|e| Error::context("failed to set upstream global", e))?;
                }
                None => {
                    globals
                        .set_item("upstream", py.None())
                        .map_err(|e| Error::context("failed to set upstream = None", e))?;
                }
            }

            // Execute the code using py.run
            let exec_result = py.run(code_cstr.as_c_str(), Some(&globals), None);

            // Restore stdout/stderr
            let _ = sys.setattr("stdout", original_stdout);
            let _ = sys.setattr("stderr", original_stderr);

            // Get captured output
            let stdout: String = stdout_capture
                .call_method0("getvalue")
                .and_then(|v| v.extract())
                .unwrap_or_default();
            let stderr: String = stderr_capture
                .call_method0("getvalue")
                .and_then(|v| v.extract())
                .unwrap_or_default();

            // Handle execution result
            match exec_result {
                Ok(_) => {
                    // Try to get the last expression result if there's a _result variable
                    let result_value = globals
                        .get_item("_result")
                        .ok()
                        .flatten()
                        .and_then(|v| python_to_json(py, &v).ok());

                    // Capture variables if requested
                    let variables = if config.capture_variables {
                        capture_variables(&globals)
                    } else {
                        HashMap::new()
                    };

                    Ok(ExecutionResult {
                        success: true,
                        stdout,
                        stderr,
                        result: result_value,
                        error: None,
                        duration_ms: 0,
                        variables,
                    })
                }
                Err(e) => {
                    // Capture full traceback for better debugging
                    let error_msg = format_python_error(py, &e);
                    Ok(ExecutionResult {
                        success: false,
                        stdout,
                        stderr,
                        result: None,
                        error: Some(error_msg),
                        duration_ms: 0,
                        variables: HashMap::new(),
                    })
                }
            }
        })
    }

    /// Execute a Python file
    ///
    /// TODO(sec): Consider path validation to restrict execution to allowed directories.
    /// Currently reads arbitrary filesystem paths which may be a security concern
    /// depending on deployment context.
    pub fn execute_file(&self, path: &str, config: &ExecutionConfig) -> ExecutionResult {
        self.execute_file_with_ats(path, config, None)
    }

    /// Execute a Python file with optional ATSStore client for attestation support.
    pub fn execute_file_with_ats(
        &self,
        path: &str,
        config: &ExecutionConfig,
        ats_client: Option<SharedAtsStoreClient>,
    ) -> ExecutionResult {
        // TODO(sec): Validate path is within allowed directories if config.allow_fs is false
        match std::fs::read_to_string(path) {
            Ok(code) => self.execute_with_ats(&code, config, ats_client, None),
            Err(e) => ExecutionResult {
                success: false,
                stdout: String::new(),
                stderr: String::new(),
                result: None,
                error: Some(format!("Failed to read file {}: {}", path, e)),
                duration_ms: 0,
                variables: HashMap::new(),
            },
        }
    }

    /// Evaluate a Python expression and return its value
    pub fn evaluate(&self, expr: &str) -> ExecutionResult {
        self.evaluate_with_ats(expr, None)
    }

    /// Evaluate a Python expression with optional ATSStore client.
    pub fn evaluate_with_ats(
        &self,
        expr: &str,
        ats_client: Option<SharedAtsStoreClient>,
    ) -> ExecutionResult {
        // Wrap expression to capture result
        let code = format!("_result = ({})", expr);
        self.execute_with_ats(&code, &ExecutionConfig::default(), ats_client, None)
    }

    /// Extract @watch decorator metadata from a handler script.
    ///
    /// Injects a `watch` decorator into the Python namespace, executes the script
    /// to register decorated functions, then collects the watcher metadata.
    /// Returns empty vec if no decorators found or on error.
    pub fn extract_watchers(&self, code: &str) -> Result<Vec<WatcherInfo>, String> {
        // Python preamble: define @watch decorator that records metadata
        // Also stub @schedule so scripts using both don't crash
        let preamble = r#"
_qntx_watchers = []

class handler:
    def __init__(self, description=None): pass
    def __call__(self, fn): return fn

class watch:
    def __init__(self, predicate, context=None):
        self._predicate = predicate
        self._context = context
    def __call__(self, fn):
        if self._predicate and self._context:
            _qntx_watchers.append({
                'handler_fn': fn.__name__,
                'predicate': self._predicate,
                'context': self._context,
            })
        return fn

class schedule:
    def __init__(self, every, description=None): pass
    def __call__(self, fn): return fn
"#;

        let full_code = format!("{}\n{}", preamble, code);
        let config = ExecutionConfig {
            capture_variables: true,
            ..Default::default()
        };
        let result = self.execute(&full_code, &config);
        if !result.success {
            return Err(extraction_failed("@watch", &result));
        }

        // Extract _qntx_watchers from the execution result
        // Re-execute to get the list as JSON
        let extract_code = format!(
            "{}\n{}\nimport json\n_result = json.dumps(_qntx_watchers)",
            preamble, code
        );
        let result = self.execute(&extract_code, &ExecutionConfig::default());
        if !result.success {
            return Err(extraction_failed("@watch", &result));
        }

        let entries: Vec<serde_json::Value> = match result.result {
            Some(serde_json::Value::String(ref s)) => serde_json::from_str(s)
                .map_err(|e| format!("failed to parse the @watch extraction result: {e}"))?,
            other => {
                return Err(format!(
                    "the @watch extraction returned no usable result: {other:?}"
                ))
            }
        };

        Ok(entries
            .iter()
            .filter_map(|entry| {
                let handler_fn = entry.get("handler_fn")?.as_str()?;
                let predicate = entry.get("predicate")?.as_str()?;
                let context = entry.get("context")?.as_str()?;
                if predicate.is_empty() || context.is_empty() {
                    return None;
                }
                Some(WatcherInfo {
                    handler_fn: handler_fn.to_string(),
                    predicates: vec![predicate.to_string()],
                    contexts: vec![context.to_string()],
                })
            })
            .collect())
    }

    /// A script as Python must receive it: decorators stubbed, entry point
    /// called. Returns the code unchanged when nothing is decorated, so a
    /// plain snippet still runs.
    pub fn prepared(&self, code: &str) -> Result<String, String> {
        const STUBS: &str = concat!(
            "class handler:\n",
            "    def __init__(self, description=None): pass\n",
            "    def __call__(self, fn): return fn\n",
            "class watch:\n",
            "    def __init__(self, predicate, context=None): pass\n",
            "    def __call__(self, fn): return fn\n",
            "class schedule:\n",
            "    def __init__(self, every, description=None): pass\n",
            "    def __call__(self, fn): return fn\n",
        );

        // Extraction runs the module to find decorators, so a plain script
        // can fail it for reasons that are not about decorators at all. The
        // failure only means something when the source claims one.
        let claims_decorator = code.contains("@handler")
            || code.contains("@watch")
            || code.contains("@schedule");

        match self.extract_handler(code) {
            Ok(Some(name)) => return Ok(format!("{STUBS}\n{code}\n{name}()")),
            Ok(None) => {}
            Err(e) if claims_decorator => return Err(e),
            Err(_) => return Ok(code.to_string()),
        }
        match self.extract_watchers(code) {
            Ok(ws) => {
                if let Some(w) = ws.first() {
                    return Ok(format!("{STUBS}\n{code}\n{}(upstream)", w.handler_fn));
                }
            }
            Err(e) if claims_decorator => return Err(e),
            Err(_) => return Ok(code.to_string()),
        }
        match self.extract_schedules(code) {
            Ok(ss) => {
                if let Some(s) = ss.first() {
                    return Ok(format!("{STUBS}\n{code}\n{}()", s.handler_fn));
                }
            }
            Err(e) if claims_decorator => return Err(e),
            Err(_) => return Ok(code.to_string()),
        }
        Ok(code.to_string())
    }

    pub fn extract_handler(&self, code: &str) -> Result<Option<String>, String> {
        let preamble = r#"
_qntx_handler = []

class handler:
    def __init__(self, description=None):
        self._description = description
    def __call__(self, fn):
        _qntx_handler.append(fn.__name__)
        return fn

class schedule:
    def __init__(self, every, description=None): pass
    def __call__(self, fn): return fn

class watch:
    def __init__(self, predicate, context=None): pass
    def __call__(self, fn): return fn
"#;

        let extract_code = format!(
            "{}\n{}\nimport json\n_result = json.dumps(_qntx_handler)",
            preamble, code
        );
        let result = self.execute(&extract_code, &ExecutionConfig::default());
        if !result.success {
            // Returning None here would be indistinguishable from "the script
            // marks no handler", and the reason would be gone — which is how
            // a NameError reached a user with the cause discarded in here.
            return Err(format!(
                "failed to read @handler from the script: {}{}",
                result.error.unwrap_or_else(|| "no reason given".to_string()),
                if result.stderr.is_empty() {
                    String::new()
                } else {
                    format!(" ({})", result.stderr.trim())
                }
            ));
        }

        match result.result {
            Some(serde_json::Value::String(ref s)) => {
                let names: Vec<String> = serde_json::from_str(s).map_err(|e| {
                    format!("failed to parse the @handler extraction result: {e}")
                })?;
                Ok(names.into_iter().next())
            }
            other => Err(format!(
                "the @handler extraction returned no usable result: {other:?}"
            )),
        }
    }

    pub fn extract_schedules(&self, code: &str) -> Result<Vec<crate::engine::ScheduleMetadata>, String> {
        // Also stub @watch so scripts using both don't crash
        let preamble = r#"
_qntx_schedules = []

class handler:
    def __init__(self, description=None): pass
    def __call__(self, fn): return fn

class schedule:
    def __init__(self, every, description=None):
        self._every = every
        self._description = description
    def __call__(self, fn):
        if self._every and self._every > 0:
            _qntx_schedules.append({
                'handler_fn': fn.__name__,
                'every': self._every,
                'description': self._description or fn.__doc__ or '',
            })
        return fn

class watch:
    def __init__(self, predicate, context=None): pass
    def __call__(self, fn): return fn
"#;

        let extract_code = format!(
            "{}\n{}\nimport json\n_result = json.dumps(_qntx_schedules)",
            preamble, code
        );
        let result = self.execute(&extract_code, &ExecutionConfig::default());
        if !result.success {
            return Err(extraction_failed("@schedule", &result));
        }

        let entries: Vec<serde_json::Value> = match result.result {
            Some(serde_json::Value::String(ref s)) => serde_json::from_str(s)
                .map_err(|e| format!("failed to parse the @schedule extraction result: {e}"))?,
            other => {
                return Err(format!(
                    "the @schedule extraction returned no usable result: {other:?}"
                ))
            }
        };

        Ok(entries
            .iter()
            .filter_map(|entry| {
                let handler_fn = entry.get("handler_fn")?.as_str()?;
                let every = entry.get("every")?.as_i64()?;
                if every <= 0 {
                    return None;
                }
                let description = entry
                    .get("description")
                    .and_then(|d| d.as_str())
                    .unwrap_or("")
                    .to_string();
                Some(crate::engine::ScheduleMetadata {
                    handler_fn: handler_fn.to_string(),
                    interval_seconds: every as i32,
                    description,
                })
            })
            .collect())
    }

    /// Install a package into the engine's site directory, at runtime. A wheel
    /// is a zip and the stdlib opens both, so this needs neither uv nor pip —
    /// pyre deploys as a Nix-wrapped interpreter where both were dead.
    pub fn pip_install(&self, package: &str) -> ExecutionResult {
        let Some(site) = self.site_dir() else {
            return ExecutionResult {
                success: false,
                stdout: String::new(),
                stderr: String::new(),
                result: None,
                error: Some(
                    "no site directory: this engine was initialized without one, so a package has nowhere to land".to_string(),
                ),
                duration_ms: 0,
                variables: std::collections::HashMap::new(),
            };
        };

        let code = format!(
            "{}\n_result = _dire_install({:?}, {:?})\n",
            DIRE_INSTALLER, package, site
        );

        let mut outcome = self.execute(&code, &ExecutionConfig::default());

        // The snippet running is not the install succeeding. Reporting the one
        // as the other is what made this look like it worked for years.
        if outcome.success {
            let field = |key: &str| {
                outcome
                    .result
                    .as_ref()
                    .and_then(|v| v.get(key))
                    .cloned()
                    .unwrap_or(serde_json::Value::Null)
            };
            if !field("installed").as_bool().unwrap_or(false) {
                let reason = field("error")
                    .as_str()
                    .map(String::from)
                    .unwrap_or_else(|| "install failed without saying why".to_string());
                outcome.success = false;
                outcome.error = Some(reason);
            }
        }

        outcome
    }
}

/// Resolves a wheel for the running interpreter and unpacks it. The platform
/// check is load-bearing: importing another architecture's wheel takes the
/// process down rather than raising, and the reload watcher then retries it.
const DIRE_INSTALLER: &str = r#"
def _dire_install(package, site):
    import importlib, io, json, os, shutil, sys, sysconfig, urllib.request, zipfile

    major, minor = sys.version_info[:2]
    want_py = "cp%d%d" % (major, minor)
    platform_tag = sysconfig.get_platform().replace("-", "_").replace(".", "_")
    arch = platform_tag.rsplit("_", 1)[-1]

    if sys.platform.startswith("linux"):
        families = ("manylinux", "linux")
    elif sys.platform == "darwin":
        families = ("macosx",)
    else:
        families = ()

    def rank(filename):
        if not filename.endswith(".whl"):
            return None
        parts = filename[:-4].split("-")
        if len(parts) < 5:
            return None
        pys, _abi, plats = parts[-3], parts[-2], parts[-1]
        if want_py not in pys.split(".") and "py3" not in pys.split("."):
            return None
        if plats == "any":
            return 1
        for tag in plats.split("."):
            if tag.endswith("_" + arch) and tag.startswith(families):
                return 2
        return None

    try:
        with urllib.request.urlopen(
                "https://pypi.org/pypi/%s/json" % package, timeout=30) as response:
            meta = json.load(response)
    except Exception as exc:
        return {"installed": False, "error": "pypi lookup failed: %s" % exc}

    best = None
    for entry in meta.get("urls", []):
        score = rank(entry["filename"])
        if score is not None and (best is None or score > best[0]):
            best = (score, entry)

    if best is None:
        return {
            "installed": False,
            "error": "no wheel for %s matching %s on %s. Refusing rather than "
                     "importing another architecture, which kills the plugin."
                     % (package, want_py, platform_tag),
        }

    entry = best[1]
    try:
        with urllib.request.urlopen(entry["url"], timeout=120) as response:
            blob = response.read()
    except Exception as exc:
        return {"installed": False, "error": "download failed: %s" % exc}

    os.makedirs(site, exist_ok=True)
    try:
        archive = zipfile.ZipFile(io.BytesIO(blob))
        tops = sorted({n.split("/")[0] for n in archive.namelist() if "/" in n})
        # Replace, never merge. A half-overwritten package is how one wheel's
        # extension module ends up beside another's, and that is unimportable
        # in the way that segfaults rather than raises.
        for top in tops:
            victim = os.path.join(site, top)
            if os.path.isdir(victim):
                shutil.rmtree(victim)
        archive.extractall(site)
    except Exception as exc:
        return {"installed": False, "error": "unpack failed: %s" % exc}

    if site not in sys.path:
        sys.path.insert(0, site)
    importlib.invalidate_caches()

    return {
        "installed": True,
        "wheel": entry["filename"],
        "site": site,
        "bytes": len(blob),
        "replaced": tops,
    }
"#;

/// Format a Python error with full traceback for better debugging
fn format_python_error(py: Python<'_>, err: &PyErr) -> String {
    // Try to get the full traceback using Python's traceback module
    if let Some(tb) = err.traceback(py) {
        if let Ok(traceback_mod) = py.import("traceback") {
            if let Ok(lines) = traceback_mod
                .call_method1("format_exception", (err.get_type(py), err.value(py), tb))
            {
                if let Ok(iter) = lines.try_iter() {
                    let formatted: Vec<String> = iter
                        .filter_map(|line| line.ok())
                        .filter_map(|line| line.extract::<String>().ok())
                        .collect();
                    if !formatted.is_empty() {
                        return formatted.join("");
                    }
                }
            }
        }
    }
    // Fallback to simple error message
    format!("{}", err)
}

/// Convert a Python object to JSON
fn python_to_json(py: Python<'_>, obj: &Bound<'_, PyAny>) -> Result<serde_json::Value, Error> {
    // Try to use json.dumps for serialization
    let json_module = py
        .import("json")
        .map_err(|e| Error::context("failed to import json module", e))?;

    match json_module.call_method1("dumps", (obj,)) {
        Ok(json_str) => {
            let s: String = json_str
                .extract()
                .map_err(|e| Error::context("failed to extract JSON string from Python", e))?;
            serde_json::from_str(&s)
                .map_err(|e| Error::context("failed to parse Python JSON output", e))
        }
        Err(_) => {
            // Fallback to string representation
            let repr: String = obj
                .repr()
                .and_then(|r| r.extract())
                .unwrap_or_else(|_| "<unknown>".to_string());
            Ok(serde_json::Value::String(repr))
        }
    }
}

/// Capture variables from execution scope
fn capture_variables(globals: &Bound<'_, PyDict>) -> HashMap<String, String> {
    let mut vars = HashMap::new();

    for (key, value) in globals.iter() {
        let key_str: String = match key.extract() {
            Ok(s) => s,
            Err(_) => continue,
        };

        // Skip private/magic variables
        if key_str.starts_with('_') {
            continue;
        }

        // Get string representation
        let value_str: String = value
            .repr()
            .and_then(|r| r.extract())
            .unwrap_or_else(|_| "<unknown>".to_string());

        // Limit size
        let value_str = if value_str.len() > MAX_VARIABLE_LENGTH {
            format!("{}{}", &value_str[..MAX_VARIABLE_LENGTH], TRUNCATION_SUFFIX)
        } else {
            value_str
        };

        vars.insert(key_str, value_str);
    }

    vars
}

#[cfg(test)]
mod tests {
    use super::*;

    // Before @handler the only way to name an entry point was to claim a
    // period or an event, and an invented period runs the thing again.
    #[test]
    fn handler_names_an_entry_point_without_a_period() {
        let engine = PythonEngine::new().unwrap();
        let script = "@handler()\ndef reconcile():\n    pass\n";

        assert_eq!(engine.extract_handler(script).unwrap().as_deref(), Some("reconcile"));
        assert!(engine.extract_schedules(script).unwrap().is_empty());
        assert!(engine.extract_watchers(script).unwrap().is_empty());
    }

    // stoke posted a @handler script to the HTTP door, which ran it as
    // written, and Python raised NameError on the decorator.
    #[test]
    fn a_decorated_script_runs_through_the_http_door() {
        let engine = PythonEngine::new().unwrap();
        let script = "@handler()\ndef go():\n    print('ran')\n";

        let ready = engine.prepared(script).unwrap();
        let result = engine.execute(&ready, &ExecutionConfig::default());

        assert!(result.success, "{:?}", result.error);
        assert_eq!(result.stdout.trim(), "ran");
    }

    #[test]
    fn an_undecorated_script_is_left_alone() {
        let engine = PythonEngine::new().unwrap();
        let plain = "print('plain')\n";
        assert_eq!(engine.prepared(plain).unwrap(), plain);
    }

    #[test]
    fn a_script_without_handler_marks_nothing() {
        let engine = PythonEngine::new().unwrap();
        let scheduled = "@schedule(every=60)\ndef tick():\n    pass\n";

        assert_eq!(engine.extract_handler(scheduled).unwrap(), None);
        assert_eq!(engine.extract_schedules(scheduled).unwrap().len(), 1);
    }

    #[test]
    fn test_simple_execution() {
        let engine = PythonEngine::new().unwrap();
        let result = engine.execute("print('Hello, World!')", &ExecutionConfig::default());
        assert!(result.success);
        assert_eq!(result.stdout.trim(), "Hello, World!");
    }

    #[test]
    fn test_expression_evaluation() {
        let engine = PythonEngine::new().unwrap();
        let result = engine.evaluate("1 + 2");
        assert!(result.success);
        assert_eq!(result.result, Some(serde_json::json!(3)));
    }

    #[test]
    fn test_syntax_error() {
        let engine = PythonEngine::new().unwrap();
        let result = engine.execute("def foo(", &ExecutionConfig::default());
        assert!(!result.success);
        assert!(result.error.is_some());
    }

    #[test]
    fn test_variable_capture() {
        let engine = PythonEngine::new().unwrap();
        let config = ExecutionConfig {
            capture_variables: true,
            ..Default::default()
        };
        let result = engine.execute("x = 42\ny = 'hello'", &config);
        assert!(result.success);
        assert!(result.variables.contains_key("x"));
        assert!(result.variables.contains_key("y"));
    }

    /// Tim: @watch decorator extracts watcher metadata from handler functions
    #[test]
    fn test_extract_watchers_single() {
        let engine = PythonEngine::new().unwrap();

        let code = r#"
@watch('data:processed', context='test/ctx')
def handle(upstream):
    pass
"#;

        let watchers = engine.extract_watchers(code).unwrap();
        assert_eq!(watchers.len(), 1);
        assert_eq!(watchers[0].predicates, vec!["data:processed"]);
        assert_eq!(watchers[0].contexts, vec!["test/ctx"]);
        assert_eq!(watchers[0].handler_fn, "handle");
    }

    /// Tim: multiple @watch decorators in one script
    #[test]
    fn test_extract_watchers_multiple() {
        let engine = PythonEngine::new().unwrap();

        let code = r#"
@watch('data:processed', context='test/ctx')
def handle_a(upstream):
    pass

@watch('data:enriched', context='test/ctx')
def handle_b(upstream):
    pass
"#;

        let watchers = engine.extract_watchers(code).unwrap();
        assert_eq!(watchers.len(), 2);
        assert_eq!(watchers[0].handler_fn, "handle_a");
        assert_eq!(watchers[1].handler_fn, "handle_b");
    }

    /// Tim: script without decorators returns empty
    #[test]
    fn test_extract_watchers_none() {
        let engine = PythonEngine::new().unwrap();

        let code = "x = 42\ndef helper(): pass\n";
        let watchers = engine.extract_watchers(code).unwrap();
        assert!(watchers.is_empty());
    }

    /// Spike: @watch with missing required 'context' kwarg returns helpful error
    #[test]
    fn test_extract_watchers_missing_context() {
        let engine = PythonEngine::new().unwrap();

        let code = r#"
@watch('data:processed')
def handle(upstream):
    pass
"#;

        let watchers = engine.extract_watchers(code).unwrap();
        // Missing context should not silently succeed — no watcher extracted
        assert!(watchers.is_empty());
    }

    /// Spike: @watch with empty predicate
    #[test]
    fn test_extract_watchers_empty_predicate() {
        let engine = PythonEngine::new().unwrap();

        let code = r#"
@watch('', context='test/ctx')
def handle(upstream):
    pass
"#;

        let watchers = engine.extract_watchers(code).unwrap();
        assert!(watchers.is_empty());
    }

    #[test]
    fn test_extract_schedules_single() {
        let engine = PythonEngine::new().unwrap();

        let code = r#"
@schedule(every=300)
def check_status():
    pass
"#;

        let schedules = engine.extract_schedules(code).unwrap();
        assert_eq!(schedules.len(), 1);
        assert_eq!(schedules[0].handler_fn, "check_status");
        assert_eq!(schedules[0].interval_seconds, 300);
    }

    #[test]
    fn test_extract_schedules_with_description() {
        let engine = PythonEngine::new().unwrap();

        let code = r#"
@schedule(every=60, description='Poll upstream data')
def poll():
    pass
"#;

        let schedules = engine.extract_schedules(code).unwrap();
        assert_eq!(schedules.len(), 1);
        assert_eq!(schedules[0].handler_fn, "poll");
        assert_eq!(schedules[0].interval_seconds, 60);
        assert_eq!(schedules[0].description, "Poll upstream data");
    }

    #[test]
    fn test_extract_schedules_none() {
        let engine = PythonEngine::new().unwrap();

        let code = "x = 42\ndef helper(): pass\n";
        let schedules = engine.extract_schedules(code).unwrap();
        assert!(schedules.is_empty());
    }

    #[test]
    fn test_extract_schedules_zero_interval() {
        let engine = PythonEngine::new().unwrap();

        let code = r#"
@schedule(every=0)
def noop():
    pass
"#;

        let schedules = engine.extract_schedules(code).unwrap();
        assert!(schedules.is_empty());
    }

    #[test]
    fn test_extract_schedules_with_watch() {
        let engine = PythonEngine::new().unwrap();

        let code = r#"
@watch('data:new', context='test/ctx')
def on_data(upstream):
    pass

@schedule(every=120)
def periodic():
    pass
"#;

        let watchers = engine.extract_watchers(code).unwrap();
        let schedules = engine.extract_schedules(code).unwrap();
        assert_eq!(watchers.len(), 1);
        assert_eq!(schedules.len(), 1);
        assert_eq!(watchers[0].handler_fn, "on_data");
        assert_eq!(schedules[0].handler_fn, "periodic");
    }

    // --- DIRE ---------------------------------------------------------------
    // These reach PyPI on purpose. They catch a wheel resolving against the
    // wrong interpreter, which no offline fixture can tell you.

    fn dire_engine(name: &str) -> (PythonEngine, String) {
        let site = std::env::temp_dir()
            .join(format!("pyre-dire-{}-{}", name, std::process::id()))
            .to_string_lossy()
            .into_owned();
        let _ = std::fs::remove_dir_all(&site);
        let engine = PythonEngine::new().unwrap();
        engine.initialize(Vec::new(), Some(site.clone())).unwrap();
        (engine, site)
    }

    // The whole point: a module the interpreter was not built with, usable
    // without rebuilding the interpreter.
    #[test]
    #[ignore = "reaches PyPI; the Nix sandbox has no network"]
    fn a_module_arrives_at_runtime_and_imports() {
        let (engine, site) = dire_engine("arrives");
        assert!(!engine.check_module("six"), "six was already present");

        let outcome = engine.pip_install("six");
        assert!(outcome.success, "{:?}", outcome.error);
        assert!(engine.check_module("six"), "installed but not importable");

        let _ = std::fs::remove_dir_all(&site);
    }

    // An install that did not happen must not answer like one that did. This
    // returned success with a null error for as long as the endpoint existed.
    #[test]
    #[ignore = "reaches PyPI; the Nix sandbox has no network"]
    fn a_failed_install_is_reported_as_a_failure() {
        let (engine, site) = dire_engine("failure");

        let outcome = engine.pip_install("pyre-dire-no-such-package-xyzzy");
        assert!(!outcome.success, "a missing package reported success");
        assert!(outcome.error.is_some(), "failed with no error attached");

        let _ = std::fs::remove_dir_all(&site);
    }

    // Pillow ships a wheel per architecture. Picking the wrong one does not
    // raise, it ends the process, so this asserts the tag actually matched.
    #[test]
    #[ignore = "reaches PyPI; the Nix sandbox has no network"]
    fn an_extension_module_matches_the_running_interpreter() {
        let (engine, site) = dire_engine("extension");

        let outcome = engine.pip_install("Pillow");
        assert!(outcome.success, "{:?}", outcome.error);

        let wheel = outcome
            .result
            .as_ref()
            .and_then(|v| v.get("wheel"))
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        assert!(wheel.ends_with(".whl"), "no wheel named: {:?}", outcome.result);
        assert!(!wheel.contains("-any.whl"), "Pillow resolved to a pure wheel: {}", wheel);

        let drawn = engine.execute(
            "from PIL import Image, ImageDraw\n\
             img = Image.new('RGBA', (64, 24), (0, 90, 90, 255))\n\
             ImageDraw.Draw(img).text((2, 6), 'dire', fill=(255, 255, 255, 255))\n\
             _result = img.size\n",
            &ExecutionConfig::default(),
        );
        assert!(drawn.success, "{:?}", drawn.error);

        let _ = std::fs::remove_dir_all(&site);
    }

    // Without a site directory there is nowhere to write, and saying so beats
    // failing further in with a permission error about the Nix store.
    #[test]
    fn an_engine_with_nowhere_to_install_says_so() {
        let engine = PythonEngine::new().unwrap();
        engine.initialize(Vec::new(), None).unwrap();

        let outcome = engine.pip_install("six");
        assert!(!outcome.success);
        assert!(outcome.error.unwrap().contains("site directory"));
    }
}
