# Pyre

Python runtime engine for [QNTX](https://github.com/teranos/QNTX). Embeds Python via PyO3 in a Rust gRPC process.

## Why

Full control over the Python execution environment. The Rust binary is the chassis — identical code for all Python plugins. Nix is the configuration surface: each domain gets its own `withPackages` set, its own process, its own port. Same binary, same gRPC protocol, different Python environments.

The `@watch` decorator and handler discovery give Python scripts first-class participation in the attestation pipeline without writing Rust or Go.

See [ADR-022](https://github.com/teranos/QNTX/blob/main/docs/adr/ADR-022-python-as-plugin-provided-service.md) and [Python Plugin User Guide](https://github.com/teranos/QNTX/blob/main/docs/development/python-plugin.md).

## What it does

- Executes Python code, expressions, and files via gRPC/HTTP
- `attest()` built-in for creating attestations from Python
- Discovers handlers from ATS (predicate=handler, context=plugin-name)
- `@watch` decorator — handlers fire automatically on upstream attestations
- Package management via uv with pip fallback
- Captures stdout/stderr and variable extraction

## Building

Nix-only build (PyO3 requires deterministic Python linking):

```bash
nix build
```

### Development iteration

`nix develop` provides a shell with Python 3.13, Rust toolchain, and protobuf. Inside it, `cargo build` and `cargo check` work with incremental compilation (seconds, not minutes).

```bash
# Fast path: build + install in one shot (~3s incremental)
nix develop -c cargo build && cp target/debug/pyre ~/.qntx/plugins/qntx-pyre-plugin

# Or enter the shell for repeated builds
nix develop
cargo build
cargo test
```

`make install` uses `nix build` (full hermetic build) — correct but slow. Use the fast path above for development.

### Pre-built binaries

CI pushes builds to [Cachix](https://app.cachix.org/cache/qntx). Downstream consumers can fetch the binary directly instead of compiling Rust:

```bash
cachix use qntx
nix build github:teranos/pyre
```

### Limitation

PyO3 0.24 supports Python up to 3.13. System Python 3.14+ will not work outside of `nix develop`.

## Usage

QNTX manages the plugin lifecycle. Add to `am.toml`:

```toml
[plugin]
enabled = ["pyre"]
```

Specialized instances use a Nix flake that wraps the same binary with a different Python environment and `--name`.

## HTTP API

### POST /execute

```json
{"content": "print('hello')", "timeout_secs": 30, "capture_variables": false}
```
```json
{"success": true, "stdout": "hello\n", "stderr": "", "result": null, "error": null, "duration_ms": 5}
```

### POST /evaluate

```json
{"expr": "1 + 2 * 3"}
```
```json
{"success": true, "result": 7, "duration_ms": 1}
```

### POST /execute-file

```json
{"path": "/path/to/script.py", "capture_variables": false}
```

### POST /uv/install

Install a package (uv preferred, pip fallback).

```json
{"package": "numpy"}
```

### GET /uv/check

Check if a package is available.

```json
{"module": "numpy"}
```

### GET /version

```json
{"python_version": "3.11.15", "plugin_version": "0.8.2"}
```

### GET /modules

Lists installed packages.

## Architecture

Implements `DomainPluginService` and `PythonService` (see [ADR-022](https://github.com/teranos/QNTX/blob/main/docs/adr/ADR-022-python-as-plugin-provided-service.md)).
