# Pyre

Python runtime engine for [QNTX](https://github.com/teranos/QNTX). Embeds Python via PyO3 in a Rust gRPC process.

## Why

Full control over the Python execution environment. The Rust binary is the chassis — identical code for all Python plugins. Nix is the configuration surface: each domain gets its own `withPackages` set, its own process, its own port. Same binary, same gRPC protocol, different Python environments.

The `@watch` decorator and handler discovery give Python scripts first-class participation in the attestation pipeline without writing Rust or Go.

See [ADR-022](https://github.com/teranos/QNTX/blob/main/docs/adr/ADR-022-python-as-plugin-provided-service.md) and [Python Plugin User Guide](https://github.com/teranos/QNTX/blob/main/docs/development/python-plugin.md).

## What it does

- Executes Python code, expressions, and files via gRPC/HTTP
- `attest()` built-in for creating attestations from Python
- `last()` built-in for reading the newest matching attestation back — a handler's only durable memory across hot reload
- Discovers handlers from ATS (predicate=handler, context=plugin-name)
- `@watch` decorator — handlers fire automatically on upstream attestations
- Installs a package at runtime, into a site directory the engine owns, with no
  pip and no uv — see DIRE below
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

Resolve a wheel for the running interpreter and unpack it into the engine's
site directory. No pip, no uv: pyre deploys as a Nix-wrapped interpreter that
has neither, and a wheel is a zip the standard library can open.

```json
{"package": "numpy"}
```

A wheel that does not match the interpreter is refused rather than installed.
Importing one built for another architecture ends the process instead of
raising, and the handler-reload watcher then retries it until the plugin is
gone. A failed install answers `success: false` with the reason attached.

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

## DIRE — Development In Runtime Environments

`.#dire` is pyre wrapped with its own Python environment. It is the shape every
consumer builds: `withPackages` for the modules you want pinned and owned, a
`wrapProgram` that sets `PYTHONPATH` and a `--name`, one process per domain.

It ships here so the pattern comes with pyre rather than being rediscovered,
and so CI builds it on both Linux and macOS. That matters more than it sounds:
the interpreter `withPackages` produces has **no pip**, which is the exact
configuration where package management used to fail silently.

```bash
nix build .#dire
```

In your own repo this is a separate flake taking `pyre` as an input. Here it is
an output of pyre's own flake so it shares these inputs and this lock; a second
lock fetching the same QNTX pin a second way disagreed on its NAR hash and
broke CI.

The point of the environment is that it does not have to hold everything. What
is baked in is pinned; what a handler needs later it asks for at runtime, and
the plugin keeps running. If the DIRE tests are red, the runtime cannot take a
module, and nothing built on top of it is worth debugging first.

## Architecture

Implements `DomainPluginService` and `PythonService` (see [ADR-022](https://github.com/teranos/QNTX/blob/main/docs/adr/ADR-022-python-as-plugin-provided-service.md)).
