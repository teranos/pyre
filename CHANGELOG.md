# Changelog

Prior to v0.9.0, pyre lived in [teranos/QNTX](https://github.com/teranos/QNTX) as `qntx-plugins/qntx-python`.

## 0.8.2 (2026-05-25)

- Strip query string from HTTP path before routing (QNTX proxy appends query to path)

## 0.8.1 (2026-05-25)

- Fix handler discovery dedup: keep newest handler when multiple attestations share the same subject

## 0.8.0 (2026-05-25)

- `@watch` decorator for automatic watcher registration from Python handlers
- Watcher metadata extraction at init via `extract_watchers()`

## 0.5.10 (2026-05-17)

- `python_provider` capability-based glyph registration
- PythonService gRPC execution replaces HTTP proxy

## 0.5.8 (2026-04-14)

- Health RPC enrichment

## 0.3.x (2026-01-27)

- Persistent Python editors with `attest()` support
- ATSStore gRPC client for creating attestations from Python
- `upstream` dict injection for watcher handler execution

## 0.1.0 (2026-01-06)

- Initial Rust gRPC Python plugin with PyO3 integration
- Nix build infrastructure
- Execute, evaluate, execute-file endpoints
- stdout/stderr capture, variable extraction
- uv/pip package management
