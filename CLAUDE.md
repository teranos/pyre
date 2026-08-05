# Pyre

Nix-only build. `cargo build` works inside `nix develop` only (PyO3 needs Python <= 3.13). Fast dev iteration: `nix develop -c cargo build && cp target/debug/pyre ~/.qntx/plugins/qntx-pyre-plugin`. `make install` is the slow path (full `nix build`).

Git deps on teranos/QNTX: `qntx-grpc` (proto types), `qntx-proto` (Struct/JSON helpers). Proto files fetched via `QNTX_PROTO_DIR` flake input. When QNTX updates: update both `Cargo.lock` and `outputHashes` in `flake.nix`.

Single binary, multiple instances via Nix wrapping (`--name`, `withPackages`).

Handler discovery: queries ATS for `predicate=handler, context={name}`, keeps newest on duplicate subjects. `@watch('pred', context='ctx')` registers watchers at init. `@schedule(every=N)` registers periodic execution via Pulse. Python builtins: `attest()`, `last()`, `pause_schedule(id)`, `resume_schedule(id)`, `delete_schedule(id)`, `fetch(url)`.

`last(subjects, predicates, contexts, actors)` returns the newest matching attestation or `None`. It is the only way a handler recovers state across a hot reload — reload re-executes the module, so module-level variables are not history.

Version in `Cargo.toml` — bump on every code change.
