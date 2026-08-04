//! The version this build reports.

/// `QNTX_PLUGIN_VERSION` from the build environment when set, else the crate
/// version. Compile-time — setting it in the process environment does nothing.
pub fn version() -> &'static str {
    match option_env!("QNTX_PLUGIN_VERSION") {
        Some(v) if !v.is_empty() => v,
        _ => env!("CARGO_PKG_VERSION"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reported_version_is_never_empty() {
        assert!(!version().is_empty());
    }

    #[test]
    fn falls_back_to_crate_version_when_override_absent() {
        if option_env!("QNTX_PLUGIN_VERSION").is_none() {
            assert_eq!(version(), env!("CARGO_PKG_VERSION"));
        }
    }
}
