//! Where the log goes when nobody is on the box.
//!
//! The plugin's own file said nothing between "initialized" and "shutting
//! down" for a week of hourly deaths. Everything it logs from here on is
//! shipped as well as written, and an error is raised as an issue. The node
//! does the same for itself; this is the plugin's half of the same picture.
//!
//! The DSN is an ingest key. It arrives in the plugin's config section the
//! way every other value does, and a config without one ships nothing.

use std::time::Duration;

use sentry::integrations::tracing::EventFilter;
use tracing::Subscriber;
use tracing_subscriber::registry::LookupSpan;

/// The config key the DSN is read from, and the one naming the environment.
pub const DSN_KEY: &str = "sentry_dsn";
pub const ENVIRONMENT_KEY: &str = "sentry_environment";

/// The tracing layer. Installed once at startup; until `bind` runs it has no
/// client and drops what it sees.
///
/// An error is an issue. Everything from info up is a log line, and also a
/// breadcrumb, so the issue an error raises carries what led to it.
pub fn layer<S>() -> sentry::integrations::tracing::SentryLayer<S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    sentry::integrations::tracing::layer().event_filter(|md| match *md.level() {
        tracing::Level::ERROR => EventFilter::Event | EventFilter::Log,
        tracing::Level::WARN | tracing::Level::INFO => EventFilter::Breadcrumb | EventFilter::Log,
        _ => EventFilter::Ignore,
    })
}

/// Bind a client to the layer. `name` is the plugin as the node knows it, and
/// the release it reports is that name at this crate's version, so an issue
/// says which build of which plugin raised it.
///
/// The guard is what keeps the client alive; drop it and the pipe closes.
pub fn bind(name: &str, dsn: &str, environment: Option<&str>) -> sentry::ClientInitGuard {
    let release = format!("{}@{}", name, crate::version::version());
    let environment = environment.unwrap_or("production").to_string();
    let mut options = sentry::ClientOptions::default();
    options.release = Some(release.clone().into());
    options.environment = Some(environment.clone().into());
    options.server_name = hostname().map(Into::into);
    options.attach_stacktrace = true;
    // Deprecated in name only: the tracing integration still reads it, and
    // without it the Log filter above goes nowhere.
    #[allow(deprecated)]
    {
        options.enable_logs = true;
    }
    let guard = sentry::init((dsn, options));
    tracing::info!(release, environment, "Shipping logs to Sentry");
    guard
}

/// Drain what is queued. Sentry sends in batches on its own clock, and a
/// process that ends before the batch goes loses the lines that say why.
pub fn flush() {
    if let Some(client) = sentry::Hub::current().client() {
        client.close(Some(Duration::from_secs(2)));
    }
}

fn hostname() -> Option<String> {
    std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}
