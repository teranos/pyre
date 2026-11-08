//! Where the log goes when nobody is on the box.
//!
//! The plugin's own file said nothing between "initialized" and "shutting
//! down" for a week of hourly deaths. Everything it logs from here on is
//! shipped as well as written, and an error is raised as an issue. The node
//! does the same for itself; this is the plugin's half of the same picture.
//!
//! The DSN is an ingest key. It arrives in the plugin's config section the
//! way every other value does, and a config without one ships nothing.

use std::sync::{Arc, Weak};
use std::time::Duration;

use parking_lot::Mutex;
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

/// The hubs of the runtime's threads, and the client they were last handed.
///
/// Sentry keeps a hub per thread, forked from the process hub when the thread
/// first logs, and `sentry::init` binds its client to the calling thread's hub
/// alone. The DSN arrives at Initialize, long after the runtime's workers took
/// their hubs, so without this only the worker that ran Initialize would ship.
#[derive(Default)]
struct Fleet {
    client: Option<Arc<sentry::Client>>,
    hubs: Vec<Weak<sentry::Hub>>,
}

static FLEET: Mutex<Fleet> = Mutex::new(Fleet { client: None, hubs: Vec::new() });

/// Called on every thread the runtime starts, workers and blocking threads
/// alike: the thread's hub is remembered so a client handed later reaches it,
/// and given the client if one is already handed.
pub fn adopt_thread() {
    let hub = sentry::Hub::current();
    let mut fleet = FLEET.lock();
    fleet.hubs.retain(|h| h.strong_count() > 0);
    fleet.hubs.push(Arc::downgrade(&hub));
    if let Some(client) = &fleet.client {
        hub.bind_client(Some(client.clone()));
    }
}

/// Bind the client to every hub adopted so far, and to the process hub, which
/// is the one the thread that ends the process flushes. A second client
/// replaces the first on all of them.
pub fn hand_client(client: Arc<sentry::Client>) {
    let mut fleet = FLEET.lock();
    fleet.hubs.retain(|h| h.strong_count() > 0);
    for hub in fleet.hubs.iter().filter_map(Weak::upgrade) {
        hub.bind_client(Some(client.clone()));
    }
    sentry::Hub::main().bind_client(Some(client.clone()));
    fleet.client = Some(client);
}

/// Bind a client to the layer. `name` is the plugin as the node knows it, and
/// the release it reports is that name at this crate's version, so an issue
/// says which build of which plugin raised it.
///
/// A DSN that does not parse is an error, not a panic in the SDK.
///
/// The guard is what keeps the client alive; drop it and the pipe closes.
pub fn bind(
    name: &str,
    dsn: &str,
    environment: Option<&str>,
) -> Result<sentry::ClientInitGuard, String> {
    let dsn: sentry::types::Dsn = dsn
        .parse()
        .map_err(|e| format!("{DSN_KEY} is not a Sentry DSN: {e}"))?;
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
    if let Some(client) = sentry::Hub::current().client() {
        hand_client(client);
    }
    tracing::info!(release, environment, "Shipping logs to Sentry");
    Ok(guard)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Barrier;

    /// Counts the events a client sends, and sends them nowhere.
    #[derive(Default)]
    struct Counting(AtomicUsize);

    impl sentry::Transport for Counting {
        fn send_envelope(&self, envelope: sentry::Envelope) {
            if envelope.event().is_some() {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    #[test]
    fn a_dsn_that_is_not_one_is_an_error_and_not_a_panic() {
        let err = bind("pyre", "not a dsn", None)
            .err()
            .expect("a malformed DSN must be refused");
        assert!(err.contains(DSN_KEY), "{err}");
    }

    /// The runtime's workers take their hubs when they start, before Initialize
    /// hands over the DSN. Each of them must ship once it is handed.
    #[test]
    fn a_client_handed_after_the_workers_started_ships_from_every_worker() {
        const WORKERS: usize = 4;
        let transport = Arc::new(Counting::default());
        let mut options = sentry::ClientOptions::default();
        options.dsn = Some("https://tim@example.com/1".parse().unwrap());
        options.transport = Some(Arc::new(transport.clone()));
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(WORKERS)
            .on_thread_start(adopt_thread)
            .enable_all()
            .build()
            .unwrap();

        hand_client(Arc::new(sentry::Client::from(options)));

        // Each task holds its worker until all of them have arrived, so the
        // four events leave four different threads.
        let barrier = Arc::new(Barrier::new(WORKERS));
        let threads = runtime.block_on(async {
            let tasks: Vec<_> = (0..WORKERS)
                .map(|_| {
                    let barrier = barrier.clone();
                    tokio::spawn(async move {
                        barrier.wait();
                        sentry::capture_message("from a worker", sentry::Level::Error);
                        std::thread::current().id()
                    })
                })
                .collect();
            let mut ids = HashSet::new();
            for task in tasks {
                ids.insert(task.await.unwrap());
            }
            ids
        });

        assert_eq!(threads.len(), WORKERS, "the events did not leave distinct workers");
        assert_eq!(transport.0.load(Ordering::SeqCst), WORKERS);
    }
}
