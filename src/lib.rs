pub mod app;
pub mod crypto;
pub mod parse;
pub mod replication;
pub mod service;
pub mod state;
pub mod transport;
pub mod workload;

#[derive(Debug, bincode::Encode, bincode::Decode)]
pub enum Never {}

// similar to tracing_subscriber::fmt::init() but reports spans
// why init() defaults to not report spans? i don't understand
// if this further grows move it into dedicated module
pub fn init_logging() {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt as _};
    fmt_common().finish().with(targets_layer()).init();
}

pub fn init_logging_file(log_file: std::fs::File) {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt as _};
    fmt_common()
        .with_ansi(false)
        .with_writer(log_file)
        .finish()
        // https://docs.rs/tracing-subscriber/latest/src/tracing_subscriber/fmt/mod.rs.html#1200
        .with(targets_layer())
        .init();
}

fn fmt_common() -> tracing_subscriber::fmt::SubscriberBuilder {
    use tracing::level_filters::LevelFilter;
    use tracing_subscriber::fmt::format::FmtSpan;
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        // .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .with_span_events(FmtSpan::CLOSE)
        .with_file(true)
        .with_line_number(true)
}

fn targets_layer() -> tracing_subscriber::filter::Targets {
    use std::{env, str::FromStr as _};

    use tracing_subscriber::{filter::Targets, fmt::Subscriber};
    // https://docs.rs/tracing-subscriber/0.3.19/src/tracing_subscriber/fmt/mod.rs.html#1200
    match env::var("RUST_LOG") {
        Ok(var) => Targets::from_str(&var)
            .map_err(|e| {
                eprintln!("Ignoring `RUST_LOG={var:?}`: {e}");
            })
            .unwrap_or_default(),
        Err(env::VarError::NotPresent) => {
            Targets::new().with_default(Subscriber::DEFAULT_MAX_LEVEL)
        }
        Err(e) => {
            eprintln!("Ignoring `RUST_LOG`: {e}");
            Targets::new().with_default(Subscriber::DEFAULT_MAX_LEVEL)
        }
    }
}

pub fn set_affinity_block_on<F: Future<Output = anyhow::Result<T>>, T>(f: F) -> anyhow::Result<T> {
    let core_ids = std::sync::Mutex::new(
        core_affinity::get_core_ids()
            .unwrap_or_else(|| {
                tracing::warn!("cannot retrieve core ids");
                Vec::new()
            })
            .into_iter(),
    );
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .on_thread_start(move || {
            let Some(core_id) = core_ids.lock().unwrap().next() else {
                tracing::warn!("worker thread without affinity");
                return;
            };
            if !core_affinity::set_for_current(core_id) {
                tracing::warn!(?core_id, "set affinity failed")
            }
        })
        .build()?
        .block_on(f)
}

pub fn fmt_bytes(bytes: &[u8], f: &mut impl std::fmt::Write) -> std::fmt::Result {
    use std::fmt::Write as _;
    let prefix_hex = bytes.iter().take(4).fold(String::new(), |mut s, b| {
        write!(&mut s, "{b:02x}").unwrap();
        s
    });
    write!(
        f,
        "[{}]({prefix_hex}{})",
        bytes.len(),
        if bytes.len() > 4 { "..." } else { "" }
    )
}
