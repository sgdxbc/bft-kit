pub mod command_pool;
pub use command_pool::CommandPool;
pub mod big_bft;
pub mod common;
pub mod crypto;
pub mod hotstuff;
pub mod parse;
pub mod pbft;
#[cfg(test)]
pub mod testing; // no test inside, common infrastructure for writing tests
pub mod transport;
pub mod unreplicated;
pub mod workload;

// similar to tracing_subscriber::fmt::init() but reports spans
// why init() defaults to not report spans? i don't understand
// if this further grows move it into dedicated module
pub fn init_logging() {
    use std::{env, str::FromStr as _};

    use tracing::level_filters::LevelFilter;
    use tracing_subscriber::{
        filter::Targets,
        fmt::{Subscriber, format::FmtSpan},
        layer::SubscriberExt,
        util::SubscriberInitExt as _,
    };

    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .finish()
        // https://docs.rs/tracing-subscriber/latest/src/tracing_subscriber/fmt/mod.rs.html#1200
        .with(match env::var("RUST_LOG") {
            Ok(var) => Targets::from_str(&var)
                .map_err(|e| {
                    eprintln!("Ignoring `RUST_LOG={:?}`: {}", var, e);
                })
                .unwrap_or_default(),
            Err(env::VarError::NotPresent) => {
                Targets::new().with_default(Subscriber::DEFAULT_MAX_LEVEL)
            }
            Err(e) => {
                eprintln!("Ignoring `RUST_LOG`: {}", e);
                Targets::new().with_default(Subscriber::DEFAULT_MAX_LEVEL)
            }
        })
        .init();
}
