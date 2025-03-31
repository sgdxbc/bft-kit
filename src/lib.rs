pub mod common;
pub mod crypto;
pub mod pbft;

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
