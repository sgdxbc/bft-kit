use std::{error::Error, net::SocketAddr, str::FromStr, time::Duration};

use anyhow::Context as _;

use crate::common::ReplicaId;

use super::BlockNum;

#[derive(Debug, Default, Clone)]
pub struct Options {
    num_faulty: Option<ReplicaId>,
    num_replica: Option<ReplicaId>,
    // add client config options if client config requires parsing
    replica_id: Option<ReplicaId>,
    max_num_inflight: Option<BlockNum>,
    max_batch_size: Option<usize>,
    num_client: Option<usize>,
    client_tick_interval: Option<f32>,
    client_duration: Option<f32>,
    replica_tick_interval: Option<f32>,
    replica_external_addresses: Vec<SocketAddr>,
    replica_internal_addresses: Vec<SocketAddr>,
    replica_connect_delay: Option<f32>,
}

impl Options {
    pub fn new() -> Self {
        Self::default()
    }
}

fn parse_line<T: FromStr>(key: &str, field: Option<&str>) -> anyhow::Result<T>
where
    T::Err: Error + Send + Sync + 'static,
{
    field
        .ok_or(anyhow::format_err!("missing value for {key}"))?
        .parse()
        .context(format!("parse {key}"))
}

impl Options {
    pub fn parse(&mut self, s: &str) -> anyhow::Result<()> {
        for line in s.lines() {
            let mut split = line.split_whitespace();
            match split.next() {
                Some("num_faulty") => {
                    self.num_faulty = Some(parse_line("num_faulty", split.next())?)
                }
                Some("num_replica") => {
                    self.num_replica = Some(parse_line("num_replica", split.next())?)
                }
                Some("replica_id") => {
                    self.replica_id = Some(parse_line("replica_id", split.next())?)
                }
                Some("max_num_inflight") => {
                    self.max_num_inflight = Some(parse_line("max_num_inflight", split.next())?)
                }
                Some("max_batch_size") => {
                    self.max_batch_size = Some(parse_line("max_batch_size", split.next())?)
                }
                Some("num_client") => {
                    self.num_client = Some(parse_line("num_client", split.next())?)
                }
                Some("client_tick_interval") => {
                    self.client_tick_interval =
                        Some(parse_line("client_tick_interval", split.next())?)
                }
                Some("client_duration") => {
                    self.client_duration = Some(parse_line("client_duration", split.next())?)
                }
                Some("replica_tick_interval") => {
                    self.replica_tick_interval =
                        Some(parse_line("replica_tick_interval", split.next())?)
                }
                Some("replica_external_address") => self
                    .replica_external_addresses
                    .push(parse_line("replica_external_address", split.next())?),
                Some("replica_internal_address") => self
                    .replica_internal_addresses
                    .push(parse_line("replica_internal_address", split.next())?),
                Some("replica_connect_delay") => {
                    self.replica_connect_delay =
                        Some(parse_line("replica_connect_delay", split.next())?)
                }
                _ => {}
            }
        }
        Ok(())
    }
}

impl TryFrom<Options> for super::Spec {
    type Error = anyhow::Error; // TODO

    fn try_from(options: Options) -> Result<Self, Self::Error> {
        Ok(Self {
            num_faulty: options
                .num_faulty
                .ok_or(anyhow::format_err!("missing num_faulty"))?,
            num_replica: options
                .num_replica
                .ok_or(anyhow::format_err!("missing num_replica"))?,
        })
    }
}

impl TryFrom<Options> for super::ReplicaConfig {
    type Error = anyhow::Error; // TODO

    fn try_from(options: Options) -> Result<Self, Self::Error> {
        let mut config = super::ReplicaConfig::new_basic(
            options.clone().try_into()?,
            options
                .replica_id
                .ok_or(anyhow::format_err!("missing replica_id"))?,
        );
        if let Some(max_batch_size) = options.max_batch_size {
            config.max_batch_size = max_batch_size
        }
        if let Some(max_num_inflight) = options.max_num_inflight {
            config.max_num_inflight = max_num_inflight
        }
        Ok(config)
    }
}

impl TryFrom<Options> for super::transport::TaskConfig {
    type Error = anyhow::Error; // TODO

    fn try_from(options: Options) -> Result<Self, Self::Error> {
        // technical speaking empty != missing, but in practice a setting without any
        // replica address is hardly valid
        anyhow::ensure!(
            !options.replica_external_addresses.is_empty(),
            "missing replica_external_address"
        );
        anyhow::ensure!(
            !options.replica_internal_addresses.is_empty(),
            "missing replica_internal_address"
        );
        Ok(Self {
            num_client: options
                .num_client
                .ok_or(anyhow::format_err!("missing num_client"))?,
            client_tick_interval: Duration::from_secs_f32(
                options
                    .client_tick_interval
                    .ok_or(anyhow::format_err!("missing client_tick_interval"))?,
            ),
            client_duration: Duration::from_secs_f32(
                options
                    .client_duration
                    .ok_or(anyhow::format_err!("missing client_duration"))?,
            ),
            replica_tick_interval: Duration::from_secs_f32(
                options
                    .replica_tick_interval
                    .ok_or(anyhow::format_err!("missing replica_tick_interval"))?,
            ),
            replica_external_addresses: options.replica_external_addresses,
            replica_internal_addresses: options.replica_internal_addresses,
            replica_connect_delay: Duration::from_secs_f32(
                options
                    .replica_connect_delay
                    .ok_or(anyhow::format_err!("missing replica_connect_delay"))?,
            ),
        })
    }
}
