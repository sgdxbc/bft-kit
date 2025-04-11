use std::time::Duration;

use crate::common::parse::Options;

impl TryFrom<Options> for super::Spec {
    type Error = anyhow::Error; // TODO

    fn try_from(options: Options) -> Result<Self, Self::Error> {
        Ok(Self {
            num_faulty: options.get("num_faulty")?,
            num_replica: options.get("num_replica")?,
        })
    }
}

impl TryFrom<Options> for super::ReplicaCoreConfig {
    type Error = anyhow::Error; // TODO

    fn try_from(options: Options) -> Result<Self, Self::Error> {
        let mut config = super::ReplicaCoreConfig::new_basic(
            options.clone().try_into()?,
            options.get("replica_id")?,
        );
        if let Some(max_batch_size) = options.try_get("max_batch_size")? {
            config.max_batch_size = max_batch_size
        }
        if let Some(max_num_inflight) = options.try_get("max_num_inflight")? {
            config.max_num_inflight = max_num_inflight
        }
        Ok(config)
    }
}

impl TryFrom<Options> for super::transport::TaskConfig {
    type Error = anyhow::Error; // TODO

    fn try_from(options: Options) -> Result<Self, Self::Error> {
        Ok(Self {
            num_client: options.get("num_client")?,
            client_tick_interval: Duration::from_secs_f32(options.get("client_tick_interval")?),
            client_duration: Duration::from_secs_f32(options.get("client_duration")?),
            replica_tick_interval: Duration::from_secs_f32(options.get("replica_tick_interval")?),
            replica_external_addresses: options.get_values("replica_external_address")?,
            replica_internal_addresses: options.get_values("replica_internal_address")?,
            replica_connect_delay: Duration::from_secs_f32(options.get("replica_connect_delay")?),
        })
    }
}
