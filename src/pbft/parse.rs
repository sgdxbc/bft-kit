use std::time::Duration;

use crate::parse::Options;

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
            workload: options.clone().try_into()?,
            replica: options.clone().try_into()?,
            service: options.clone().try_into()?,
            use_tcp: options.try_get("use_tcp")?.unwrap_or(false),
            tick_interval: Duration::from_secs_f32(options.get("tick_interval")?),
        })
    }
}
