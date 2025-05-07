use std::time::Duration;

use crate::{ReplicaId, crypto::threshold::givre_replica_key_shares, parse::Options};

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
        let mut config = super::ReplicaCoreConfig {
            spec: options.clone().try_into()?,
            id: options.get("replica_id")?,
            open_loop: matches!(options.try_get("open_loop")?, Some(true)),
            max_batch_size: 1,
        };
        if let Some(max_batch_size) = options.try_get("max_batch_size")? {
            config.max_batch_size = max_batch_size
        }
        Ok(config)
    }
}

impl TryFrom<Options> for super::transport::TaskConfig {
    type Error = anyhow::Error; // TODO

    fn try_from(options: Options) -> Result<Self, Self::Error> {
        Ok(Self {
            client: options.clone().try_into()?,
            replica: options.clone().try_into()?,
            service: options.clone().try_into()?,
            num_client: options.get("num_client")?,
            client_duration: Duration::from_secs_f32(options.get("client_duration")?),
            tick_interval: Duration::from_secs_f32(options.get("tick_interval")?),
        })
    }
}

// technically a TryFrom but nontrivial generation goes on
impl super::CryptoConfig {
    pub fn new(options: Options) -> anyhow::Result<Self> {
        let num_replica = options.get("num_replica")?;
        let num_faulty = options.get("num_faulty")?;
        let replica_id = options.get::<ReplicaId>("replica_id")?;
        let key_share =
            givre_replica_key_shares(num_replica, num_faulty)[replica_id as usize].clone();
        Ok(Self {
            key_share,
            supply_size: options.get("supply_size")?,
            refill_threshold: options.get("refill_threshold")?,
        })
    }
}
