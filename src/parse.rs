//! https://github.com/sgdxbc/bft-kit/discussions/4
use std::{collections::HashMap, str::FromStr};

#[derive(Debug, Clone, Default)]
pub struct Settings(HashMap<String, Vec<String>>);

impl Settings {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn parse(&mut self, s: &str) {
        for line in s.lines() {
            let mut split = line.split_whitespace();
            let (Some(key), Some(value)) = (split.next(), split.next()) else {
                continue;
            };
            self.0.entry(key.into()).or_default().push(value.into())
        }
    }

    pub fn get<T: FromStr>(&self, key: &str) -> anyhow::Result<T>
    where
        T::Err: std::error::Error + Send + Sync + 'static,
    {
        let Some(values) = self.0.get(key) else {
            anyhow::bail!("missing options key {key}")
        };
        Ok(values.last().unwrap().parse()?) // later one overrides
    }

    pub fn try_get<T: FromStr>(&self, key: &str) -> anyhow::Result<Option<T>>
    where
        T::Err: std::error::Error + Send + Sync + 'static,
    {
        let Some(values) = self.0.get(key) else {
            return Ok(None);
        };
        Ok(Some(values.last().unwrap().parse()?)) // later one overrides
    }

    pub fn get_values<T: FromStr>(&self, key: &str) -> anyhow::Result<Vec<T>>
    where
        T::Err: std::error::Error + Send + Sync + 'static,
    {
        let Some(values) = self.0.get(key) else {
            anyhow::bail!("missing options key {key}")
        };
        Ok(values
            .iter()
            .map(|value| value.parse())
            .collect::<Result<_, _>>()?)
    }
}

// impl TryFrom<Options> for crate::transport::ReplicaConfig {
//     type Error = anyhow::Error;

//     fn try_from(options: Options) -> Result<Self, Self::Error> {
//         Ok(Self {
//             // if necessary, allow nonconsecutive replica id
//             server_internal_addresses: options
//                 .get_values("server_internal_address")?
//                 .into_iter()
//                 .enumerate()
//                 .map(|(i, addr)| (i as _, addr))
//                 .collect(),
//             server_interconnect_delay: Duration::from_secs_f32(
//                 options.get("server_interconnect_delay")?,
//             ),
//         })
//     }
// }

// impl TryFrom<Options> for crate::transport::ServiceConfig {
//     type Error = anyhow::Error;

//     fn try_from(options: Options) -> Result<Self, Self::Error> {
//         Ok(Self {
//             // if necessary, allow nonconsecutive replica id
//             server_external_addresses: options
//                 .get_values("server_external_address")?
//                 .into_iter()
//                 .enumerate()
//                 .map(|(i, addr)| (i as _, addr))
//                 .collect(),
//         })
//     }
// }

// impl TryFrom<Options> for crate::workload::ClientConfig {
//     type Error = anyhow::Error;

//     fn try_from(options: Options) -> Result<Self, Self::Error> {
//         Ok(if !matches!(options.try_get("open_loop")?, Some(true)) {
//             Self::CloseLoop
//         } else {
//             Self::OpenLoop(crate::workload::OpenLoopClientConfig {
//                 num_max_inflight: options.get("num_max_concurrent")?,
//                 sending_rate: options.get("sending_rate")?,
//             })
//         })
//     }
// }

// impl TryFrom<Options> for crate::workload::Config {
//     type Error = anyhow::Error;

//     fn try_from(options: Options) -> Result<Self, Self::Error> {
//         Ok(Self {
//             client: options.clone().try_into()?,
//             num_client: options.get("num_client")?,
//             duration: Duration::from_secs_f32(options.get("client_duration")?),
//         })
//     }
// }
