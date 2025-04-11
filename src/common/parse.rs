use std::{collections::HashMap, str::FromStr};

#[derive(Debug, Clone, Default)]
pub struct Options(HashMap<String, Vec<String>>);

impl Options {
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
