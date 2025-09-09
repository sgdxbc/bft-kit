use std::{
    collections::{BTreeMap, HashMap, HashSet},
    iter::once,
    mem::take,
    sync::Arc,
};

use bincode::{Decode, Encode};
use primitive_types::H256;
use rand::{Rng as _, SeedableRng as _, rngs::StdRng, seq::IteratorRandom as _};
use rocksdb::DB;
use tokio::{
    select, spawn,
    sync::{
        mpsc::{Receiver, Sender, channel},
        oneshot,
    },
    task::{JoinHandle, spawn_blocking},
};
use tokio_util::bytes::Bytes;

use crate::{network::Dest, replica::ReplicaIndex, task::SegmentedTask};

pub type StorageKey = H256;
pub type NodeIndex = u16;
type StateVersion = u64;
type ActiveGroupIndex = NodeIndex;
type StripeIndex = u16;
type StripeShardIndex = usize;

pub enum StorageOp {
    Fetch(StorageKey, oneshot::Sender<StorageRes<Option<Bytes>>>),
    Bump(Bump, oneshot::Sender<StorageRes<()>>),
    // archive
}

#[derive(Debug)]
pub enum StorageRes<T> {
    Ok(T),
    Forward(usize),
}

pub struct Bump {
    pub inserts: HashMap<StorageKey, Bytes>,
    pub deletes: Vec<StorageKey>,
}

pub struct Storage {
    db: Arc<DB>,
    config: ShardedStorageConfig,

    version: StateVersion,

    rx_op: Receiver<StorageOp>,
    rx_message: Receiver<Message>,
    tx_archive_push: Sender<message::ArchivePush>,
    tx_archived: Sender<message::Archived>,
    tx_epoch_change: Sender<(StateVersion, oneshot::Sender<()>)>,
    rx_entered: oneshot::Receiver<()>,
}

impl Storage {
    pub fn spawn(
        task: SegmentedTask,
        db: impl Into<Arc<DB>> + Send + 'static,
        config: ShardedStorageConfig,
        node_indices: HashSet<NodeIndex>,
        node_table: Vec<ReplicaIndex>,
        rx_op: Receiver<StorageOp>,
        tx_message: Sender<(Dest, Message)>,
        rx_message: Receiver<Message>,
    ) -> JoinHandle<()> {
        spawn(task.clone().wrap_fallible(Self::start(
            task,
            db,
            config,
            node_indices,
            node_table,
            rx_op,
            tx_message,
            rx_message,
        )))
    }

    async fn start(
        task: SegmentedTask,
        db: impl Into<Arc<DB>>,
        config: ShardedStorageConfig,
        node_indices: HashSet<NodeIndex>,
        node_table: Vec<ReplicaIndex>,
        rx_op: Receiver<StorageOp>,
        tx_message: Sender<(Dest, Message)>,
        rx_message: Receiver<Message>,
    ) -> anyhow::Result<()> {
        let db = db.into();
        let network_dispatcher = NetworkDispatcher {
            tx_message,
            node_table,
        };

        let (tx_epoch_change, rx_epoch_change) = channel(1);
        let (tx_entered, rx_entered) = oneshot::channel();
        let _ = tx_epoch_change.try_send((0, tx_entered));
        let (tx_archived, rx_archived) = channel(100);
        let (tx_archive_push, rx_archive_push) = channel(100);

        let mut storage = Self {
            db: db.clone(),
            config: config.clone(),
            version: 0,
            rx_op,
            tx_epoch_change,
            rx_entered,
            rx_message,
            tx_archive_push,
            tx_archived: tx_archived.clone(),
        };
        let storage = spawn(
            task.clone()
                .wrap_fallible(async move { storage.run().await }),
        );

        let archive_worker = ArchiveWorker::spawn(
            task.clone(),
            db.clone(),
            network_dispatcher,
            config.clone(),
            node_indices,
            rx_epoch_change,
            rx_archive_push,
            tx_archived,
        );
        let collect_worker = CollectWorker::spawn(db, task, config, rx_archived);

        storage.await?;
        archive_worker.await?;
        collect_worker.await?;
        Ok(())
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        loop {
            enum Event {
                Op(StorageOp),
                Message(Message),
                EnteredEpoch,
            }
            match select! {
                Some(op) = self.rx_op.recv() => Event::Op(op),
                Some(msg) = self.rx_message.recv() => Event::Message(msg),
                Ok(()) = &mut self.rx_entered => Event::EnteredEpoch,
                else => break,
            } {
                Event::Op(op) => self.handle_op(op).await?,
                Event::Message(message) => self.handle_message(message).await?,
                Event::EnteredEpoch => self.handle_entered_epoch().await?,
            }
        }
        while let Some(op) = self.rx_op.recv().await {
            self.handle_op(op).await?
        }
        Ok(())
    }

    async fn handle_op(&mut self, op: StorageOp) -> anyhow::Result<()> {
        match op {
            StorageOp::Fetch(key, tx_value) => {
                let value = get_versioned(
                    self.db.clone(),
                    &key,
                    self.version,
                    self.config.stripe_of(&key),
                )
                .await?;
                let _ = tx_value.send(StorageRes::Ok(value));
            }
            StorageOp::Bump(bump, tx_ok) => {
                self.version += 1;
                for (key, value) in bump.inserts {
                    put_versioned(
                        self.db.clone(),
                        &key,
                        self.version,
                        value,
                        self.config.stripe_of(&key),
                    )
                    .await?
                }
                for key in bump.deletes {
                    delete_versioned(
                        self.db.clone(),
                        &key,
                        self.version,
                        self.config.stripe_of(&key),
                    )
                    .await?
                }
                let _ = tx_ok.send(StorageRes::Ok(()));
            }
        }
        Ok(())
    }

    async fn handle_message(&mut self, message: Message) -> anyhow::Result<()> {
        match message {
            Message::ArchivePush(push) => {
                let _ = self.tx_archive_push.send(push).await;
            }
            Message::Archived(archived) => {
                let _ = self.tx_archived.send(archived).await;
            }
        }
        Ok(())
    }

    async fn handle_entered_epoch(&mut self) -> anyhow::Result<()> {
        if self.config.bypass_vote {
            let (tx_entered, rx_entered) = oneshot::channel();
            let _ = self.tx_epoch_change.send((self.version, tx_entered)).await;
            self.rx_entered = rx_entered;
            return Ok(());
        }

        todo!()
    }
}

#[derive(Debug, Clone)]
pub struct ShardedStorageConfig {
    num_node: NodeIndex, // virtual "storage node"
    num_faulty_node: NodeIndex,
    num_stripe: StripeIndex,
    num_active_copy: usize,
    // pub repair_threshold: NodeIndex,
    bypass_vote: bool,
}

impl ShardedStorageConfig {
    // active tier
    fn num_active_group(&self) -> ActiveGroupIndex {
        self.num_node
    }

    fn primary_node_of_group(&self, index: ActiveGroupIndex) -> NodeIndex {
        index
    }

    fn as_primary_in_groups(
        &self,
        node_indices: &HashSet<NodeIndex>,
    ) -> impl Iterator<Item = ActiveGroupIndex> {
        node_indices.iter().copied()
    }

    fn nodes_of_group(&self, index: ActiveGroupIndex) -> impl Iterator<Item = NodeIndex> {
        let sampled = (0..self.num_node - 1).choose_multiple(
            &mut StdRng::seed_from_u64(index as _),
            self.num_active_copy - 1,
        );
        let node_index = self.primary_node_of_group(index);
        once(node_index).chain(
            sampled
                .into_iter()
                .map(move |i| if i >= node_index { i + 1 } else { i }),
        )
    }

    fn groups_of_nodes(
        &self,
        node_indices: &HashSet<NodeIndex>,
    ) -> impl Iterator<Item = ActiveGroupIndex> {
        (0..self.num_active_group()).filter(|&index| {
            self.nodes_of_group(index)
                .any(|node_index| node_indices.contains(&node_index))
        })
    }

    fn group_of(&self, key: &StorageKey) -> ActiveGroupIndex {
        StdRng::from_seed(key.0).random_range(0..self.num_active_group())
    }

    fn nodes_of(&self, key: &StorageKey) -> impl Iterator<Item = NodeIndex> {
        self.nodes_of_group(self.group_of(key))
    }

    // archive tier
    fn repair_threshold(&self) -> NodeIndex {
        self.num_faulty_node + 1 // make this configurable only if needed (probably for performance)
    }

    fn stripe_width(&self) -> NodeIndex {
        self.repair_threshold() + self.num_faulty_node * 2
    }

    fn stripe_of(&self, key: &StorageKey) -> StripeIndex {
        // assuming the keys are uniformly distributed, by hashing
        (key.to_low_u64_le() % self.num_stripe as u64) as _
    }

    fn archive_nodes_of_stripe(&self, index: StripeIndex) -> impl Iterator<Item = NodeIndex> {
        (index..)
            .take(self.stripe_width() as _)
            .map(|index| (index % self.num_node as StripeIndex) as _)
    }

    fn archive_placements(
        &self,
        node_index: NodeIndex,
    ) -> impl Iterator<Item = (StripeIndex, StripeShardIndex)> {
        (0..self.num_stripe).filter_map(move |stripe_index| {
            self.archive_nodes_of_stripe(stripe_index)
                .enumerate()
                .find_map(|(shard_index, placed_node_index)| {
                    if placed_node_index == node_index {
                        Some((stripe_index, shard_index))
                    } else {
                        None
                    }
                })
        })
    }
}

// key spec
// {stripe index:04x}/{key:x}.{version:08x}(+delete)
// {stripe index:04x}:{version:08x}-{shard index:08x}

async fn get_versioned(
    db: Arc<DB>,
    key: &StorageKey,
    version: StateVersion,
    stripe_index: StripeIndex,
) -> anyhow::Result<Option<Bytes>> {
    let prefix = format!("{stripe_index:04x}/{key:x}");
    let seek_key = format!("{prefix}.{version:08x}");
    let value = spawn_blocking(move || {
        let mut iter = db.raw_iterator();
        iter.seek_for_prev(seek_key);
        iter.status()?;
        let value = if let Some((found_key, value)) = iter.item()
            && let Some(postfix) = found_key.strip_prefix(prefix.as_bytes())
            && !postfix.ends_with(b".delete")
        {
            Some(Bytes::copy_from_slice(value))
        } else {
            None
        };
        anyhow::Ok(value)
    })
    .await??;
    Ok(value)
}

async fn put_versioned(
    db: Arc<DB>,
    key: &StorageKey,
    version: StateVersion,
    value: Bytes,
    stripe_index: StripeIndex,
) -> anyhow::Result<()> {
    let key = format!("{stripe_index:04x}/{key:x}.{version:08x}");
    spawn_blocking(move || db.put(key, value)).await??;
    Ok(())
}

async fn delete_versioned(
    db: Arc<DB>,
    key: &StorageKey,
    version: StateVersion,
    stripe_index: StripeIndex,
) -> anyhow::Result<()> {
    let key = format!("{stripe_index:04x}/{key:x}.{version:08x}+delete");
    spawn_blocking(move || db.put(key, b"")).await??;
    Ok(())
}

struct ArchiveWorker {
    db: Arc<DB>,
    network_dispatcher: NetworkDispatcher,
    config: ShardedStorageConfig,
    node_indices: HashSet<NodeIndex>,
    group_indices: HashSet<ActiveGroupIndex>,

    epoch: u64,
    state: Option<ArchivingState>,
    reorder_archive_pushes: HashMap<(u64, StripeIndex), Vec<message::ArchivePush>>,

    rx_epoch_change: Receiver<(StateVersion, oneshot::Sender<()>)>,
    rx_archive_push: Receiver<message::ArchivePush>,
    tx_archived: Sender<message::Archived>,
}

struct ArchivingState {
    version: StateVersion,
    stripe_index: StripeIndex,
    stripe_data: HashMap<ActiveGroupIndex, BTreeMap<[u8; 32], Vec<u8>>>,
    tx_entered: oneshot::Sender<()>,
}

impl ArchiveWorker {
    fn spawn(
        task: SegmentedTask,
        db: Arc<DB>,
        network_dispatcher: NetworkDispatcher,
        config: ShardedStorageConfig,
        node_indices: HashSet<NodeIndex>,
        rx_epoch_change: Receiver<(StateVersion, oneshot::Sender<()>)>,
        rx_archive_push: Receiver<message::ArchivePush>,
        tx_archived: Sender<message::Archived>,
    ) -> JoinHandle<()> {
        let mut worker = Self {
            db,
            network_dispatcher,
            group_indices: config.groups_of_nodes(&node_indices).collect(),
            node_indices,
            epoch: 0,
            state: None,
            reorder_archive_pushes: Default::default(),
            config,
            rx_epoch_change,
            rx_archive_push,
            tx_archived,
        };
        spawn(task.wrap_fallible(async move { worker.run().await }))
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        loop {
            enum Event {
                EpochChange((StateVersion, oneshot::Sender<()>)),
                ArchivePush(message::ArchivePush),
            }
            match select! {
                Some(epoch_change) = self.rx_epoch_change.recv() => Event::EpochChange(epoch_change),
                Some(archive_push) = self.rx_archive_push.recv() => Event::ArchivePush(archive_push),
                else => break,
            } {
                Event::EpochChange((version, tx_entered)) => {
                    self.handle_epoch_change(version, tx_entered).await?
                }
                Event::ArchivePush(push) => self.handle_archive_push(push).await?,
            }
        }
        while let Some((version, tx_entered)) = self.rx_epoch_change.recv().await {
            self.handle_epoch_change(version, tx_entered).await?
        }
        Ok(())
    }

    async fn handle_epoch_change(
        &mut self,
        version: StateVersion,
        tx_entered: oneshot::Sender<()>,
    ) -> anyhow::Result<()> {
        self.epoch += 1;
        tracing::info!("entering epoch {} at version {version}", self.epoch);

        let state = ArchivingState {
            version,
            stripe_index: 0,
            stripe_data: Default::default(),
            tx_entered,
        };
        let replaced = self.state.replace(state);
        if replaced.is_some() {
            tracing::warn!("previous archiving state was not completed")
        }
        if self.config.num_node == 1 {
            self.complete_archive().await;
            Ok(())
        } else {
            self.start_stripe().await
        }
    }

    async fn start_stripe(&mut self) -> anyhow::Result<()> {
        let Some(state) = &mut self.state else {
            unimplemented!()
        };
        assert!(state.stripe_data.is_empty());
        if state.stripe_index == self.config.num_stripe {
            self.complete_archive().await;
            return Ok(());
        }

        for &group_index in &self.group_indices {
            state.stripe_data.insert(group_index, Default::default());
        }
        let local_data =
            snapshot_versioned(self.db.clone(), state.stripe_index, state.version).await?;
        for (key, value) in local_data {
            let group_index = self.config.group_of(&key);
            // temporary
            if !self.group_indices.contains(&group_index) {
                continue;
            }
            state
                .stripe_data
                .get_mut(&group_index)
                .unwrap()
                .insert(key.0, value);
        }

        for group_index in self.config.as_primary_in_groups(&self.node_indices) {
            let data = state.stripe_data[&group_index].clone();
            let archive_push = message::ArchivePush {
                epoch: self.epoch,
                stripe_index: state.stripe_index,
                group_index,
                data,
            };
            tracing::debug!(
                "push archive stripe {} group {}",
                state.stripe_index,
                group_index
            );
            self.network_dispatcher
                .send_to_all(Message::ArchivePush(archive_push))
                .await
        }

        if let Some(archive_pushes) = self
            .reorder_archive_pushes
            .remove(&(self.epoch, state.stripe_index))
        {
            for push in archive_pushes {
                self.handle_archive_push(push).await?
            }
        }

        self.may_archive_stripe().await
    }

    async fn handle_archive_push(&mut self, push: message::ArchivePush) -> anyhow::Result<()> {
        tracing::debug!(
            "handling archive push epoch {} stripe {} group {}",
            push.epoch,
            push.stripe_index,
            push.group_index
        );
        let Some(state) = &mut self.state else {
            if push.epoch <= self.epoch {
                tracing::warn!("ignoring stale archive push for epoch {}", push.epoch);
            } else {
                self.reorder_archive_pushes
                    .entry((push.epoch, push.stripe_index))
                    .or_default()
                    .push(push)
            }
            return Ok(());
        };
        if self.epoch < push.epoch || state.stripe_index < push.stripe_index {
            self.reorder_archive_pushes
                .entry((push.epoch, push.stripe_index))
                .or_default()
                .push(push);
            return Ok(());
        }

        state.stripe_data.insert(push.group_index, push.data);
        self.may_archive_stripe().await
    }

    async fn may_archive_stripe(&mut self) -> anyhow::Result<()> {
        let Some(state) = &mut self.state else {
            unimplemented!()
        };
        if (state.stripe_data.len() as StripeIndex) < self.config.num_active_group() {
            return Ok(());
        }

        tracing::debug!(?self.node_indices, "archive stripe {}", state.stripe_index);
        let stripe_data = take(&mut state.stripe_data);
        // TODO

        state.stripe_index += 1;
        Box::pin(self.start_stripe()).await
    }

    async fn complete_archive(&mut self) {
        let Some(state) = self.state.take() else {
            unimplemented!()
        };
        let _ = state.tx_entered.send(());

        let archived = message::Archived {
            version: state.version,
            node_indices: self.node_indices.clone(),
        };
        self.network_dispatcher
            .send_to_all(Message::Archived(archived.clone()))
            .await;
        let _ = self.tx_archived.send(archived).await;
    }
}

async fn snapshot_versioned(
    db: Arc<DB>,
    stripe_index: StripeIndex,
    archive_version: StateVersion,
) -> anyhow::Result<Vec<(H256, Vec<u8>)>> {
    let prefix = format!("{stripe_index:04x}/");
    let data = spawn_blocking(move || {
        let mut data = Vec::new();
        let mut iter = db.raw_iterator();
        iter.seek(&prefix);
        iter.status()?;
        while let Some(key) = iter.key() {
            let Some(postfix) = key.strip_prefix(prefix.as_bytes()) else {
                break;
            };
            let mut split = str::from_utf8(postfix)?.split('.');
            let (Some(storage_key), Some(version)) = (split.next(), split.next()) else {
                anyhow::bail!("invalid key format {:?}", str::from_utf8(key))
            };
            let storage_key = storage_key.to_string();
            if StateVersion::from_str_radix(version, 16)? <= archive_version {
                iter.seek_for_prev(format!("{prefix}{storage_key}.{archive_version:08x}"));
                iter.status()?;
                let Some((found_key, value)) = iter.item() else {
                    unimplemented!()
                };
                if !found_key.ends_with(b"+delete") {
                    data.push((storage_key.parse()?, value.to_vec()))
                }
            }
            iter.seek(format!("{prefix}{storage_key}.{:08x}", StateVersion::MAX))
        }
        anyhow::Ok(data)
    })
    .await??;
    Ok(data)
}

struct CollectWorker {
    db: Arc<DB>,
    config: ShardedStorageConfig,
    node_archived_versions: Vec<StateVersion>, // [node index -> version]
    quorum_archived_version: StateVersion,

    rx_archived: Receiver<message::Archived>,
}

impl CollectWorker {
    fn spawn(
        db: Arc<DB>,
        task: SegmentedTask,
        config: ShardedStorageConfig,
        rx_archived: Receiver<message::Archived>,
    ) -> JoinHandle<()> {
        let mut worker = Self {
            db,
            node_archived_versions: vec![0; config.num_node as usize],
            config,
            quorum_archived_version: 0,
            rx_archived,
        };
        spawn(task.wrap_fallible(async move { worker.run().await }))
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        while let Some(archived) = self.rx_archived.recv().await {
            for node_index in archived.node_indices {
                self.node_archived_versions[node_index as usize] =
                    self.node_archived_versions[node_index as usize].max(archived.version)
            }
            let mut versions = self.node_archived_versions.clone();
            versions.sort_unstable();
            let quorum_archived_version = versions[self.config.num_faulty_node as usize];
            if quorum_archived_version > self.quorum_archived_version {
                self.quorum_archived_version = quorum_archived_version;

                tracing::info!(
                    "garbage collect up to version {}",
                    self.quorum_archived_version
                );
                collect_versioned(
                    self.db.clone(),
                    self.quorum_archived_version,
                    self.config.num_stripe,
                )
                .await?
            }
        }
        Ok(())
    }
}

async fn collect_versioned(
    db: Arc<DB>,
    archived_version: StateVersion,
    num_stripe: StripeIndex,
) -> anyhow::Result<()> {
    spawn_blocking(move || {
        let mut batch = rocksdb::WriteBatch::default();
        for stripe_index in 0..num_stripe {
            let prefix = format!("{stripe_index:04x}/");
            let mut iter = db.raw_iterator();
            iter.seek(&prefix);
            iter.status()?;

            let mut prev = None;
            while let Some((key, _value)) = iter.item() {
                let Some(postfix) = key.strip_prefix(prefix.as_bytes()) else {
                    break;
                };
                let mut split = str::from_utf8(postfix)?.split('.');
                let (Some(storage_key), Some(version)) = (split.next(), split.next()) else {
                    anyhow::bail!("invalid key format {:?}", str::from_utf8(key))
                };
                let version = StateVersion::from_str_radix(version, 16)?;
                if let Some((prev_key, prev_storage_key)) = prev.take()
                    && prev_storage_key == storage_key
                    && version <= archived_version
                {
                    batch.delete(prev_key)
                }
                if version < archived_version {
                    prev = Some((key.to_vec(), storage_key.to_string()));
                    iter.next()
                } else {
                    iter.seek(format!("{prefix}{storage_key}.{:08x}", StateVersion::MAX))
                }
                iter.status()?
            }
        }
        tracing::debug!("deleting {} entries", batch.len());
        db.write(batch)?;

        //
        anyhow::Ok(())
    })
    .await??;
    Ok(())
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum Message {
    ArchivePush(message::ArchivePush),
    Archived(message::Archived),
}

#[derive(Debug, Clone)]
struct NetworkDispatcher {
    tx_message: Sender<(Dest, Message)>,
    node_table: Vec<ReplicaIndex>,
}

impl NetworkDispatcher {
    async fn send_to_all(&self, message: Message) {
        let _ = self.tx_message.send((Dest::All, message)).await;
    }
}

pub mod message {
    use std::collections::{BTreeMap, HashSet};

    use bincode::{Decode, Encode};

    use crate::replica::ReplicaIndex;

    use super::{ActiveGroupIndex, NodeIndex, StateVersion, StripeIndex};

    #[derive(Debug, Clone, Encode, Decode)]
    pub struct VoteArchive {
        pub epoch: u64,
        pub node_indices: HashSet<NodeIndex>,
    }

    #[derive(Debug, Clone, Encode, Decode)]
    pub struct Query {
        pub version: StateVersion,
        pub key: [u8; 32], // `StorageKey` does not support Encode/Decode
        pub replica_index: ReplicaIndex,
    }

    #[derive(Debug, Clone, Encode, Decode)]
    pub struct QueryOk {
        pub version: StateVersion,
        pub key: [u8; 32],
        pub value: Option<Vec<u8>>, // `Bytes` does not support Encode/Decode
    }

    #[derive(Debug, Clone, Encode, Decode)]
    pub struct ArchivePush {
        pub epoch: u64,
        pub stripe_index: StripeIndex,
        pub group_index: ActiveGroupIndex,
        pub data: BTreeMap<[u8; 32], Vec<u8>>,
    }

    #[derive(Debug, Clone, Encode, Decode)]
    pub struct Archived {
        pub version: StateVersion,
        pub node_indices: HashSet<NodeIndex>,
    }
}

mod parse {
    use crate::parse::{Configs, Extract};

    use super::ShardedStorageConfig;

    impl Extract for ShardedStorageConfig {
        fn extract(configs: &Configs) -> anyhow::Result<Self> {
            Ok(Self {
                num_node: configs.get("big.num-node")?,
                num_faulty_node: configs.get("big.num-faulty-node")?,
                num_active_copy: configs.get("big.num-active-copy")?,
                num_stripe: configs.get("big.num-stripe")?,
                bypass_vote: configs.get("big.bypass-vote")?,
            })
        }
    }
}
