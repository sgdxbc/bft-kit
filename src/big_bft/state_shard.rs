use std::{
    collections::{HashMap, HashSet},
    mem::take,
};

use bincode::{Decode, Encode};
use sha2::{Digest, Sha256};

use super::DigestHash;

#[derive(Debug, Clone, Default, Encode, Decode)]
pub struct StateShard {
    pub store: HashMap<DigestHash, String>,
    //
}

pub struct StateShardProof {
    siblings: Vec<(bool, DigestHash)>, // (prepend?, sibling shard's digest hash)
}

pub enum StateShardProofSibling {
    Left(DigestHash),
    Right(DigestHash),
}

impl StateShard {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn digest_hash(&self) -> DigestHash {
        let mut state = Sha256::new();
        for (key, value) in &self.store {
            state.update(key);
            state.update(value)
        }
        state.finalize().into()
    }
}

impl StateShardProof {
    pub fn verify(&self, root: &DigestHash, state_share: &StateShard) -> bool {
        let mut digest_hash = state_share.digest_hash();
        for &(prepend, sibling) in &self.siblings {
            digest_hash = Sha256::digest(
                if prepend {
                    [sibling, digest_hash]
                } else {
                    [digest_hash, sibling]
                }
                .concat(),
            )
            .into()
        }
        &digest_hash == root
    }
}

pub struct StateShardDigestHashes {
    shards: Vec<DigestHash>,
    dirty_indexes: HashSet<usize>,
    inner_nodes: Vec<Vec<DigestHash>>,
}

impl StateShardDigestHashes {
    pub fn new(num_shard: usize) -> Self {
        assert!(num_shard > 1);
        assert!(num_shard.is_power_of_two());
        let shard_hash = StateShard::default().digest_hash();
        let mut inner_nodes = Vec::new();
        let mut num_node = num_shard / 2;
        while num_node > 0 {
            inner_nodes.push(vec![Default::default(); num_node]);
            num_node /= 2
        }
        Self {
            shards: vec![shard_hash; num_shard],
            dirty_indexes: (0..num_shard).collect(),
            inner_nodes,
        }
    }

    pub fn update(&mut self, shard_index: usize, shard: &StateShard) {
        self.shards[shard_index] = shard.digest_hash();
        self.dirty_indexes.insert(shard_index);
    }

    pub fn refresh(&mut self) {
        let mut dirty_indexes = take(&mut self.dirty_indexes);
        for level in 0..self.inner_nodes.len() {
            dirty_indexes = dirty_indexes.into_iter().map(|index| index / 2).collect();
            for &index in &dirty_indexes {
                let children_nodes = if level == 0 {
                    &self.shards
                } else {
                    &self.inner_nodes[level - 1]
                };
                self.inner_nodes[level][index] = Sha256::digest(
                    [children_nodes[index * 2], children_nodes[index * 2 + 1]].concat(),
                )
                .into()
            }
        }
    }

    fn is_clean(&self) -> bool {
        self.dirty_indexes.is_empty()
    }

    pub fn prove(&self, shard_index: usize) -> StateShardProof {
        assert!(self.is_clean());
        let mut siblings = Vec::new();
        let mut index = shard_index;
        for level_nodes in [&self.shards]
            .into_iter()
            .chain(&self.inner_nodes)
            // maybe a little bit too elegant :)
            .take(self.inner_nodes.len())
        {
            siblings.push((index % 2 != 0, level_nodes[index ^ 1]));
            index /= 2
        }
        StateShardProof { siblings }
    }

    pub fn root(&self) -> DigestHash {
        assert!(self.is_clean());
        // learned from std's Vec::last
        if let [.., root] = &self.inner_nodes[..] {
            root[0]
        } else {
            unreachable!()
        }
    }

    pub fn verify(&self, shard_index: usize, shard: &StateShard) -> bool {
        shard.digest_hash() == self.shards[shard_index]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_shards_valid() {
        let mut shard_hashes = StateShardDigestHashes::new(1 << 4);
        shard_hashes.refresh();
        let root = shard_hashes.root();
        for index in 0..1 << 4 {
            println!("working on index {index}");
            let proof = shard_hashes.prove(index);
            let valid = proof.verify(&root, &Default::default());
            assert!(valid)
        }
    }

    #[test]
    fn update_single_shard() {
        for update_index in 0..1 << 4 {
            let mut shard_hashes = StateShardDigestHashes::new(1 << 4);
            let mut shard = StateShard::new();
            let k = format!("key{update_index}").into_bytes();
            let mut key = DigestHash::default();
            key[..k.len()].copy_from_slice(&k);
            shard.store.insert(key, format!("value{update_index}"));
            shard_hashes.update(update_index, &shard);
            shard_hashes.refresh();
            let root = shard_hashes.root();

            for index in 0..1 << 4 {
                let proof = shard_hashes.prove(index);
                let shard = if index == update_index {
                    &shard
                } else {
                    &Default::default()
                };
                let valid = proof.verify(&root, shard);
                assert!(valid)
            }
        }
    }
}
