use std::sync::Arc;

use rocksdb::{DB, Options, WriteBatch};
use tokio_util::bytes::Bytes;

use crate::crypto::{Digest, DigestHash, UpdateHash};

use super::StorageKey;

pub struct StatefulTrie {
    db: Arc<DB>,
    cf_name: String,
}

// the proof of a _path_ presents under a certain root. the path may leads to either
// * a leaf node with matching key, this is the only case of a proof of presence
// * a branch or leaf node with mismatching prefix to the key
// * a missing branch

// in any case, the proof is only valid if
// * all nodes feature an expected prefix, specified by the presenting _something_
// * all nodes contain a correct branch digest at a certain index. the correct digest is the digest
// of the following node, or the digest of what this path leads to; it should be None if the path
// leads to a missing branch
// * the digest of the first node matches the given root
pub struct Proof(Vec<ProofNode>);

struct ProofNode {
    prefix: Vec<u8>,
    branch_digests: [Option<Digest>; 16],
}

#[allow(unused)]
impl Proof {
    fn branch_index(prefix: &[u8], path: &[u8]) -> anyhow::Result<usize> {
        let Some(nibble) = path.strip_prefix(prefix).and_then(|p| p.get(0)) else {
            anyhow::bail!("path does not extend the prefix");
        };
        match nibble {
            b'0'..=b'9' => Ok((nibble - b'0') as _),
            b'a'..=b'f' => Ok((nibble - b'a' + 10) as _),
            _ => Err(anyhow::anyhow!("invalid nibble")),
        }
    }

    fn verify(
        &self,
        root: &Digest,
        target_path: &[u8],
        target_digest: Option<Digest>,
    ) -> anyhow::Result<()> {
        let mut path = target_path;
        let mut digest = target_digest;
        for node in self.0.iter().rev() {
            let index = Self::branch_index(&node.prefix, &path)?;
            anyhow::ensure!(node.branch_digests[index] == digest);
            path = &node.prefix;
            digest = Some(node.digest())
        }
        anyhow::ensure!(digest.as_ref() == Some(root));
        Ok(())
    }

    fn verify_presence(
        &self,
        root: &Digest,
        key: &StorageKey,
        value: &Bytes,
    ) -> anyhow::Result<()> {
        let key = key.to_hex();
        let target_path = key.as_bytes();
        self.verify(root, target_path, Some((target_path, value).digest()))
    }

    fn verify_missing_branch(&self, root: &Digest, key: &StorageKey) -> anyhow::Result<()> {
        let key = key.to_hex();
        let target_path = key.as_bytes();
        self.verify(root, target_path, None)
    }

    fn verify_mismatch_branch(
        &self,
        root: &Digest,
        key: &StorageKey,
        node: &ProofNode,
    ) -> anyhow::Result<()> {
        let key = key.to_hex();
        let target_path = key.as_bytes();
        anyhow::ensure!(
            !target_path.starts_with(&*node.prefix),
            "the given node's prefix matches the key"
        );
        self.verify(root, target_path, Some(node.digest()))
    }

    fn verify_mismatch_leaf(
        &self,
        root: &Digest,
        key: &StorageKey,
        leaf_key: &StorageKey,
        leaf_value: &Bytes,
    ) -> anyhow::Result<()> {
        let key = key.to_hex();
        let target_path = key.as_bytes();
        let leaf_key = leaf_key.to_hex();
        let leaf_path = leaf_key.as_bytes();
        anyhow::ensure!(
            target_path != leaf_path,
            "the given leaf's key matches the target key"
        );
        self.verify(root, target_path, Some((leaf_path, leaf_value).digest()))
    }

    fn update(
        &mut self,
        target_path: &[u8],
        target_digest: Option<Digest>,
    ) -> anyhow::Result<Digest> {
        let mut path = target_path;
        let mut digest = target_digest;
        for node in self.0.iter_mut().rev() {
            let index = Self::branch_index(&node.prefix, &path)?;
            node.branch_digests[index] = digest;
            path = &node.prefix;
            digest = Some(node.digest())
        }
        digest.ok_or_else(|| anyhow::anyhow!("empty proof"))
    }

    fn update_leaf(&mut self, key: &StorageKey, value: Option<&Bytes>) -> anyhow::Result<Digest> {
        let key = key.to_hex();
        let target_path = key.as_bytes();
        let target_digest = value.map(|value| (target_path, value).digest());
        self.update(target_path, target_digest)
    }
}

pub enum ProofOfAbsence {
    MissingBranch(Proof),
    MismatchBranch(ProofNode, Proof),
    MismatchLeaf(StorageKey, Bytes, Proof),
}

pub enum Get {
    Presence(Bytes, Proof),
    Absence(ProofOfAbsence),
}

impl StatefulTrie {
    pub fn new(db: Arc<DB>, cf_name: impl ToString) -> Self {
        Self {
            db,
            cf_name: cf_name.to_string(),
        }
    }

    pub fn init(db: &mut DB, cf_name: impl ToString) -> anyhow::Result<()> {
        db.create_cf(&cf_name.to_string(), &Options::default())?;
        let Some(cf) = db.cf_handle(&cf_name.to_string()) else {
            anyhow::bail!("column family {} not found", cf_name.to_string())
        };
        let node = ProofNode {
            prefix: b"".to_vec(),
            branch_digests: [(); 16].map(|()| None),
        };
        let data = bincode::encode_to_vec(node.branch_digests, bincode::config::standard())?;
        db.put_cf(cf, node.prefix, data)?;
        Ok(())
    }

    pub fn get(&self, key: &StorageKey) -> anyhow::Result<Get> {
        let Some(cf) = self.db.cf_handle(&self.cf_name) else {
            anyhow::bail!("column family {} not found", self.cf_name)
        };
        let mut iter = self.db.raw_iterator_cf(cf);
        iter.seek_to_first();
        iter.status()?;
        let target_key = key.to_hex().into_bytes();
        let mut proof_nodes = Vec::new();
        while let Some((found_key, found_value)) = iter.item() {
            if found_key == target_key {
                return Ok(Get::Presence(
                    Bytes::copy_from_slice(found_value),
                    Proof(proof_nodes),
                ));
            }
            if found_key.len() == target_key.len() {
                return Ok(Get::Absence(ProofOfAbsence::MismatchLeaf(
                    StorageKey::from_hex(str::from_utf8(found_key)?)?,
                    Bytes::copy_from_slice(found_value),
                    Proof(proof_nodes),
                )));
            }
            let (branch_digests, len) =
                bincode::decode_from_slice(found_value, bincode::config::standard())?;
            anyhow::ensure!(len == found_value.len());
            let node = ProofNode {
                prefix: found_key.to_vec(),
                branch_digests,
            };
            if !target_key.starts_with(found_key) {
                return Ok(Get::Absence(ProofOfAbsence::MismatchBranch(
                    node,
                    Proof(proof_nodes),
                )));
            }
            proof_nodes.push(node);
            iter.seek(&target_key[..found_key.len() + 1]);
            iter.status()?
        }
        Ok(Get::Absence(ProofOfAbsence::MissingBranch(Proof(
            proof_nodes,
        ))))
    }

    pub fn insert(&self, key: &StorageKey, value: &Bytes) -> anyhow::Result<()> {
        match self.get(key)? {
            Get::Presence(_, mut proof)
            | Get::Absence(ProofOfAbsence::MissingBranch(mut proof)) => {
                proof.update_leaf(key, Some(value))?;
                let Some(cf) = self.db.cf_handle(&self.cf_name) else {
                    anyhow::bail!("column family {} not found", self.cf_name)
                };
                let mut batch = WriteBatch::new();
                for node in proof.0 {
                    let data =
                        bincode::encode_to_vec(node.branch_digests, bincode::config::standard())?;
                    batch.put_cf(cf, node.prefix, data);
                }
                batch.put_cf(cf, key.to_hex(), value);
                self.db.write(batch)?
            }
            #[allow(unused)]
            Get::Absence(ProofOfAbsence::MismatchBranch(node, mut proof)) => todo!(),
            #[allow(unused)]
            Get::Absence(ProofOfAbsence::MismatchLeaf(leaf_key, leaf_value, mut proof)) => todo!(),
        }
        Ok(())
    }

    pub fn remove(&self, key: &StorageKey) -> anyhow::Result<()> {
        Ok(())
    }
}

// pub struct StatelessTrie {
//     root: Digest,
// }

// impl StatelessTrie {
//     pub fn verify(
//         &self,
//         key: &StorageKey,
//         value: &Option<Bytes>,
//         proof: &Proof,
//     ) -> anyhow::Result<()> {
//         Ok(())
//     }

//     pub fn update(
//         &mut self,
//         key: &StorageKey,
//         value: &Option<Bytes>,
//         proof: &Proof,
//         new_value: &Option<Bytes>,
//     ) -> anyhow::Result<()> {
//         Ok(())
//     }
// }

impl UpdateHash for (&'_ [u8], &'_ Bytes) {
    fn update<D: sha2::Digest>(&self, state: &mut D) {
        state.update(self.0);
        state.update(self.1)
    }
}

impl UpdateHash for ProofNode {
    fn update<D: sha2::Digest>(&self, state: &mut D) {
        state.update(&self.prefix);
        for digest in &self.branch_digests {
            match digest {
                Some(digest) => state.update(digest),
                None => state.update([0u8]),
            }
        }
    }
}
