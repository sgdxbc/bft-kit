use bft_kit::{crypto::DigestHash, storage::trie::StatefulTrie};
use rand::{RngCore, rng};
use rocksdb::DB;
use tempfile::tempdir;

fn main() -> anyhow::Result<()> {
    let num_key = 1000;

    let tmp_dir = tempdir()?;
    let mut db = DB::open_default(tmp_dir.path())?;
    StatefulTrie::init(&mut db, "test")?;
    let trie = StatefulTrie::new(db.into(), "test");
    for i in 0..num_key {
        let key = format!("key-{i:04}");
        let mut value = vec![0; 68];
        rng().fill_bytes(&mut value);
        trie.insert(&key.digest(), &value.into())?;
    }

    tmp_dir.close()?;
    Ok(())
}
