use rocksdb::{DB, Error};

use super::{
    AppProtocol, AppState,
    ycsb::{YcsbOp, YcsbRes},
};

pub struct Rocksdb(pub DB);

impl AppProtocol for Rocksdb {
    type Op = YcsbOp;
    type Res = YcsbRes;
}

impl AppState for Rocksdb {
    fn execute(&mut self, op: Self::Op) -> Self::Res {
        Self::execute(self, op).unwrap_or_else(|err| YcsbRes::Err(err.to_string()))
    }
}

impl Rocksdb {
    fn execute(&mut self, op: YcsbOp) -> Result<YcsbRes, Error> {
        let res = match op {
            YcsbOp::Insert(key, value) | YcsbOp::Update(key, value) => {
                self.0.put(key, value)?;
                YcsbRes::Ok
            }
            YcsbOp::Get(key) => match self.0.get(key)? {
                Some(value) => YcsbRes::Get(String::from_utf8(value).unwrap_or_default()),
                None => YcsbRes::NotFound,
            },
            YcsbOp::Scan(prefix, limit) => {
                let values = self
                    .0
                    .prefix_iterator(prefix)
                    .take(limit)
                    .map(|result| {
                        let (k, v) = result?;
                        Ok((
                            String::from_utf8(k.to_vec()).unwrap_or_default(),
                            String::from_utf8(v.to_vec()).unwrap_or_default(),
                        ))
                    })
                    .collect::<Result<_, Error>>()?;
                YcsbRes::Scan(values)
            }
        };
        Ok(res)
    }
}
