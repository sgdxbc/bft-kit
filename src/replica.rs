// use tokio::sync::mpsc::{Receiver, Sender};

// use crate::network::Dest;

pub mod replay;

pub type ReplicaIndex = u16;

pub struct Request {
    pub client_id: u32,
    pub client_seq: u64,
    pub op: Vec<u8>,
}

pub struct Reply {
    pub client_seq: u64,
    pub res: Vec<u8>,
}

pub enum Message {
    Request(Request),
    Reply(Reply),
}

// pub struct Replica {
//     rx_message: Receiver<Message>,
//     tx_execute_request: Sender<Request>,
//     rx_execute_reply: Receiver<Reply>,
//     tx_message: Sender<(Dest, Message)>,
// }
