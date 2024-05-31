use internment::Intern;
use serde_json::Value;

pub mod consumer;
pub mod producer;

pub type Topic = Intern<str>;
pub type Partition = i32;
pub type Key = Box<str>;
pub type Payload = Value;
pub type Offset = i64;
