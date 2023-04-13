use std::{collections::HashMap, sync::{Arc}};

use tokio::sync::Mutex;

use bytes::Bytes;

pub type SharedDb = Arc<Mutex<HashMap<String, (Bytes, Option<u128>)>>>;
