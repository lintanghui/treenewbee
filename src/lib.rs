#[macro_use]
extern crate log;
#[macro_use] extern crate failure;

mod protocol;
mod com;

// use self::protocol::rdb::RDBParser;

use serde;
use serde_derive::{Deserialize, Serialize};
use toml;
use env_logger;

use std::io;

#[derive(Serialize, Deserialize)]
pub(crate) struct Config {
    pub(crate) source: Endpoints,
    pub(crate) target: Endpoints,
    pub(crate) worker: WorkerConfig,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct WorkerConfig {
    pub(crate) thread: usize,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Endpoints {
    pub(crate) servers: Vec<String>,
    pub(crate) kind: Option<String>,
    pub(crate) hash: Option<String>,
    pub(crate) hash_tag: Option<String>,
}

fn load_config() -> Config {
    use std::env;
    let path = env::var("BEE_CFG").unwrap_or_else(|_| "bee.toml".to_string());
    use std::fs;
    use std::io::{BufReader, Read};

    let fd = fs::File::open(&path).expect("fail to open config file(default: bee.toml)");
    let mut rd = BufReader::new(fd);
    let mut data = String::new();
    rd.read_to_string(&mut data)
        .expect("fail to read config file");

    toml::from_str(&data).expect("fail to parse toml")
}

pub fn run() -> Result<(), io::Error> {
    env_logger::init();
    info!("tree-new-bee is starting");
    let config = load_config();
    Ok(())
}
