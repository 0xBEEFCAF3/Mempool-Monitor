use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use crate::{database::Database, worker::TaskContext};
use anyhow::Result;
use async_channel::{bounded, Receiver, Sender};
use bitcoincore_zmq::MessageStream;
use bitcoind::bitcoincore_rpc::{Auth, Client, RpcApi};
use futures_util::StreamExt;
use log::info;
use tokio::sync::RwLock;

const NUM_WORKERS: usize = 2;

fn connect_bitcoind(bitcoind_host: &str, bitcoind_auth: Auth) -> Result<Client> {
    let bitcoind = Client::new(bitcoind_host, bitcoind_auth)?;
    Ok(bitcoind)
}

pub struct App {
    zmq: MessageStream,
    db: Database,
    lock: Arc<RwLock<()>>,
    raw_txs_tx: Sender<Vec<u8>>,
    raw_txs_rx: Receiver<Vec<u8>>,
    bitcoind_url: String,
    bitcoind_auth: Auth,
}

impl App {
    pub fn new(
        bitcoind_url: String,
        bitcoind_auth: Auth,
        zmq: MessageStream,
        db: Database,
    ) -> Self {
        let (sender, receiver) = bounded(10_000);
        Self {
            bitcoind_url,
            bitcoind_auth,
            zmq,
            db,
            lock: Arc::new(RwLock::new(())),
            raw_txs_tx: sender,
            raw_txs_rx: receiver,
        }
    }

    fn extract_existing_mempool(&self) -> Result<()> {
        let bitcoind = connect_bitcoind(&self.bitcoind_url, self.bitcoind_auth.clone())?;
        let mempool = bitcoind.get_raw_mempool_verbose()?;
        info!("Found {} transactions in mempool", mempool.len());
        let mempool_info = bitcoind.get_mempool_info()?;
        let mempool_size = mempool_info.bytes;
        let tx_count = mempool_info.size;

        for (txid, mempool_tx) in mempool.iter() {
            let pool_entrance_time = mempool_tx.time;
            let tx = bitcoind.get_transaction(txid, None)?.transaction()?;
            let found_at = SystemTime::UNIX_EPOCH + Duration::from_secs(pool_entrance_time);
            self.db
                .insert_mempool_tx(tx, Some(found_at), mempool_size as u64, tx_count as u64)?;
        }

        Ok(())
    }

    pub fn init(&mut self) -> Result<()> {
        self.extract_existing_mempool()?;
        let mut task_handles = vec![];
        for _ in 0..NUM_WORKERS {
            let bitcoind = connect_bitcoind(&self.bitcoind_url, self.bitcoind_auth.clone())?;
            let mut task_context = TaskContext::new(
                bitcoind,
                self.db.clone(),
                self.lock.clone(),
                self.raw_txs_rx.clone(),
            );
            task_handles.push(tokio::spawn(async move { task_context.run().await }));
        }
        Ok(())
    }

    pub async fn run(&mut self) -> Result<()> {
        info!("===== Starting mempool tracker =====");
        while let Some(message) = self.zmq.next().await {
            match message {
                Ok(message) => {
                    self.raw_txs_tx
                        .send(message.serialize_data_to_vec())
                        .await?;
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }

        Ok(())
    }
}
