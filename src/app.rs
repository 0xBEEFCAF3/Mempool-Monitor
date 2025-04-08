use std::time::{Duration, SystemTime};

use crate::{
    database::Database,
    worker::{Task, TaskContext},
    BitcoinZmqFactory,
};
use anyhow::Result;
use async_channel::{bounded, Receiver, Sender};
use bitcoind::bitcoincore_rpc::{Auth, Client, RpcApi};
use futures_util::StreamExt;
use log::info;

const NUM_WORKERS: usize = 2;

fn connect_bitcoind(bitcoind_host: &str, bitcoind_auth: Auth) -> Result<Client> {
    let bitcoind = Client::new(bitcoind_host, bitcoind_auth)?;
    Ok(bitcoind)
}

#[derive(Debug)]
pub struct App {
    zmq_factory: BitcoinZmqFactory,
    db: Database,
    tasks_tx: Sender<Task>,
    tasks_rx: Receiver<Task>,
    bitcoind_url: String,
    bitcoind_auth: Auth,
}

impl App {
    pub fn new(
        bitcoind_url: String,
        bitcoind_auth: Auth,
        zmq_factory: BitcoinZmqFactory,
        db: Database,
    ) -> Self {
        let (sender, receiver) = bounded(10_000);
        Self {
            bitcoind_url,
            bitcoind_auth,
            zmq_factory,
            db,
            tasks_tx: sender,
            tasks_rx: receiver,
        }
    }

    fn extract_existing_mempool(&self) -> Result<()> {
        let bitcoind = connect_bitcoind(&self.bitcoind_url, self.bitcoind_auth.clone())?;
        let mempool = bitcoind.get_raw_mempool_verbose()?;
        info!("Found {} transactions in mempool", mempool.len());

        for (txid, mempool_tx) in mempool.iter() {
            let pool_entrance_time = mempool_tx.time;
            let tx = bitcoind
                .get_raw_transaction_info(txid, None)?
                .transaction()?;
            let found_at = SystemTime::UNIX_EPOCH + Duration::from_secs(pool_entrance_time);
            self.db
                .insert_mempool_tx(tx, Some(found_at))?;
        }

        Ok(())
    }

    pub fn init(&mut self) -> Result<()> {
        self.extract_existing_mempool()?;
        let mut task_handles = vec![];
        for _ in 0..NUM_WORKERS {
            let bitcoind = connect_bitcoind(&self.bitcoind_url, self.bitcoind_auth.clone())?;
            let mut task_context =
                TaskContext::new(bitcoind, self.db.clone(), self.tasks_rx.clone());
            task_handles.push(tokio::spawn(async move { task_context.run().await }));
        }
        Ok(())
    }

    pub async fn run(&mut self) -> Result<()> {
        info!("===== Starting mempool tracker =====");
        let tasks_tx = self.tasks_tx.clone();
        let tasks_tx_2 = self.tasks_tx.clone();
        let mempool_state_handle = tokio::spawn(async move {
            loop {
                tasks_tx.send(Task::MempoolState).await?;
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
            #[allow(unreachable_code)]
            Ok::<(), anyhow::Error>(())
        });
        let prune_check_handle = tokio::spawn(async move {
            loop {
                tasks_tx_2.send(Task::PruneCheck).await?;
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            #[allow(unreachable_code)]
            Ok::<(), anyhow::Error>(())
        });
        let mut zmq_message_stream = self.zmq_factory.connect()?;

        let zmq_handle = {
            let tasks_tx = self.tasks_tx.clone();
            tokio::spawn(async move {
                info!("Starting zmq handle");
                while let Some(message) = zmq_message_stream.next().await {
                    match message {
                        Ok(message) => {
                            tasks_tx
                                .send(Task::RawTx(message.serialize_data_to_vec()))
                                .await?;
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
                Ok::<(), anyhow::Error>(())
            })
        };

        let _ = tokio::select! {
            r = mempool_state_handle => r?,
            r = prune_check_handle => r?,
            r = zmq_handle => r?,
        };
        Ok(())
    }
}
