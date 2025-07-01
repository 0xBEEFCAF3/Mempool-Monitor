use std::time::Duration;

use crate::{
    database::Database,
    utils::compute_fee_rate,
    worker::{get_absolute_fee, Task, TaskContext},
    zmq_factory::BitcoinZmqFactory,
};

use anyhow::Result;
use async_channel::{bounded, Receiver, Sender};
use bitcoind_async_client::{traits::Reader, Client};
use futures_util::StreamExt;
use log::{error, info};
use tokio::signal::ctrl_c;

#[derive(Debug)]
pub struct App {
    zmq_factory: BitcoinZmqFactory,
    db: Database,
    tasks_tx: Sender<Task>,
    tasks_rx: Receiver<Task>,
    rpc_client: Client,
    num_workers: usize,
    mempool_state_check_interval: Duration,
    prune_check_interval: Duration,
}

impl App {
    pub fn new(
        rpc_client: Client,
        zmq_factory: BitcoinZmqFactory,
        db: Database,
        num_workers: usize,
        mempool_state_check_interval: Duration,
        prune_check_interval: Duration,
    ) -> Self {
        let (sender, receiver) = bounded(100_000);
        Self {
            rpc_client,
            zmq_factory,
            db,
            tasks_tx: sender,
            tasks_rx: receiver,
            num_workers,
            mempool_state_check_interval,
            prune_check_interval,
        }
    }

    async fn extract_existing_mempool(&self) -> Result<()> {
        // let bitcoind = connect_bitcoind(&self.bitcoind_url, self.bitcoind_auth.clone())?;
        let mempool = self.rpc_client.get_raw_mempool_verbose().await?;
        info!("Found {} transactions in mempool", mempool.len());

        for (txid, mempool_tx) in mempool.iter() {
            let pool_entrance_time = mempool_tx.time;
            match self
                .rpc_client
                .get_raw_transaction_verbosity_zero(txid)
                .await
            {
                Ok(tx_info) => {
                    let tx = tx_info.transaction()?;
                    let absolute_fee = get_absolute_fee(&tx, &self.rpc_client).await?;
                    let fee_rate = compute_fee_rate(&tx, absolute_fee)?;
                    self.db.insert_mempool_tx(
                        tx,
                        Some(pool_entrance_time),
                        absolute_fee,
                        fee_rate,
                    )?;
                }
                Err(e) => {
                    error!("Error getting transaction info: {}", e);
                }
            }
        }

        Ok(())
    }

    pub async fn init(&mut self) -> Result<()> {
        let blockchain_info = self.rpc_client.get_blockchain_info().await?;
        info!("Blockchain info: {:?}", blockchain_info);

        if !blockchain_info.initial_block_download {
            error!("Blockchain is still in initial block download");
            return Err(anyhow::anyhow!("Blockchain is still in initial block download"));
        }

        let mempool_info = self.rpc_client.get_mempool_info().await?;
        info!("Mempool info: {:?}", mempool_info);

        if !mempool_info.loaded {
            error!("Mempool is not loaded");
            return Err(anyhow::anyhow!("Mempool is not loaded"));
        }

        info!("Initializing mempool tracker");
        // Run migrations
        info!("Running migrations");
        self.db.run_migrations()?;
        // Any txs that are neither pruned nor mined should be removed
        info!("Removing stale txs");
        self.db.remove_stale_txs()?;
        // Extract existing mempool
        info!("Extracting existing mempool");
        self.extract_existing_mempool().await?;
        // Start workers
        let mut task_handles = vec![];
        for _ in 0..self.num_workers {
            let bitcoind = self.rpc_client.clone();
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
        let tasks_tx_3 = self.tasks_tx.clone();

        let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);
        let shutdown_rx_1 = shutdown_tx.subscribe();
        let shutdown_rx_2 = shutdown_tx.subscribe();
        let shutdown_rx_3 = shutdown_tx.subscribe();

        let mempool_state_check_interval = self.mempool_state_check_interval.clone();
        let prune_check_interval = self.prune_check_interval.clone();

        let mempool_state_handle = tokio::spawn(async move {
            let mut shutdown = shutdown_rx_1;
            loop {
                tokio::select! {
                    _ = shutdown.recv() => {
                        info!("Shutting down mempool state task");
                        break;
                    }
                    _ = tokio::time::sleep(mempool_state_check_interval) => {
                        tasks_tx.send(Task::MempoolState).await?;
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        });

        let prune_check_handle = tokio::spawn(async move {
            let mut shutdown = shutdown_rx_2;
            loop {
                tokio::select! {
                    _ = shutdown.recv() => {
                        info!("Shutting down prune check task");
                        break;
                    }
                    _ = tokio::time::sleep(prune_check_interval) => {
                        tasks_tx_2.send(Task::PruneCheck).await?;
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        });

        let mut zmq_message_stream = self.zmq_factory.connect()?;
        let zmq_handle = {
            let mut shutdown = shutdown_rx_3;
            tokio::spawn(async move {
                info!("Starting zmq handle");
                loop {
                    tokio::select! {
                        _ = shutdown.recv() => {
                            info!("Shutting down zmq handle");
                            break;
                        }
                        message = zmq_message_stream.next() => {
                            match message {
                                Some(Ok(message)) => {
                                    tasks_tx_3.send(Task::RawTx(message.serialize_data_to_vec())).await?;
                                }
                                Some(Err(e)) => return Err(e.into()),
                                None => break,
                            }
                        }
                    }
                }
                Ok::<(), anyhow::Error>(())
            })
        };

        // Wait for ctrl-c
        tokio::select! {
            _ = ctrl_c() => {
                info!("Received shutdown signal");
                shutdown_tx.send(()).map_err(|e| anyhow::anyhow!("Failed to send shutdown signal: {}", e))?;
            }
            r = mempool_state_handle => r?.map_err(|e| anyhow::anyhow!("Mempool state task failed: {}", e))?,
            r = prune_check_handle => r?.map_err(|e| anyhow::anyhow!("Prune check task failed: {}", e))?,
            r = zmq_handle => r?.map_err(|e| anyhow::anyhow!("ZMQ task failed: {}", e))?,
        };

        // Clean up
        info!("Shutting down workers...");
        self.tasks_tx.close();
        self.db.flush()?;
        info!("Shutdown complete");

        Ok(())
    }
}
