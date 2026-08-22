//! Async JSONL writer for wallet_probe events.
//!
//! Backed by an `mpsc::UnboundedSender`; a background task consumes messages
//! and appends them to a file, flushing on each write.

use crate::wallet_probe::types::WalletProbeEvent;
use std::path::{Path, PathBuf};
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
pub struct JsonlWriter {
    tx: mpsc::UnboundedSender<WalletProbeEvent>,
}

impl JsonlWriter {
    /// Open (or create) the JSONL file at `path` and spawn a background writer task.
    ///
    /// The returned handle can be cloned freely; drop all clones to close the writer.
    pub async fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path: PathBuf = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                tokio::fs::create_dir_all(parent).await?;
            }
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;
        info!("wallet_probe JSONL writer opened at {}", path.display());

        let (tx, rx) = mpsc::unbounded_channel::<WalletProbeEvent>();
        tokio::spawn(writer_task(file, rx, path));

        Ok(Self { tx })
    }

    /// Enqueue an event for asynchronous writing. Never blocks.
    pub fn write(&self, event: WalletProbeEvent) {
        if let Err(e) = self.tx.send(event) {
            warn!("wallet_probe writer channel closed: {}", e);
        }
    }
}

async fn writer_task(
    mut file: File,
    mut rx: mpsc::UnboundedReceiver<WalletProbeEvent>,
    path: PathBuf,
) {
    while let Some(event) = rx.recv().await {
        let mut line = match serde_json::to_string(&event) {
            Ok(s) => s,
            Err(e) => {
                error!("wallet_probe serialize error: {}", e);
                continue;
            }
        };
        line.push('\n');
        if let Err(e) = file.write_all(line.as_bytes()).await {
            error!("wallet_probe write error on {}: {}", path.display(), e);
            continue;
        }
        if let Err(e) = file.flush().await {
            error!("wallet_probe flush error on {}: {}", path.display(), e);
        }
    }
    info!("wallet_probe writer task exiting (channel closed)");
}
