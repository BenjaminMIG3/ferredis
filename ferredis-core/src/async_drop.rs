//! Libération asynchrone de clés (UNLINK, FLUSH ASYNC).

use crate::store::SharedDb;
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct AsyncDropQueue {
    tx: mpsc::UnboundedSender<DropJob>,
}

enum DropJob {
    Unlink { db: usize, keys: Vec<Bytes> },
    FlushDb { db: usize },
    FlushAll,
}

impl AsyncDropQueue {
    pub fn spawn(store: SharedDb) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(job) = rx.recv().await {
                match job {
                    DropJob::Unlink { db, keys } => {
                        let _ = store.del(db, &keys);
                    }
                    DropJob::FlushDb { db } => store.flushdb(db),
                    DropJob::FlushAll => store.flushall(),
                }
            }
        });
        Self { tx }
    }

    pub fn unlink(&self, db: usize, keys: Vec<Bytes>) {
        let _ = self.tx.send(DropJob::Unlink { db, keys });
    }

    pub fn flushdb(&self, db: usize) {
        let _ = self.tx.send(DropJob::FlushDb { db });
    }

    pub fn flushall(&self) {
        let _ = self.tx.send(DropJob::FlushAll);
    }
}

pub type SharedAsyncDrop = Arc<AsyncDropQueue>;

pub fn shared_async_drop(db: SharedDb) -> SharedAsyncDrop {
    Arc::new(AsyncDropQueue::spawn(db))
}
