//! Contrôle serveur (SHUTDOWN, etc.).

use parking_lot::Mutex;
use std::sync::Arc;
use tokio::sync::watch;

#[derive(Debug, Clone, Default)]
pub struct ShutdownRequest {
    pub nosave: bool,
    pub save: bool,
    pub now: bool,
    pub force: bool,
    pub abort: bool,
}

#[derive(Debug)]
pub struct ServerControl {
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
    pending: Mutex<Option<ShutdownRequest>>,
}

impl ServerControl {
    pub fn new() -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Self {
            shutdown_tx,
            shutdown_rx,
            pending: Mutex::new(None),
        }
    }

    pub fn subscribe_shutdown(&self) -> watch::Receiver<bool> {
        self.shutdown_rx.clone()
    }

    pub fn request_shutdown(&self, req: ShutdownRequest) {
        if req.abort {
            *self.pending.lock() = None;
            return;
        }
        *self.pending.lock() = Some(req);
        let _ = self.shutdown_tx.send(true);
    }

    pub fn take_shutdown(&self) -> Option<ShutdownRequest> {
        self.pending.lock().take()
    }
}

pub type SharedControl = Arc<ServerControl>;

pub fn shared_control() -> SharedControl {
    Arc::new(ServerControl::new())
}
