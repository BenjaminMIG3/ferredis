//! Client-side caching : `CLIENT TRACKING` + invalidation RESP3 push.
//!
//! Après `TRACKING ON`, les lectures enregistrent les clés ; toute mutation
//! (via [`invalidate_keys`](TrackingHub::invalidate_keys)) envoie un push
//! `>2\r\n$10\r\ninvalidate\r\n*…` aux clients concernés (ou au `REDIRECT`).

use crate::resp::RespValue;
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;

/// Options Redis `CLIENT TRACKING`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TrackingFlags {
    pub optin: bool,
    pub optout: bool,
    pub bcast: bool,
    pub noloop: bool,
}

/// État tracking d'un client.
pub struct TrackingSession {
    pub enabled: AtomicBool,
    pub flags: Mutex<TrackingFlags>,
    /// `CLIENT CACHING YES|NO` pour OPTIN/OPTOUT (None = défaut).
    pub caching: Mutex<Option<bool>>,
    /// Client id destinataire des invalidations (`REDIRECT`), -1 = self.
    pub redirect: AtomicI64,
    pub prefixes: Mutex<Vec<Bytes>>,
    /// Clés suivies (mode non-BCAST).
    keys: Mutex<HashSet<Bytes>>,
    /// Canal push vers la connexion (octets déjà encodés RESP3).
    push_tx: Mutex<Option<UnboundedSender<Bytes>>>,
}

impl Default for TrackingSession {
    fn default() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            flags: Mutex::new(TrackingFlags::default()),
            caching: Mutex::new(None),
            redirect: AtomicI64::new(-1),
            prefixes: Mutex::new(Vec::new()),
            keys: Mutex::new(HashSet::new()),
            push_tx: Mutex::new(None),
        }
    }
}

impl TrackingSession {
    pub fn attach_push(&self, tx: UnboundedSender<Bytes>) {
        *self.push_tx.lock() = Some(tx);
    }

    pub fn clear_push(&self) {
        *self.push_tx.lock() = None;
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub fn set_enabled(&self, on: bool) {
        self.enabled.store(on, Ordering::Relaxed);
        if !on {
            self.keys.lock().clear();
            *self.caching.lock() = None;
        }
    }

    pub fn redirect_id(&self) -> i64 {
        self.redirect.load(Ordering::Relaxed)
    }

    /// Enregistre les clés lues si le mode tracking l'exige.
    pub fn note_read_keys(&self, keys: &[Bytes]) {
        if !self.is_enabled() || keys.is_empty() {
            return;
        }
        let flags = *self.flags.lock();
        if flags.bcast {
            return;
        }
        let allow = match (flags.optin, flags.optout) {
            (true, _) => self.caching.lock().take() == Some(true),
            (_, true) => self.caching.lock().take() != Some(false),
            _ => true,
        };
        if !allow {
            return;
        }
        let mut set = self.keys.lock();
        for k in keys {
            set.insert(k.clone());
        }
    }

    fn matches_bcast(&self, key: &Bytes) -> bool {
        let prefixes = self.prefixes.lock();
        if prefixes.is_empty() {
            return true;
        }
        prefixes.iter().any(|p| key.starts_with(p))
    }

    fn interested_in(&self, key: &Bytes) -> bool {
        if !self.is_enabled() {
            return false;
        }
        let flags = *self.flags.lock();
        if flags.bcast {
            return self.matches_bcast(key);
        }
        self.keys.lock().contains(key)
    }

    fn send_invalidate(&self, keys: &[Bytes]) {
        let Some(tx) = self.push_tx.lock().clone() else {
            return;
        };
        let payload = invalidate_push(keys);
        let _ = tx.send(payload.to_bytes_proto(3).freeze());
    }
}

fn invalidate_push(keys: &[Bytes]) -> RespValue {
    RespValue::push(vec![
        RespValue::bulk(Bytes::from_static(b"invalidate")),
        RespValue::array(keys.iter().cloned().map(RespValue::bulk).collect()),
    ])
}

/// Hub global : sessions par client id + index inverse clé → clients.
pub struct TrackingHub {
    sessions: DashMap<u64, Arc<TrackingSession>>,
    /// Index non-BCAST : clé → clients.
    key_index: DashMap<Bytes, HashSet<u64>>,
    /// Clients en BCAST (scan à l'invalidation).
    bcast_clients: Mutex<HashSet<u64>>,
    /// io-threads prep (compteur, activation P12).
    pub io_threads_hint: AtomicU8,
}

pub type SharedTracking = Arc<TrackingHub>;

pub fn shared_tracking() -> SharedTracking {
    Arc::new(TrackingHub::new())
}

impl TrackingHub {
    pub fn new() -> Self {
        Self {
            sessions: DashMap::new(),
            key_index: DashMap::new(),
            bcast_clients: Mutex::new(HashSet::new()),
            io_threads_hint: AtomicU8::new(1),
        }
    }

    pub fn session(&self, client_id: u64) -> Arc<TrackingSession> {
        self.sessions
            .entry(client_id)
            .or_insert_with(|| Arc::new(TrackingSession::default()))
            .clone()
    }

    pub fn remove(&self, client_id: u64) {
        if let Some((_, sess)) = self.sessions.remove(&client_id) {
            sess.clear_push();
            sess.set_enabled(false);
            self.bcast_clients.lock().remove(&client_id);
            // Nettoyage best-effort de l'index (clés orphelines OK — prune paresseux).
            for mut entry in self.key_index.iter_mut() {
                entry.value_mut().remove(&client_id);
            }
        }
    }

    pub fn enable(
        &self,
        client_id: u64,
        flags: TrackingFlags,
        redirect: i64,
        prefixes: Vec<Bytes>,
    ) -> Arc<TrackingSession> {
        let sess = self.session(client_id);
        *sess.flags.lock() = flags;
        sess.redirect.store(redirect, Ordering::Relaxed);
        *sess.prefixes.lock() = prefixes;
        sess.set_enabled(true);
        if flags.bcast {
            self.bcast_clients.lock().insert(client_id);
        } else {
            self.bcast_clients.lock().remove(&client_id);
        }
        sess
    }

    pub fn disable(&self, client_id: u64) {
        if let Some(sess) = self.sessions.get(&client_id) {
            sess.set_enabled(false);
        }
        self.bcast_clients.lock().remove(&client_id);
        for mut entry in self.key_index.iter_mut() {
            entry.value_mut().remove(&client_id);
        }
    }

    /// Après une lecture : associe les clés au client.
    pub fn track_reads(&self, client_id: u64, keys: &[Bytes]) {
        let Some(sess) = self.sessions.get(&client_id).map(|e| e.clone()) else {
            return;
        };
        if !sess.is_enabled() || sess.flags.lock().bcast {
            return;
        }
        sess.note_read_keys(keys);
        let held = sess.keys.lock();
        for k in keys {
            if held.contains(k) {
                self.key_index.entry(k.clone()).or_default().insert(client_id);
            }
        }
    }

    /// Invalidation après mutation de clés.
    pub fn invalidate_keys(&self, writer_id: Option<u64>, keys: &[Bytes]) {
        if keys.is_empty() {
            return;
        }
        let mut targets: HashMap<u64, Vec<Bytes>> = HashMap::new();

        for key in keys {
            if let Some(ids) = self.key_index.get(key) {
                for &id in ids.iter() {
                    if writer_id == Some(id) {
                        if let Some(s) = self.sessions.get(&id) {
                            if s.flags.lock().noloop {
                                continue;
                            }
                        }
                    }
                    targets.entry(id).or_default().push(key.clone());
                }
            }
            for &id in self.bcast_clients.lock().iter() {
                if writer_id == Some(id) {
                    if let Some(s) = self.sessions.get(&id) {
                        if s.flags.lock().noloop {
                            continue;
                        }
                    }
                }
                if let Some(s) = self.sessions.get(&id) {
                    if s.interested_in(key) {
                        targets.entry(id).or_default().push(key.clone());
                    }
                }
            }
        }

        for (id, klist) in targets {
            let Some(sess) = self.sessions.get(&id).map(|e| e.clone()) else {
                continue;
            };
            // Retirer les clés du suivi après invalidation (Redis).
            {
                let mut held = sess.keys.lock();
                for k in &klist {
                    held.remove(k);
                    if let Some(mut set) = self.key_index.get_mut(k) {
                        set.remove(&id);
                    }
                }
            }
            let dest = match sess.redirect_id() {
                -1 => id,
                r if r >= 0 => r as u64,
                _ => id,
            };
            let deliver = if dest == id {
                sess
            } else if let Some(other) = self.sessions.get(&dest) {
                other.clone()
            } else {
                continue;
            };
            deliver.send_invalidate(&klist);
        }
    }
}

impl Default for TrackingHub {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[test]
    fn invalidate_after_read() {
        let hub = TrackingHub::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let s = hub.enable(1, TrackingFlags::default(), -1, Vec::new());
        s.attach_push(tx);
        hub.track_reads(1, &[Bytes::from_static(b"foo")]);
        hub.invalidate_keys(Some(2), &[Bytes::from_static(b"foo")]);
        let msg = rx.try_recv().expect("push");
        assert!(msg.starts_with(b">"), "{msg:?}");
        assert!(msg.windows(10).any(|w| w == b"invalidate"));
    }
}
