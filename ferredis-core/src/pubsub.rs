//! Pub/Sub hub compatible with Redis semantics (canaux, motifs et canaux « shard »).

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

pub type PubSubMsg = (Bytes, Bytes); // (channel, payload)

/// Famille d'abonnement : SUBSCRIBE, PSUBSCRIBE ou SSUBSCRIBE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubKind {
    Channel = 0,
    Pattern = 1,
    Shard = 2,
}

impl SubKind {
    pub fn subscribe_verb(self) -> &'static str {
        match self {
            Self::Channel => "subscribe",
            Self::Pattern => "psubscribe",
            Self::Shard => "ssubscribe",
        }
    }

    pub fn unsubscribe_verb(self) -> &'static str {
        match self {
            Self::Channel => "unsubscribe",
            Self::Pattern => "punsubscribe",
            Self::Shard => "sunsubscribe",
        }
    }
}

#[derive(Debug)]
struct ClientSubs {
    subs: [HashSet<Bytes>; 3],
    tx: mpsc::UnboundedSender<PubSubEvent>,
}

impl ClientSubs {
    /// Compteur renvoyé par Redis : canaux + motifs, ou canaux shard seuls.
    fn count(&self, kind: SubKind) -> i64 {
        match kind {
            SubKind::Shard => self.subs[SubKind::Shard as usize].len() as i64,
            _ => (self.subs[SubKind::Channel as usize].len() + self.subs[SubKind::Pattern as usize].len())
                as i64,
        }
    }
}

#[derive(Debug, Clone)]
pub enum PubSubEvent {
    Message {
        channel: Bytes,
        payload: Bytes,
    },
    PMessage {
        pattern: Bytes,
        channel: Bytes,
        payload: Bytes,
    },
    SMessage {
        channel: Bytes,
        payload: Bytes,
    },
}

#[derive(Debug)]
pub struct PubSubHub {
    next_id: AtomicU64,
    clients: DashMap<u64, Mutex<ClientSubs>>,
    indexes: [DashMap<Bytes, HashSet<u64>>; 3],
}

impl Default for PubSubHub {
    fn default() -> Self {
        Self::new()
    }
}

impl PubSubHub {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            clients: DashMap::new(),
            indexes: [DashMap::new(), DashMap::new(), DashMap::new()],
        }
    }

    pub fn register(&self) -> (u64, mpsc::UnboundedReceiver<PubSubEvent>) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::unbounded_channel();
        self.clients.insert(
            id,
            Mutex::new(ClientSubs {
                subs: Default::default(),
                tx,
            }),
        );
        (id, rx)
    }

    pub fn unregister(&self, id: u64) {
        if let Some((_, client)) = self.clients.remove(&id) {
            let client = client.into_inner();
            for (k, set) in client.subs.into_iter().enumerate() {
                for name in set {
                    self.drop_member(k, &name, id);
                }
            }
        }
    }

    pub fn subscribe(&self, kind: SubKind, id: u64, name: Bytes) -> i64 {
        let Some(client) = self.clients.get(&id) else {
            return 0;
        };
        let mut c = client.lock();
        if c.subs[kind as usize].insert(name.clone()) {
            self.indexes[kind as usize].entry(name).or_default().insert(id);
        }
        c.count(kind)
    }

    /// Désabonne `name` (ou tout si `None`) ; une entrée `(nom, compteur)` par désabonnement.
    pub fn unsubscribe(&self, kind: SubKind, id: u64, name: Option<Bytes>) -> Vec<(Bytes, i64)> {
        let Some(client) = self.clients.get(&id) else {
            return Vec::new();
        };
        let mut c = client.lock();
        let names: Vec<Bytes> = match name {
            Some(n) => vec![n],
            None => c.subs[kind as usize].iter().cloned().collect(),
        };
        let mut out = Vec::with_capacity(names.len());
        for n in names {
            if c.subs[kind as usize].remove(&n) {
                self.drop_member(kind as usize, &n, id);
            }
            out.push((n, c.count(kind)));
        }
        out
    }

    fn send(&self, id: u64, event: PubSubEvent) -> bool {
        self.clients
            .get(&id)
            .is_some_and(|c| c.lock().tx.send(event).is_ok())
    }

    /// Abonnés directs d'un canal, copiés pour relâcher l'index avant de verrouiller les clients
    /// (ordre de verrouillage inverse de `subscribe` : pas d'interblocage).
    fn direct_subscribers(&self, kind: SubKind, channel: &Bytes) -> Vec<u64> {
        self.indexes[kind as usize]
            .get(channel)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default()
    }

    /// PUBLISH : chaque abonné direct et chaque correspondance de motif compte une livraison.
    pub fn publish(&self, channel: &Bytes, payload: Bytes) -> i64 {
        let direct = self.direct_subscribers(SubKind::Channel, channel);
        let by_pattern: Vec<(Bytes, u64)> = self.indexes[SubKind::Pattern as usize]
            .iter()
            .filter(|e| crate::store::match_glob_bytes(e.key(), channel))
            .flat_map(|e| e.value().iter().map(|&id| (e.key().clone(), id)).collect::<Vec<_>>())
            .collect();
        let mut delivered = 0i64;
        for id in direct {
            let ev = PubSubEvent::Message { channel: channel.clone(), payload: payload.clone() };
            delivered += self.send(id, ev) as i64;
        }
        for (pattern, id) in by_pattern {
            let ev = PubSubEvent::PMessage { pattern, channel: channel.clone(), payload: payload.clone() };
            delivered += self.send(id, ev) as i64;
        }
        delivered
    }

    /// SPUBLISH : uniquement les abonnés SSUBSCRIBE du canal.
    pub fn spublish(&self, channel: &Bytes, payload: Bytes) -> i64 {
        self.direct_subscribers(SubKind::Shard, channel)
            .into_iter()
            .map(|id| {
                let ev = PubSubEvent::SMessage { channel: channel.clone(), payload: payload.clone() };
                self.send(id, ev) as i64
            })
            .sum()
    }

    /// Retire `id` de l'index et supprime l'entrée devenue vide (pas de fuite mémoire).
    fn drop_member(&self, kind: usize, name: &Bytes, id: u64) {
        if let Some(mut set) = self.indexes[kind].get_mut(name) {
            set.remove(&id);
        }
        self.indexes[kind].remove_if(name, |_, s| s.is_empty());
    }

    /// `(canaux, motifs, canaux shard)` auxquels le client est abonné.
    pub fn counts(&self, id: u64) -> (usize, usize, usize) {
        self.clients
            .get(&id)
            .map(|c| {
                let g = c.lock();
                (g.subs[0].len(), g.subs[1].len(), g.subs[2].len())
            })
            .unwrap_or((0, 0, 0))
    }

    /// Canaux actifs filtrés par motif glob (PUBSUB CHANNELS / SHARDCHANNELS).
    pub fn active(&self, kind: SubKind, pattern: Option<&[u8]>) -> Vec<Bytes> {
        let mut out: Vec<Bytes> = self.indexes[kind as usize]
            .iter()
            .filter(|e| pattern.map_or(true, |p| crate::store::match_glob_bytes(p, e.key())))
            .map(|e| e.key().clone())
            .collect();
        out.sort_unstable();
        out
    }

    /// Nombre d'abonnés d'un canal (PUBSUB NUMSUB / SHARDNUMSUB).
    pub fn numsub(&self, kind: SubKind, channel: &Bytes) -> usize {
        self.indexes[kind as usize].get(channel).map_or(0, |s| s.len())
    }

    /// Nombre de canaux (ou motifs) distincts ayant au moins un abonné.
    pub fn count(&self, kind: SubKind) -> usize {
        self.indexes[kind as usize].len()
    }

    pub fn is_subscribed(&self, id: u64) -> bool {
        self.clients
            .get(&id)
            .is_some_and(|c| c.lock().subs.iter().any(|s| !s.is_empty()))
    }
}

pub type SharedPubSub = Arc<PubSubHub>;

pub fn shared_pubsub() -> SharedPubSub {
    Arc::new(PubSubHub::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn publish_counts_channel_and_each_pattern() {
        let hub = PubSubHub::new();
        let (id, mut rx) = hub.register();
        assert_eq!(hub.subscribe(SubKind::Channel, id, b("news")), 1);
        assert_eq!(hub.subscribe(SubKind::Pattern, id, b("n*")), 2);
        assert_eq!(hub.subscribe(SubKind::Pattern, id, b("*s")), 3);
        assert_eq!(hub.publish(&b("news"), b("hi")), 3);
        let mut kinds = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            kinds.push(matches!(ev, PubSubEvent::Message { .. }));
        }
        assert_eq!(kinds.iter().filter(|m| **m).count(), 1);
        assert_eq!(kinds.len(), 3);
    }

    #[test]
    fn shard_channels_are_isolated() {
        let hub = PubSubHub::new();
        let (id, mut rx) = hub.register();
        assert_eq!(hub.subscribe(SubKind::Channel, id, b("a")), 1);
        assert_eq!(hub.subscribe(SubKind::Shard, id, b("a")), 1);
        assert_eq!(hub.subscribe(SubKind::Shard, id, b("b")), 2);
        assert_eq!(hub.spublish(&b("a"), b("x")), 1);
        assert!(matches!(rx.try_recv(), Ok(PubSubEvent::SMessage { .. })));
        assert_eq!(hub.active(SubKind::Shard, None), vec![b("a"), b("b")]);
        assert_eq!(hub.numsub(SubKind::Shard, &b("b")), 1);
        let out = hub.unsubscribe(SubKind::Shard, id, None);
        assert_eq!(out.len(), 2);
        assert_eq!(out.last().map(|o| o.1), Some(0));
        assert_eq!(hub.count(SubKind::Shard), 0);
        assert!(hub.is_subscribed(id));
        hub.unregister(id);
        assert_eq!(hub.count(SubKind::Channel), 0);
    }
}
