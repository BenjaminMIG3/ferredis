//! Mode Cluster CP opt-in (consensus type Raft simplifié) — phase F6.
//!
//! Par défaut Ferredis reste AP (Redis Cluster). Avec `cluster-mode cp`,
//! les écritures attendent un quorum de votes avant ACK.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterConsistency {
    /// Redis Cluster classique (disponibilité).
    Ap,
    /// Quorum avant commit write.
    Cp,
}

impl ClusterConsistency {
    pub fn parse(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "cp" | "raft" => Self::Cp,
            _ => Self::Ap,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ap => "ap",
            Self::Cp => "cp",
        }
    }
}

#[derive(Debug, Clone)]
pub struct RaftPeer {
    pub id: String,
    pub addr: String,
    pub last_ack: Instant,
    pub alive: bool,
}

struct RaftInner {
    enabled: bool,
    consistency: ClusterConsistency,
    pub node_id: String,
    term: u64,
    role: RaftRole,
    peers: HashMap<String, RaftPeer>,
    commit_index: u64,
    last_log_index: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

pub struct RaftState {
    inner: Mutex<RaftInner>,
    /// Écritures en attente de quorum.
    pending_writes: AtomicU64,
    quorum_rejects: AtomicU64,
    leader: AtomicBool,
}

pub type SharedRaft = Arc<RaftState>;

pub fn shared_raft(node_id: impl Into<String>) -> SharedRaft {
    Arc::new(RaftState {
        inner: Mutex::new(RaftInner {
            enabled: false,
            consistency: ClusterConsistency::Ap,
            node_id: node_id.into(),
            term: 0,
            role: RaftRole::Follower,
            peers: HashMap::new(),
            commit_index: 0,
            last_log_index: 0,
        }),
        pending_writes: AtomicU64::new(0),
        quorum_rejects: AtomicU64::new(0),
        leader: AtomicBool::new(false),
    })
}

impl RaftState {
    pub fn set_consistency(&self, c: ClusterConsistency) {
        let mut g = self.inner.lock();
        g.consistency = c;
        g.enabled = c == ClusterConsistency::Cp;
        if g.enabled && g.role != RaftRole::Leader {
            // Bootstrap single-node leader jusqu'à ajout de peers.
            g.role = RaftRole::Leader;
            g.term = g.term.max(1);
            self.leader.store(true, Ordering::Release);
        }
    }

    pub fn consistency(&self) -> ClusterConsistency {
        self.inner.lock().consistency
    }

    pub fn is_cp(&self) -> bool {
        self.consistency() == ClusterConsistency::Cp
    }

    pub fn is_leader(&self) -> bool {
        self.leader.load(Ordering::Acquire)
    }

    pub fn add_peer(&self, id: String, addr: String) {
        self.inner.lock().peers.insert(
            id.clone(),
            RaftPeer {
                id,
                addr,
                last_ack: Instant::now(),
                alive: true,
            },
        );
    }

    pub fn peer_count(&self) -> usize {
        self.inner.lock().peers.len()
    }

    /// Quorum = majority(peers+self). En single-node, quorum = 1.
    pub fn quorum_size(&self) -> usize {
        let n = self.peer_count() + 1;
        n / 2 + 1
    }

    /// Enregistre une écriture ; en mode CP, exige le quorum local (votes simulés des peers vivants).
    pub fn commit_write(&self, timeout: Duration) -> Result<u64, &'static str> {
        if !self.is_cp() {
            let mut g = self.inner.lock();
            g.last_log_index += 1;
            g.commit_index = g.last_log_index;
            return Ok(g.commit_index);
        }
        if !self.is_leader() {
            return Err("ERR not leader — redirect to raft leader");
        }
        self.pending_writes.fetch_add(1, Ordering::Relaxed);
        let deadline = Instant::now() + timeout;
        let need = self.quorum_size();
        loop {
            let acks = {
                let g = self.inner.lock();
                1 + g.peers.values().filter(|p| p.alive).count()
            };
            if acks >= need {
                let mut g = self.inner.lock();
                g.last_log_index += 1;
                g.commit_index = g.last_log_index;
                self.pending_writes.fetch_sub(1, Ordering::Relaxed);
                return Ok(g.commit_index);
            }
            if Instant::now() >= deadline {
                self.pending_writes.fetch_sub(1, Ordering::Relaxed);
                self.quorum_rejects.fetch_add(1, Ordering::Relaxed);
                return Err("ERR raft quorum timeout");
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    pub fn mark_peer_ack(&self, id: &str) {
        if let Some(p) = self.inner.lock().peers.get_mut(id) {
            p.last_ack = Instant::now();
            p.alive = true;
        }
    }

    pub fn info_lines(&self) -> String {
        let g = self.inner.lock();
        let peers = g.peers.len();
        let quorum = (peers + 1) / 2 + 1;
        format!(
            "raft_enabled:{}\r\nraft_node_id:{}\r\ncluster_consistency:{}\r\nraft_role:{}\r\nraft_term:{}\r\nraft_commit_index:{}\r\nraft_peers:{peers}\r\nraft_quorum:{quorum}\r\nraft_pending_writes:{}\r\nraft_quorum_rejects:{}\r\n",
            g.enabled as u8,
            g.node_id,
            g.consistency.as_str(),
            match g.role {
                RaftRole::Follower => "follower",
                RaftRole::Candidate => "candidate",
                RaftRole::Leader => "leader",
            },
            g.term,
            g.commit_index,
            self.pending_writes.load(Ordering::Relaxed),
            self.quorum_rejects.load(Ordering::Relaxed),
        )
    }
}
