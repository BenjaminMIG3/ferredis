//! Suivi mémoire, compression et défragmentation active (phase F1).

mod compress;
mod defrag;
mod tracker;

pub use compress::{compress_string_bytes, decompress_string_bytes, is_compressed_payload};
pub use defrag::{active_defrag_pass, DefragStats};
pub use tracker::{memory_tracker, MemoryTracker, SharedMemoryTracker};
