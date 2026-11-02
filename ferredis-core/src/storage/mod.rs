//! Storage hybride hot / warm / cold (phase F4).

mod cold;
mod engine;

pub use cold::ColdStore;
pub use engine::{StorageEngine, StorageMode, StorageStats, SharedStorage};
