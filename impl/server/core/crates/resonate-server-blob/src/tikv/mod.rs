//! The blob server on TiKV (feature `tikv`): a [`Store`](crate::store::Store)
//! over TiKV's transactional API, and the `resonate-server-tikv` plugin that
//! runs the blob server on it.
//!
//! TiKV gives the one thing the blob server needs from a bucket, a
//! conditional write per origin document, without an object store and
//! without a SQL layer: a pessimistic transaction that reads a key, checks
//! its revision and writes it commits atomically or not at all. API v2
//! keyspaces and a root prefix keep deployments apart.
//!
//! The on-disk layout is the one Loams 0.1.0 wrote (see [`store`]), so
//! a 0.1.0 keyspace opens unchanged.

pub mod keyspace;
pub mod plugin;
pub mod store;

pub use keyspace::ensure_keyspace;
pub use plugin::{Config, PLUGIN};
pub use store::{TikvStore, TikvStoreCfg};
