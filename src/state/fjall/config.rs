//! Configuration for the fjall-backed cell cache.

use std::path::PathBuf;

/// Configuration for the fjall-backed cell cache.
///
/// Carries only the already-resolved on-disk root. The authoritative
/// `cache_dir` resolution — environment variable, default, and validation —
/// lives on [`KeyedStateConfiguration`]; this type just hands the resolved
/// value to [`FjallClient::open`], mirroring how [`CassandraStore`] opens from
/// a [`CassandraConfiguration`].
///
/// Production deployments mount `cache_dir` at an emptyDir-type volume; on
/// partition revocation the per-partition keyspace is dropped, and on process
/// restart the whole root is wiped because Cassandra is authoritative.
///
/// [`KeyedStateConfiguration`]: crate::state::config::KeyedStateConfiguration
/// [`FjallClient::open`]: crate::state::fjall::FjallClient::open
/// [`CassandraStore`]: crate::cassandra::CassandraStore
/// [`CassandraConfiguration`]: crate::cassandra::CassandraConfiguration
#[derive(Clone, Debug)]
pub struct FjallConfiguration {
    /// Root directory under which the fjall keyspace is opened.
    pub cache_dir: PathBuf,
}
