//! V10: partitioning (sharding) on top of Raft.
//!
//! "Hashing picks the shard, the table picks the server, Raft picks the order,
//! the LSM store stores it."
//!
//! * [`hash`]    - a hash that gives the same answer on every machine, forever
//! * [`ring`]    - consistent hashing with virtual nodes (used for placement)
//! * [`routing`] - fixed key -> partition mapping and the epoch-stamped
//!   partition -> servers table
//! * [`node`]    - one server hosting many Raft groups ("multi-Raft") and
//!   routing client commands to the right partition leader
//! * [`txn`]     - atomic multi-key writes across partitions (two-phase commit
//!   whose every step is a Raft entry) and recovery of stuck transactions
//! * [`rebalance`] - adding and removing servers by moving partitions with
//!   Raft membership changes, then publishing a new routing-table epoch

pub mod hash;
pub mod node;
pub mod rebalance;
pub mod ring;
pub mod routing;
#[cfg(test)]
mod tests;
pub mod txn;
