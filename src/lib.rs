// Enable unstable features for educational purposes and performance
// optimization These features are only available in nightly Rust
#![feature(allocator_api)]
#![feature(const_trait_impl)]

pub mod cache;
pub mod column_family;
pub mod compression;
pub mod db;
pub mod filter;
pub mod iterator;
pub mod memtable;
pub mod statistics;
pub mod table;
pub mod util;
pub mod version;
pub mod wal;

pub use column_family::{
    ColumnFamilyDescriptor, ColumnFamilyHandle, ColumnFamilyOptions, DEFAULT_COLUMN_FAMILY_NAME,
};
pub use db::{DB, DBOptions, ReadOptions, WriteOptions};
pub use filter::{BloomFilterPolicy, FilterPolicy};
pub use statistics::Statistics;
pub use table::format::CompressionType;
pub use util::{Result, Slice, Status};
