/// Column Family module for RucksDB
///
/// Column Families allow multiple logical databases within a single DB
/// instance. Each CF has independent:
/// - MemTable and immutable MemTable
/// - SSTable files and version history
/// - Compaction state
/// - Options (compression, cache, bloom filters, etc.)
///
/// # Architecture
///
/// ```text
/// DB
///  ├─→ ColumnFamily("default")
///  │    ├─→ MemTable
///  │    ├─→ Immutable MemTable
///  │    └─→ SSTables (Version)
///  ├─→ ColumnFamily("users")
///  │    ├─→ MemTable
///  │    └─→ ...
///  └─→ ColumnFamily("posts")
///       └─→ ...
/// ```
///
/// # Usage
///
/// ```ignore
/// use rucksdb::{DB, DBOptions, ColumnFamilyOptions};
///
/// let mut db = DB::open("mydb", DBOptions::default())?;
///
/// // Create a new column family
/// let cf_options = ColumnFamilyOptions::default();
/// let users_cf = db.create_column_family("users", cf_options)?;
///
/// // Write to specific CF
/// db.put_cf(&users_cf, "user1", "alice")?;
///
/// // Read from specific CF
/// let value = db.get_cf(&users_cf, "user1")?;
/// ```
mod column_family_data;
pub mod column_family_descriptor;
pub mod column_family_handle;
pub mod column_family_options;

#[allow(unused_imports)]
pub(crate) use column_family_data::ColumnFamilyData;
pub use column_family_descriptor::ColumnFamilyDescriptor;
pub use column_family_handle::ColumnFamilyHandle;
pub use column_family_options::ColumnFamilyOptions;

/// Default column family name
pub const DEFAULT_COLUMN_FAMILY_NAME: &str = "default";
