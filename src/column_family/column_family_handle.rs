/// Handle to a Column Family
///
/// A lightweight reference to a column family that can be used in
/// put/get/delete/iterator operations.
///
/// # Lifetime
///
/// The handle remains valid as long as the column family exists.
/// If a column family is dropped, any existing handles become invalid.
///
/// # Example
///
/// ```ignore
/// use rucksdb::{DB, ColumnFamilyHandle};
///
/// let db = DB::open("mydb", Default::default())?;
/// let cf_handle = db.create_column_family("users", Default::default())?;
///
/// // Use handle for operations
/// db.put_cf(&cf_handle, "key1", "value1")?;
/// let value = db.get_cf(&cf_handle, "key1")?;
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnFamilyHandle {
    /// Internal ID of the column family
    pub(crate) id: u32,

    /// Name of the column family
    pub(crate) name: String,
}

impl ColumnFamilyHandle {
    /// Create a new column family handle
    #[allow(dead_code)]
    pub(crate) fn new(id: u32, name: String) -> Self {
        ColumnFamilyHandle { id, name }
    }

    /// Get the column family ID
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Get the column family name
    pub fn name(&self) -> &str {
        &self.name
    }
}
