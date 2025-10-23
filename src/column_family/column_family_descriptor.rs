use crate::column_family::ColumnFamilyOptions;

/// Descriptor for creating or opening a Column Family
///
/// Contains the name and options for a column family.
/// Used when opening a database with multiple column families.
///
/// # Example
///
/// ```ignore
/// use rucksdb::{DB, DBOptions, ColumnFamilyDescriptor, ColumnFamilyOptions};
///
/// let cf_descriptors = vec![
///     ColumnFamilyDescriptor::new("default", ColumnFamilyOptions::default()),
///     ColumnFamilyDescriptor::new("users", ColumnFamilyOptions::default()),
///     ColumnFamilyDescriptor::new("posts", ColumnFamilyOptions {
///         write_buffer_size: 8 * 1024 * 1024,  // 8MB for posts
///         ..Default::default()
///     }),
/// ];
///
/// let db = DB::open_with_column_families("mydb", DBOptions::default(), cf_descriptors)?;
/// ```
#[derive(Debug, Clone)]
pub struct ColumnFamilyDescriptor {
    /// Name of the column family
    pub name: String,

    /// Options for this column family
    pub options: ColumnFamilyOptions,
}

impl ColumnFamilyDescriptor {
    /// Create a new column family descriptor
    pub fn new<S: Into<String>>(name: S, options: ColumnFamilyOptions) -> Self {
        ColumnFamilyDescriptor {
            name: name.into(),
            options,
        }
    }
}
