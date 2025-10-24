use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::{
    column_family::{
        ColumnFamilyData, ColumnFamilyDescriptor, ColumnFamilyHandle, ColumnFamilyOptions,
    },
    util::{Result, Status},
};

/// Manages all Column Families in a DB instance
///
/// ColumnFamilySet maintains:
/// - Map of CF name → ColumnFamilyData
/// - Next CF ID allocation
/// - Default column family (always exists)
///
/// # Thread Safety
///
/// All CF operations are protected by RwLock:
/// - Reads (get CF, list CFs) use read lock
/// - Writes (create, drop) use write lock
///
/// # Lifecycle
///
/// ```text
/// DB Open → Create ColumnFamilySet → Create "default" CF
///         ↓
/// User creates more CFs → Add to map
///         ↓
/// User drops CF → Remove from map (except "default")
/// ```
pub struct ColumnFamilySet {
    /// Map of CF ID → ColumnFamilyData
    column_families: RwLock<HashMap<u32, Arc<ColumnFamilyData>>>,

    /// Map of CF name → CF ID for quick lookup
    name_to_id: RwLock<HashMap<String, u32>>,

    /// Next CF ID to allocate
    next_id: RwLock<u32>,

    /// Database path (for creating CFs)
    db_path: String,
}

impl ColumnFamilySet {
    /// Create a new ColumnFamilySet with default CF
    pub fn new(db_path: &str, default_cf_options: ColumnFamilyOptions) -> Result<Self> {
        let mut cfs = HashMap::new();
        let mut name_map = HashMap::new();

        // Create default column family (ID = 0)
        let default_cf = Arc::new(ColumnFamilyData::new(
            0,
            crate::column_family::DEFAULT_COLUMN_FAMILY_NAME.to_string(),
            default_cf_options,
            db_path,
        ));

        cfs.insert(0, default_cf);
        name_map.insert(
            crate::column_family::DEFAULT_COLUMN_FAMILY_NAME.to_string(),
            0,
        );

        Ok(ColumnFamilySet {
            column_families: RwLock::new(cfs),
            name_to_id: RwLock::new(name_map),
            next_id: RwLock::new(1), // Next ID after default
            db_path: db_path.to_string(),
        })
    }

    /// Open ColumnFamilySet with specified CFs
    pub fn open(db_path: &str, descriptors: &[ColumnFamilyDescriptor]) -> Result<Self> {
        let mut cfs = HashMap::new();
        let mut name_map = HashMap::new();
        let mut max_id = 0;

        // Ensure default CF exists
        let default_exists = descriptors
            .iter()
            .any(|d| d.name == crate::column_family::DEFAULT_COLUMN_FAMILY_NAME);

        if !default_exists {
            return Err(Status::invalid_argument(
                "Default column family must be specified",
            ));
        }

        // Create all CFs
        for (id, descriptor) in descriptors.iter().enumerate() {
            let id = id as u32;
            let cf = Arc::new(ColumnFamilyData::new(
                id,
                descriptor.name.clone(),
                descriptor.options.clone(),
                db_path,
            ));

            cfs.insert(id, cf);
            name_map.insert(descriptor.name.clone(), id);
            max_id = max_id.max(id);
        }

        Ok(ColumnFamilySet {
            column_families: RwLock::new(cfs),
            name_to_id: RwLock::new(name_map),
            next_id: RwLock::new(max_id + 1),
            db_path: db_path.to_string(),
        })
    }

    /// Get default column family
    pub fn default_cf(&self) -> Arc<ColumnFamilyData> {
        let cfs = self.column_families.read().unwrap();
        Arc::clone(cfs.get(&0).unwrap())
    }

    /// Get column family by handle
    pub fn get_cf(&self, handle: &ColumnFamilyHandle) -> Option<Arc<ColumnFamilyData>> {
        let cfs = self.column_families.read().unwrap();
        cfs.get(&handle.id()).map(Arc::clone)
    }

    /// Get column family by name
    pub fn get_cf_by_name(&self, name: &str) -> Option<Arc<ColumnFamilyData>> {
        let name_map = self.name_to_id.read().unwrap();
        let id = *name_map.get(name)?;
        drop(name_map);

        let cfs = self.column_families.read().unwrap();
        cfs.get(&id).map(Arc::clone)
    }

    /// Create a new column family
    pub fn create_cf(
        &self,
        name: String,
        options: ColumnFamilyOptions,
    ) -> Result<ColumnFamilyHandle> {
        // Check if CF already exists
        {
            let name_map = self.name_to_id.read().unwrap();
            if name_map.contains_key(&name) {
                return Err(Status::invalid_argument(format!(
                    "Column family '{}' already exists",
                    name
                )));
            }
        }

        // Allocate new ID
        let id = {
            let mut next_id = self.next_id.write().unwrap();
            let id = *next_id;
            *next_id += 1;
            id
        };

        // Create ColumnFamilyData
        let cf = Arc::new(ColumnFamilyData::new(
            id,
            name.clone(),
            options,
            &self.db_path,
        ));

        let handle = cf.handle().clone();

        // Add to maps
        {
            let mut cfs = self.column_families.write().unwrap();
            cfs.insert(id, cf);
        }
        {
            let mut name_map = self.name_to_id.write().unwrap();
            name_map.insert(name, id);
        }

        Ok(handle)
    }

    /// Drop a column family
    pub fn drop_cf(&self, handle: &ColumnFamilyHandle) -> Result<()> {
        // Cannot drop default CF
        if handle.id() == 0 {
            return Err(Status::invalid_argument(
                "Cannot drop default column family",
            ));
        }

        let name = handle.name().to_string();

        // Remove from maps
        {
            let mut cfs = self.column_families.write().unwrap();
            if cfs.remove(&handle.id()).is_none() {
                return Err(Status::not_found(format!(
                    "Column family '{}' not found",
                    name
                )));
            }
        }
        {
            let mut name_map = self.name_to_id.write().unwrap();
            name_map.remove(&name);
        }

        Ok(())
    }

    /// List all column family handles
    pub fn list_column_families(&self) -> Vec<ColumnFamilyHandle> {
        let cfs = self.column_families.read().unwrap();
        cfs.values().map(|cf| cf.handle().clone()).collect()
    }

    /// Get number of column families
    pub fn count(&self) -> usize {
        let cfs = self.column_families.read().unwrap();
        cfs.len()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_column_family_set_creation() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().to_str().unwrap();

        let cf_set = ColumnFamilySet::new(db_path, ColumnFamilyOptions::default()).unwrap();
        assert_eq!(cf_set.count(), 1); // Only default CF

        let default_cf = cf_set.default_cf();
        assert_eq!(default_cf.name(), "default");
        assert_eq!(default_cf.id(), 0);
    }

    #[test]
    fn test_create_and_get_cf() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().to_str().unwrap();

        let cf_set = ColumnFamilySet::new(db_path, ColumnFamilyOptions::default()).unwrap();

        // Create new CF
        let handle = cf_set
            .create_cf("users".to_string(), ColumnFamilyOptions::default())
            .unwrap();
        assert_eq!(handle.name(), "users");
        assert_eq!(handle.id(), 1);
        assert_eq!(cf_set.count(), 2);

        // Get by handle
        let cf = cf_set.get_cf(&handle).unwrap();
        assert_eq!(cf.name(), "users");

        // Get by name
        let cf2 = cf_set.get_cf_by_name("users").unwrap();
        assert_eq!(cf2.id(), cf.id());
    }

    #[test]
    fn test_create_duplicate_cf() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().to_str().unwrap();

        let cf_set = ColumnFamilySet::new(db_path, ColumnFamilyOptions::default()).unwrap();

        cf_set
            .create_cf("users".to_string(), ColumnFamilyOptions::default())
            .unwrap();

        // Try to create duplicate
        let result = cf_set.create_cf("users".to_string(), ColumnFamilyOptions::default());
        assert!(result.is_err());
    }

    #[test]
    fn test_drop_cf() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().to_str().unwrap();

        let cf_set = ColumnFamilySet::new(db_path, ColumnFamilyOptions::default()).unwrap();

        let handle = cf_set
            .create_cf("users".to_string(), ColumnFamilyOptions::default())
            .unwrap();
        assert_eq!(cf_set.count(), 2);

        // Drop CF
        cf_set.drop_cf(&handle).unwrap();
        assert_eq!(cf_set.count(), 1);

        // Verify it's gone
        assert!(cf_set.get_cf(&handle).is_none());
        assert!(cf_set.get_cf_by_name("users").is_none());
    }

    #[test]
    fn test_cannot_drop_default_cf() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().to_str().unwrap();

        let cf_set = ColumnFamilySet::new(db_path, ColumnFamilyOptions::default()).unwrap();
        let default_handle = cf_set.default_cf().handle().clone();

        let result = cf_set.drop_cf(&default_handle);
        assert!(result.is_err());
    }

    #[test]
    fn test_list_column_families() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().to_str().unwrap();

        let cf_set = ColumnFamilySet::new(db_path, ColumnFamilyOptions::default()).unwrap();

        cf_set
            .create_cf("users".to_string(), ColumnFamilyOptions::default())
            .unwrap();
        cf_set
            .create_cf("posts".to_string(), ColumnFamilyOptions::default())
            .unwrap();

        let handles = cf_set.list_column_families();
        assert_eq!(handles.len(), 3);

        let names: Vec<&str> = handles.iter().map(|h| h.name()).collect();
        assert!(names.contains(&"default"));
        assert!(names.contains(&"users"));
        assert!(names.contains(&"posts"));
    }

    #[test]
    fn test_open_with_descriptors() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().to_str().unwrap();

        let descriptors = vec![
            ColumnFamilyDescriptor::new("default", ColumnFamilyOptions::default()),
            ColumnFamilyDescriptor::new("users", ColumnFamilyOptions::default()),
            ColumnFamilyDescriptor::new("posts", ColumnFamilyOptions::default()),
        ];

        let cf_set = ColumnFamilySet::open(db_path, &descriptors).unwrap();
        assert_eq!(cf_set.count(), 3);

        assert!(cf_set.get_cf_by_name("default").is_some());
        assert!(cf_set.get_cf_by_name("users").is_some());
        assert!(cf_set.get_cf_by_name("posts").is_some());
    }

    #[test]
    fn test_open_without_default_fails() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().to_str().unwrap();

        let descriptors = vec![ColumnFamilyDescriptor::new(
            "users",
            ColumnFamilyOptions::default(),
        )];

        let result = ColumnFamilySet::open(db_path, &descriptors);
        assert!(result.is_err());
    }
}
