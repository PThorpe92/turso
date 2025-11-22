use std::cmp::Eq;
use std::sync::Arc;
use tempfile;

use crate::{
    error::LimboError,
    io::{File, IO},
    turso_assert,
    types::{IOResult, ImmutableRecord, Value, ValueRef},
    Result,
};

/// Hash function for join keys.
/// Uses FNV-1a hash algorithm which is simple and fast.
fn hash_join_key(key_values: &[ValueRef]) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET_BASIS;
    for value in key_values {
        // Hash the value based on its type
        match value {
            ValueRef::Null => {
                // Hash a special marker for NULL
                hash ^= 0;
                hash = hash.wrapping_mul(FNV_PRIME);
            }
            ValueRef::Integer(i) => {
                let bytes = i.to_le_bytes();
                for byte in bytes {
                    hash ^= byte as u64;
                    hash = hash.wrapping_mul(FNV_PRIME);
                }
            }
            ValueRef::Float(f) => {
                let bytes = f.to_le_bytes();
                for byte in bytes {
                    hash ^= byte as u64;
                    hash = hash.wrapping_mul(FNV_PRIME);
                }
            }
            ValueRef::Text(text) => {
                for byte in text.as_bytes() {
                    hash ^= *byte as u64;
                    hash = hash.wrapping_mul(FNV_PRIME);
                }
            }
            ValueRef::Blob(blob) => {
                for byte in *blob {
                    hash ^= *byte as u64;
                    hash = hash.wrapping_mul(FNV_PRIME);
                }
            }
        }
    }
    hash
}

/// Check if two key value arrays are equal.
fn keys_equal(key1: &[Value], key2: &[ValueRef]) -> bool {
    if key1.len() != key2.len() {
        return false;
    }
    for (v1, v2) in key1.iter().zip(key2.iter()) {
        if !values_equal(v1.as_ref(), *v2) {
            return false;
        }
    }
    true
}

/// Check if two values are equal.
fn values_equal(v1: ValueRef, v2: ValueRef) -> bool {
    match (v1, v2) {
        (ValueRef::Null, ValueRef::Null) => true,
        (ValueRef::Integer(i1), ValueRef::Integer(i2)) => i1 == i2,
        (ValueRef::Float(f1), ValueRef::Float(f2)) => {
            // For hash join equality, we use exact float comparison
            // This matches SQLite behavior for = operator
            f1 == f2
        }
        (ValueRef::Text(t1), ValueRef::Text(t2)) => t1.as_str() == t2.as_str(),
        (ValueRef::Blob(b1), ValueRef::Blob(b2)) => b1 == b2,
        _ => false,
    }
}

/// State machine states for hash table operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HashTableState {
    Building,
    Probing,
    Spilled,
    Closed,
}

/// A single entry in a hash table bucket.
#[derive(Debug, Clone)]
pub struct HashEntry {
    /// Hash value of the join keys.
    pub hash: u64,
    /// The join key values.
    pub key_values: Vec<Value>,
    /// The full row data.
    pub row_data: ImmutableRecord,
}

impl HashEntry {
    fn new(hash: u64, key_values: Vec<Value>, row_data: ImmutableRecord) -> Self {
        Self {
            hash,
            key_values,
            row_data,
        }
    }

    /// Get the size of this entry in bytes (approximate).
    fn size_bytes(&self) -> usize {
        let key_size: usize = self
            .key_values
            .iter()
            .map(|v| match v {
                Value::Null => 1,
                Value::Integer(_) => 8,
                Value::Float(_) => 8,
                Value::Text(t) => t.as_str().len(),
                Value::Blob(b) => b.len(),
            })
            .sum();
        key_size + self.row_data.get_payload().len() + 8 // +8 for hash
    }
}

/// A bucket in the hash table. Uses chaining for collision resolution.
#[derive(Debug, Clone)]
pub struct HashBucket {
    entries: Vec<HashEntry>,
}

impl HashBucket {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    fn insert(&mut self, entry: HashEntry) {
        self.entries.push(entry);
    }

    fn find_matches<'a>(&'a self, hash: u64, probe_keys: &[ValueRef]) -> Vec<&'a HashEntry> {
        self.entries
            .iter()
            .filter(|entry| entry.hash == hash && keys_equal(&entry.key_values, probe_keys))
            .collect()
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn size_bytes(&self) -> usize {
        self.entries.iter().map(|e| e.size_bytes()).sum()
    }
}

/// Temporary file for spilled partitions.
struct TempFile {
    _temp_dir: tempfile::TempDir,
    file: Arc<dyn File>,
}

impl core::ops::Deref for TempFile {
    type Target = Arc<dyn File>;

    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

/// Configuration for the hash table.
#[derive(Debug, Clone)]
pub struct HashTableConfig {
    /// Initial number of buckets (must be power of 2).
    pub initial_buckets: usize,
    /// Maximum memory budget in bytes.
    pub mem_budget: usize,
    /// Number of keys in the join condition.
    pub num_keys: usize,
}

impl Default for HashTableConfig {
    fn default() -> Self {
        Self {
            initial_buckets: 1024,
            mem_budget: 64 * 1024 * 1024, // 64 MB default
            num_keys: 1,
        }
    }
}

/// The main hash table structure for hash joins.
pub struct HashTable {
    /// The hash buckets.
    buckets: Vec<HashBucket>,
    /// Number of entries in the table.
    num_entries: usize,
    /// Current memory usage in bytes.
    mem_used: usize,
    /// Memory budget in bytes.
    mem_budget: usize,
    /// Number of join keys.
    num_keys: usize,
    /// Whether the hash table has spilled to disk.
    spilled: bool,
    /// Current state of the hash table.
    state: HashTableState,
    /// IO object for disk operations.
    io: Arc<dyn IO>,
    /// Temporary file for spilled data (if any).
    temp_file: Option<TempFile>,
    /// Current probe position (bucket index).
    probe_bucket_idx: usize,
    /// Current probe position (entry index within bucket).
    probe_entry_idx: usize,
    /// Current probe key values being searched.
    current_probe_keys: Option<Vec<Value>>,
}

impl HashTable {
    /// Create a new hash table.
    pub fn new(config: HashTableConfig, io: Arc<dyn IO>) -> Self {
        let num_buckets = config.initial_buckets;
        let mut buckets = Vec::with_capacity(num_buckets);
        for _ in 0..num_buckets {
            buckets.push(HashBucket::new());
        }

        Self {
            buckets,
            num_entries: 0,
            mem_used: 0,
            mem_budget: config.mem_budget,
            num_keys: config.num_keys,
            spilled: false,
            state: HashTableState::Building,
            io,
            temp_file: None,
            probe_bucket_idx: 0,
            probe_entry_idx: 0,
            current_probe_keys: None,
        }
    }

    /// Insert a row into the hash table.
    /// Returns Ok(IOResult::Done(())) on success.
    /// Returns Ok(IOResult::IO(...)) if spilling to disk is needed (async I/O).
    pub fn insert(
        &mut self,
        key_values: Vec<Value>,
        row_data: ImmutableRecord,
    ) -> Result<IOResult<()>> {
        turso_assert!(
            self.state == HashTableState::Building,
            "Cannot insert into hash table in state {:?}",
            self.state
        );

        // Compute hash of the join keys
        let key_refs: Vec<ValueRef> = key_values.iter().map(|v| v.as_ref()).collect();
        let hash = hash_join_key(&key_refs);

        // Create entry
        let entry = HashEntry::new(hash, key_values, row_data);
        let entry_size = entry.size_bytes();

        // Check if we would exceed memory budget
        if self.mem_used + entry_size > self.mem_budget && !self.spilled {
            // For MVP, we'll just return an error instead of implementing grace hash join
            // TODO: Implement spilling to disk with grace hash join
            return Err(LimboError::InternalError(
                "Hash table memory budget exceeded. Grace hash join not yet implemented."
                    .to_string(),
            ));
        }

        // Insert into appropriate bucket
        let bucket_idx = (hash as usize) % self.buckets.len();
        self.buckets[bucket_idx].insert(entry);
        self.num_entries += 1;
        self.mem_used += entry_size;

        Ok(IOResult::Done(()))
    }

    /// Finalize the build phase and prepare for probing.
    pub fn finalize_build(&mut self) {
        turso_assert!(
            self.state == HashTableState::Building,
            "Cannot finalize build in state {:?}",
            self.state
        );
        self.state = HashTableState::Probing;
    }

    /// Probe the hash table with the given keys.
    /// Returns the first matching entry if found, or None if no match.
    /// Call `next_match()` to get subsequent matches.
    pub fn probe(&mut self, probe_keys: Vec<Value>) -> Option<&HashEntry> {
        turso_assert!(
            self.state == HashTableState::Probing,
            "Cannot probe hash table in state {:?}",
            self.state
        );

        // Store probe keys first
        self.current_probe_keys = Some(probe_keys);

        // Compute hash of probe keys
        let probe_keys_ref = self.current_probe_keys.as_ref().unwrap();
        let key_refs: Vec<ValueRef> = probe_keys_ref.iter().map(|v| v.as_ref()).collect();
        let hash = hash_join_key(&key_refs);

        // Find the bucket
        let bucket_idx = (hash as usize) % self.buckets.len();
        self.probe_bucket_idx = bucket_idx;
        self.probe_entry_idx = 0;

        // Search for matches in the bucket
        let bucket = &self.buckets[bucket_idx];
        for (idx, entry) in bucket.entries.iter().enumerate() {
            if entry.hash == hash && keys_equal(&entry.key_values, &key_refs) {
                self.probe_entry_idx = idx + 1; // Next call to next_match starts here
                return Some(entry);
            }
        }

        None
    }

    /// Get the next matching entry for the current probe keys.
    /// Returns Some(entry) if another match is found, or None if no more matches.
    pub fn next_match(&mut self) -> Option<&HashEntry> {
        turso_assert!(
            self.state == HashTableState::Probing,
            "Cannot get next match in state {:?}",
            self.state
        );

        let probe_keys = self.current_probe_keys.as_ref()?;
        let key_refs: Vec<ValueRef> = probe_keys.iter().map(|v| v.as_ref()).collect();
        let hash = hash_join_key(&key_refs);

        let bucket = &self.buckets[self.probe_bucket_idx];
        for idx in self.probe_entry_idx..bucket.entries.len() {
            let entry = &bucket.entries[idx];
            if entry.hash == hash && keys_equal(&entry.key_values, &key_refs) {
                self.probe_entry_idx = idx + 1;
                return Some(entry);
            }
        }

        None
    }

    /// Close the hash table and free resources.
    pub fn close(&mut self) {
        self.state = HashTableState::Closed;
        self.buckets.clear();
        self.num_entries = 0;
        self.mem_used = 0;
        self.temp_file = None;
    }

    /// Get statistics about the hash table.
    pub fn stats(&self) -> HashTableStats {
        let mut max_chain_length = 0;
        let mut num_empty_buckets = 0;
        let mut total_chain_length = 0;

        for bucket in &self.buckets {
            let chain_len = bucket.entries.len();
            if chain_len == 0 {
                num_empty_buckets += 1;
            } else {
                total_chain_length += chain_len;
                max_chain_length = max_chain_length.max(chain_len);
            }
        }

        let num_non_empty = self.buckets.len() - num_empty_buckets;
        let avg_chain_length = if num_non_empty > 0 {
            total_chain_length as f64 / num_non_empty as f64
        } else {
            0.0
        };

        HashTableStats {
            num_buckets: self.buckets.len(),
            num_entries: self.num_entries,
            mem_used: self.mem_used,
            mem_budget: self.mem_budget,
            spilled: self.spilled,
            max_chain_length,
            avg_chain_length,
            num_empty_buckets,
        }
    }

    /// Check if the hash table is empty.
    pub fn is_empty(&self) -> bool {
        self.num_entries == 0
    }
}

/// Statistics about a hash table.
#[derive(Debug, Clone)]
pub struct HashTableStats {
    pub num_buckets: usize,
    pub num_entries: usize,
    pub mem_used: usize,
    pub mem_budget: usize,
    pub spilled: bool,
    pub max_chain_length: usize,
    pub avg_chain_length: f64,
    pub num_empty_buckets: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{types::ImmutableRecord, PlatformIO};

    #[test]
    fn test_hash_function_consistency() {
        // Test that the same keys produce the same hash
        let keys1 = vec![
            ValueRef::Integer(42),
            ValueRef::Text("hello".into()),
        ];
        let keys2 = vec![
            ValueRef::Integer(42),
            ValueRef::Text("hello".into()),
        ];
        let keys3 = vec![
            ValueRef::Integer(43),
            ValueRef::Text("hello".into()),
        ];

        let hash1 = hash_join_key(&keys1);
        let hash2 = hash_join_key(&keys2);
        let hash3 = hash_join_key(&keys3);

        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_keys_equal() {
        let key1 = vec![Value::Integer(42), Value::Text("hello".into())];
        let key2 = vec![ValueRef::Integer(42), ValueRef::Text("hello".into())];
        let key3 = vec![ValueRef::Integer(43), ValueRef::Text("hello".into())];

        assert!(keys_equal(&key1, &key2));
        assert!(!keys_equal(&key1, &key3));
    }

    #[test]
    fn test_hash_table_basic() {
        let io = Arc::new(PlatformIO::new().unwrap());
        let config = HashTableConfig {
            initial_buckets: 4,
            mem_budget: 1024 * 1024,
            num_keys: 1,
        };
        let mut ht = HashTable::new(config, io);

        // Insert some entries
        let key1 = vec![Value::Integer(1)];
        let record1 = ImmutableRecord::from_values(&[Value::Integer(1), Value::Text("row1".into())], 2);
        ht.insert(key1.clone(), record1).unwrap();

        let key2 = vec![Value::Integer(2)];
        let record2 = ImmutableRecord::from_values(&[Value::Integer(2), Value::Text("row2".into())], 2);
        ht.insert(key2.clone(), record2).unwrap();

        ht.finalize_build();

        // Probe for key1
        let result = ht.probe(key1.clone());
        assert!(result.is_some());
        assert_eq!(result.unwrap().key_values[0].as_ref(), ValueRef::Integer(1));

        // Probe for key2
        let result = ht.probe(key2);
        assert!(result.is_some());
        assert_eq!(result.unwrap().key_values[0].as_ref(), ValueRef::Integer(2));

        // Probe for non-existent key
        let result = ht.probe(vec![Value::Integer(999)]);
        assert!(result.is_none());
    }

    #[test]
    fn test_hash_table_collisions() {
        let io = Arc::new(PlatformIO::new().unwrap());
        let config = HashTableConfig {
            initial_buckets: 2, // Small number to force collisions
            mem_budget: 1024 * 1024,
            num_keys: 1,
        };
        let mut ht = HashTable::new(config, io);

        // Insert multiple entries
        for i in 0..10 {
            let key = vec![Value::Integer(i)];
            let record = ImmutableRecord::from_values(&[Value::Integer(i)], 1);
            ht.insert(key, record).unwrap();
        }

        ht.finalize_build();

        // Verify all entries can be found
        for i in 0..10 {
            let result = ht.probe(vec![Value::Integer(i)]);
            assert!(result.is_some());
            assert_eq!(result.unwrap().key_values[0].as_ref(), ValueRef::Integer(i));
        }

        let stats = ht.stats();
        assert_eq!(stats.num_entries, 10);
        assert!(stats.max_chain_length > 1); // Should have collisions with only 2 buckets
    }

    #[test]
    fn test_hash_table_duplicate_keys() {
        let io = Arc::new(PlatformIO::new().unwrap());
        let config = HashTableConfig {
            initial_buckets: 4,
            mem_budget: 1024 * 1024,
            num_keys: 1,
        };
        let mut ht = HashTable::new(config, io);

        // Insert multiple entries with the same key
        let key = vec![Value::Integer(42)];
        for i in 0..3 {
            let record = ImmutableRecord::from_values(&[Value::Integer(42), Value::Integer(i)], 2);
            ht.insert(key.clone(), record).unwrap();
        }

        ht.finalize_build();

        // Probe should return first match
        let result = ht.probe(key.clone());
        assert!(result.is_some());

        // next_match should return additional matches
        let result2 = ht.next_match();
        assert!(result2.is_some());

        let result3 = ht.next_match();
        assert!(result3.is_some());

        // No more matches
        let result4 = ht.next_match();
        assert!(result4.is_none());
    }
}
