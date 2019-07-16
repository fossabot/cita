#![allow(dead_code)]

use std::default::Default;
use std::path::Path;
use std::sync::Arc;

use crate::database::{DataCategory, Database, DatabaseError};
use rocksdb::{
    BlockBasedOptions, ColumnFamily, DBCompactionStyle, Error as RocksError, Options, ReadOptions,
    WriteBatch, WriteOptions, DB,
};

// Default config
const BACKGROUND_FLUSHES: i32 = 2;
const BACKGROUND_COMPACTIONS: i32 = 2;
const WRITE_BUFFER_SIZE: usize = 4 * 64 * 1024 * 1024;

// RocksDB columns
/// For State
const COL_STATE: &str = "col0";
/// For Block headers
const COL_HEADERS: &str = "col1";
/// For Block bodies
const COL_BODIES: &str = "col2";
/// For Extras
const COL_EXTRA: &str = "col3";
/// For Traces
const COL_TRACE: &str = "col4";
/// TBD. For the empty accounts bloom filter.
const COL_ACCOUNT_BLOOM: &str = "col5";
const COL_OTHER: &str = "col6";

pub struct RocksDB {
    db: Arc<DB>,
    pub categorys: Vec<DataCategory>,
    config: Config,
    write_opts: WriteOptions,
    read_opts: ReadOptions,
}

// rocksdb guarantees synchronization
unsafe impl Sync for RocksDB {}
unsafe impl Send for RocksDB {}

impl RocksDB {
    /// Open a rocksDB with default config.
    pub fn open_default<P: AsRef<Path>>(path: P) -> Result<Self, DatabaseError> {
        Self::open(path, &Config::default())
    }

    /// Open rocksDB with config.
    pub fn open<P: AsRef<Path>>(path: P, config: &Config) -> Result<Self, DatabaseError> {
        let mut opts = Options::default();
        opts.set_write_buffer_size(WRITE_BUFFER_SIZE);
        opts.set_max_background_flushes(BACKGROUND_FLUSHES);
        opts.set_max_background_compactions(BACKGROUND_COMPACTIONS);
        opts.create_if_missing(true);
        // If true, any column families that didn't exist when opening the database will be created.
        opts.create_missing_column_families(true);

        let block_opts = BlockBasedOptions::default();
        opts.set_block_based_table_factory(&block_opts);

        opts.set_max_open_files(config.max_open_files);
        opts.set_use_fsync(false);
        opts.set_compaction_style(DBCompactionStyle::Level);
        opts.set_target_file_size_base(config.compaction.target_file_size_base);
        if let Some(level_multiplier) = config.compaction.max_bytes_for_level_multiplier {
            opts.set_max_bytes_for_level_multiplier(level_multiplier);
        }
        if let Some(compactions) = config.compaction.max_background_compactions {
            opts.set_max_background_compactions(compactions);
        }

        let mut write_opts = WriteOptions::default();
        if !config.wal {
            write_opts.disable_wal(true);
        }

        let categorys = vec![
            DataCategory::State,
            DataCategory::Headers,
            DataCategory::Bodies,
            DataCategory::Extra,
            DataCategory::Trace,
            DataCategory::AccountBloom,
            DataCategory::Other,
        ];

        let mut columns = vec![];
        // TODO Use iterator
        for category in categorys.clone() {
            columns.push(map_data_category(category));
        }

        let db = if config.category_num.unwrap_or(0) == 0 {
            DB::open(&opts, path).map_err(|e| DatabaseError::Internal(e.to_string()))?
        } else {
            DB::open_cf(&opts, path, columns.iter())
                .map_err(|e| DatabaseError::Internal(e.to_string()))?
        };
        println!("open ok");

        Ok(RocksDB {
            db: Arc::new(db),
            categorys: categorys.clone(),
            write_opts,
            read_opts: ReadOptions::default(),
            config: config.clone(),
        })
    }

    #[cfg(test)]
    fn clean(&self) {
        let columns = [
            map_data_category(DataCategory::State),
            map_data_category(DataCategory::Headers),
            map_data_category(DataCategory::Bodies),
            map_data_category(DataCategory::Extra),
            map_data_category(DataCategory::Trace),
            map_data_category(DataCategory::AccountBloom),
            map_data_category(DataCategory::Other),
        ];

        for col in columns.iter() {
            self.db.drop_cf(col).unwrap();
        }
    }
}

impl Database for RocksDB {
    fn get(&self, category: DataCategory, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
        let db = Arc::clone(&self.db);
        let key = key.to_vec();

        let col = get_column(&db, category)?;
        let v = db.get_cf(col, &key).map_err(map_db_err)?;
        Ok(v.map(|v| v.to_vec()))
    }

    fn get_batch(
        &self,
        category: DataCategory,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, DatabaseError> {
        let db = Arc::clone(&self.db);
        let keys = keys.to_vec();

        let col = get_column(&db, category)?;
        let mut values = Vec::with_capacity(keys.len());

        for key in keys {
            let v = db.get_cf(col, key).map_err(map_db_err)?;
            values.push(v.map(|v| v.to_vec()));
        }
        Ok(values)
    }

    fn insert(
        &self,
        category: DataCategory,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), DatabaseError> {
        let db = Arc::clone(&self.db);

        let col = get_column(&db, category)?;
        db.put_cf(col, key, value).map_err(map_db_err)?;
        Ok(())
    }

    fn insert_batch(
        &self,
        category: DataCategory,
        keys: Vec<Vec<u8>>,
        values: Vec<Vec<u8>>,
    ) -> Result<(), DatabaseError> {
        let db = Arc::clone(&self.db);

        if keys.len() != values.len() {
            return Err(DatabaseError::InvalidData);
        }

        let col = get_column(&db, category)?;
        let mut batch = WriteBatch::default();

        for i in 0..keys.len() {
            batch
                .put_cf(col, &keys[i], &values[i])
                .map_err(map_db_err)?;
        }
        db.write(batch).map_err(map_db_err)?;
        Ok(())
    }

    fn contains(&self, category: DataCategory, key: &[u8]) -> Result<bool, DatabaseError> {
        let db = Arc::clone(&self.db);
        let key = key.to_vec();

        let col = get_column(&db, category)?;
        let v = db.get_cf(col, &key).map_err(map_db_err)?;
        Ok(v.is_some())
    }

    fn remove(&self, category: DataCategory, key: &[u8]) -> Result<(), DatabaseError> {
        let db = Arc::clone(&self.db);
        let key = key.to_vec();

        let col = get_column(&db, category)?;
        db.delete_cf(col, key).map_err(map_db_err)?;
        Ok(())
    }

    fn remove_batch(&self, category: DataCategory, keys: &[Vec<u8>]) -> Result<(), DatabaseError> {
        let db = Arc::clone(&self.db);
        let keys = keys.to_vec();

        let col = get_column(&db, category)?;

        let mut batch = WriteBatch::default();
        for key in keys {
            batch.delete_cf(col, key).map_err(map_db_err)?;
        }
        db.write(batch).map_err(map_db_err)?;
        Ok(())
    }
}

// TODO generate columns from category_num: col0-col6
fn map_data_category(category: DataCategory) -> &'static str {
    match category {
        DataCategory::State => COL_STATE,
        DataCategory::Headers => COL_HEADERS,
        DataCategory::Bodies => COL_BODIES,
        DataCategory::Extra => COL_EXTRA,
        DataCategory::Trace => COL_TRACE,
        DataCategory::AccountBloom => COL_ACCOUNT_BLOOM,
        DataCategory::Other => COL_OTHER,
    }
}

fn map_db_err(err: RocksError) -> DatabaseError {
    DatabaseError::Internal(err.to_string())
}

fn get_column(db: &DB, category: DataCategory) -> Result<ColumnFamily, DatabaseError> {
    db.cf_handle(map_data_category(category))
        .ok_or(DatabaseError::NotFound)
}

/// RocksDB configuration
/// TODO https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
#[derive(Clone)]
pub struct Config {
    /// WAL
    pub wal: bool,
    /// Number of categorys
    pub category_num: Option<u32>,
    /// Number of open files
    pub max_open_files: i32,
    /// About compaction
    pub compaction: Compaction,
    /// Good value for total_threads is the number of cores.
    pub increase_parallelism: Option<i32>,
}

impl Config {
    /// Create new `Config` with default parameters and specified set of category.
    pub fn with_category_num(category_num: Option<u32>) -> Self {
        let mut config = Self::default();
        config.category_num = category_num;
        config
    }
}

impl Default for Config {
    fn default() -> Config {
        Config {
            wal: true,
            category_num: None,
            max_open_files: 512,
            compaction: Compaction::default(),
            increase_parallelism: None,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub struct Compaction {
    /// L0-L1 target file size
    pub target_file_size_base: u64,
    pub max_bytes_for_level_multiplier: Option<f64>,
    /// Sets the maximum number of concurrent background compaction jobs
    pub max_background_compactions: Option<i32>,
}

impl Default for Compaction {
    fn default() -> Compaction {
        Compaction {
            target_file_size_base: 64 * 1024 * 1024,
            max_bytes_for_level_multiplier: None,
            max_background_compactions: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::test::{
        contains, get, insert, insert_batch, remove, remove_batch,
    };

    use super::{Config, RocksDB};

    #[test]
    fn test_get() {
        let cfg = Config::with_category_num(Some(7));
        let db = RocksDB::open("rocksdb/test_get", &cfg).unwrap();

        get(&db);
        db.clean();
    }

    #[test]
    fn test_insert() {
        let cfg = Config::with_category_num(Some(7));
        let db = RocksDB::open("rocksdb/test_insert", &cfg).unwrap();

        insert(&db);
        db.clean();
    }

    #[test]
    fn test_insert_batch() {
        let cfg = Config::with_category_num(Some(7));
        let db = RocksDB::open("rocksdb/test_insert_batch", &cfg).unwrap();

        insert_batch(&db);
        db.clean();
    }

    #[test]
    fn test_contain() {
        let cfg = Config::with_category_num(Some(7));
        let db = RocksDB::open("rocksdb/test_contain", &cfg).unwrap();

        contains(&db);
        db.clean()
    }

    #[test]
    fn test_remove() {
        let cfg = Config::with_category_num(Some(7));
        let db = RocksDB::open("rocksdb/test_remove", &cfg).unwrap();

        remove(&db);
        db.clean();
    }

    #[test]
    fn test_remove_batch() {
        let cfg = Config::with_category_num(Some(7));
        let db = RocksDB::open("rocksdb/test_remove_batch", &cfg).unwrap();

        remove_batch(&db);
        db.clean();
    }
}
