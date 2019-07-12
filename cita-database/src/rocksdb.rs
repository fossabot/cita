use std::default::Default;
use std::path::Path;
use std::sync::Arc;

use crate::database::{DataCategory, Database, DatabaseError};
use rocksdb::{BlockBasedOptions, ColumnFamily, Error as RocksError, Options, WriteBatch, DB};

pub struct RocksDB {
    db: Arc<DB>,
}

impl RocksDB {
    /// Open database default
    pub fn open_default<P: AsRef<Path>>(path: P) -> Result<Self, DatabaseError> {
        let db = DB::open_default(path)
            .map_err(|e| DatabaseError::Internal(e.to_string()))?;

        Ok(RocksDB { db: Arc::new(db) })
    }

    /// Open database with config
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, DatabaseError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let block_opts = BlockBasedOptions::default();
        opts.set_block_based_table_factory(&block_opts);

        let columns = [
            map_data_category(DataCategory::State),
            map_data_category(DataCategory::Headers),
            map_data_category(DataCategory::Bodies),
            map_data_category(DataCategory::Extra),
            map_data_category(DataCategory::Trace),
            map_data_category(DataCategory::AccountBloom),
            map_data_category(DataCategory::NodeInfo),
        ];
        let db = DB::open_cf(&opts, path, columns.iter())
            .map_err(|e| DatabaseError::Internal(e.to_string()))?;

        Ok(RocksDB { db: Arc::new(db) })
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
// Database categorys
/// Column for State
const COL_STATE: &str = "col0";
/// Column for Block headers
const COL_HEADERS: &str = "col1";
/// Column for Block bodies
const COL_BODIES: &str = "col2";
/// Column for Extras
const COL_EXTRA: &str = "col3";
/// Column for Traces
const COL_TRACE: &str = "col4";
/// TBD Column for the empty accounts bloom filter.
const COL_ACCOUNT_BLOOM: &str = "col5";
/// TODO Remove it. Useless category.
const COL_NODE_INFO: &str = "col6";

fn map_data_category(category: DataCategory) -> &'static str {
    match category {
        DataCategory::State => COL_STATE,
        DataCategory::Headers => COL_HEADERS,
        DataCategory::Bodies => COL_BODIES,
        DataCategory::Extra => COL_EXTRA,
        DataCategory::Trace => COL_TRACE,
        DataCategory::AccountBloom => COL_ACCOUNT_BLOOM,
        DataCategory::NodeInfo => COL_NODE_INFO,
    }
}

fn map_db_err(err: RocksError) -> DatabaseError {
    DatabaseError::Internal(err.to_string())
}

fn get_column(db: &DB, category: DataCategory) -> Result<ColumnFamily, DatabaseError> {
    db.cf_handle(map_data_category(category))
        .ok_or(DatabaseError::NotFound)
}
