use std::default::Default;
use std::path::Path;
use std::sync::Arc;

use crate::columns::map_columns;
use crate::config::{Config, BACKGROUND_COMPACTIONS, BACKGROUND_FLUSHES, WRITE_BUFFER_SIZE};
use crate::database::{DataCategory, Database, DatabaseError};
use rocksdb::{
    BlockBasedOptions, ColumnFamily, DBCompactionStyle, Error as RocksError, Options, ReadOptions,
    WriteBatch, WriteOptions, DB,
};

pub struct RocksDB {
    db: Arc<DB>,
    pub categorys: Vec<DataCategory>,
    pub config: Config,
    pub write_opts: WriteOptions,
    pub read_opts: ReadOptions,
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

        let columns: Vec<_> = (0..config.category_num.unwrap_or(0))
            .map(|c| format!("col{}", c))
            .collect();
        let columns: Vec<&str> = columns.iter().map(|n| n as &str).collect();
        debug!("[database] Columns: {:?}", columns);

        let db = match config.category_num {
            Some(_) => DB::open_cf(&opts, path, columns.iter())
                .map_err(|e| DatabaseError::Internal(e.to_string()))?,
            None => DB::open(&opts, path).map_err(|e| DatabaseError::Internal(e.to_string()))?,
        };

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
            map_columns(DataCategory::State),
            map_columns(DataCategory::Headers),
            map_columns(DataCategory::Bodies),
            map_columns(DataCategory::Extra),
            map_columns(DataCategory::Trace),
            map_columns(DataCategory::AccountBloom),
            map_columns(DataCategory::Other),
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

fn map_db_err(err: RocksError) -> DatabaseError {
    DatabaseError::Internal(err.to_string())
}

fn get_column(db: &DB, category: DataCategory) -> Result<ColumnFamily, DatabaseError> {
    db.cf_handle(map_columns(category))
        .ok_or(DatabaseError::NotFound)
}

#[cfg(test)]
mod tests {
    use super::{Config, RocksDB};
    use crate::test::{contains, get, insert, insert_batch, remove, remove_batch};

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
