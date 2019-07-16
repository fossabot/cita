use crate::database::{DataCategory, Database, DatabaseError};

fn get_value<K: AsRef<[u8]>, D: Database>(
    db: &D,
    key: K,
) -> Result<Option<Vec<u8>>, DatabaseError> {
    let value = db.get(DataCategory::State, key.as_ref())?;

    Ok(value)
}

pub fn get<D: Database>(db: &D) {
    let data = b"test".to_vec();

    assert_eq!(get_value(db, "test"), Ok(None));

    db.insert(DataCategory::State, data.clone(), data.clone())
        .unwrap();
    assert_eq!(get_value(db, "test"), Ok(Some(data)));
}

pub fn insert<D: Database>(db: &D) {
    let data = b"test".to_vec();

    db.insert(DataCategory::State, data.clone(), data.clone())
        .unwrap();
    assert_eq!(get_value(db, "test"), Ok(Some(data)));
}

pub fn insert_batch<D: Database>(db: &D) {
    let data1 = b"test1".to_vec();
    let data2 = b"test2".to_vec();

    db.insert_batch(
        DataCategory::State,
        vec![data1.clone(), data2.clone()],
        vec![data1.clone(), data2.clone()],
    )
    .unwrap();

    assert_eq!(get_value(db, data1.clone()), Ok(Some(data1)));
    assert_eq!(get_value(db, data2.clone()), Ok(Some(data2)));

    match db.insert_batch(DataCategory::State, vec![b"test3".to_vec()], vec![]) {
        Err(DatabaseError::InvalidData) => (), // pass
        _ => panic!("should return error DatabaseError::InvalidData"),
    }
}

pub fn contains<D: Database>(db: &D) {
    let data = b"test".to_vec();
    let none_exist = b"none_exist".to_vec();

    db.insert(DataCategory::State, data.clone(), data.clone())
        .unwrap();

    assert_eq!(db.contains(DataCategory::State, &data), Ok(true));

    assert_eq!(db.contains(DataCategory::State, &none_exist), Ok(false));
}

pub fn remove<D: Database>(db: &D) {
    let data = b"test".to_vec();

    db.insert(DataCategory::State, data.clone(), data.clone())
        .unwrap();

    db.remove(DataCategory::State, &data).unwrap();
    assert_eq!(get_value(db, data), Ok(None));
}

pub fn remove_batch<D: Database>(db: &D) {
    let data1 = b"test1".to_vec();
    let data2 = b"test2".to_vec();

    db.insert_batch(
        DataCategory::State,
        vec![data1.clone(), data2.clone()],
        vec![data1.clone(), data2.clone()],
    )
    .unwrap();

    db.remove_batch(DataCategory::State, &[data1.clone(), data2.clone()])
        .unwrap();

    assert_eq!(get_value(db, data1), Ok(None));
    assert_eq!(get_value(db, data2), Ok(None));
}
