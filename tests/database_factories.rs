use ruslock::protocol::constants::{
    EXPRIED_FLAG_MILLISECOND_TIME, TIMEOUT_FLAG_RCOUNT_IS_PRIORITY,
};
use ruslock::ClientOptions;

#[cfg(feature = "blocking")]
#[test]
fn blocking_database_supports_full_db_range_and_default_flags() {
    let client = ruslock::blocking::Client::with_options("127.0.0.1:1", ClientOptions::default());
    let db0 = client.select_database(0);
    let db255 = client.select_database(255);
    assert_eq!(db0.db_id(), 0);
    assert_eq!(db255.db_id(), 255);

    db255.set_default_timeout_flags(TIMEOUT_FLAG_RCOUNT_IS_PRIORITY);
    db255.set_default_expired_flags(EXPRIED_FLAG_MILLISECOND_TIME);
    let lock = db255.lock("factory-lock", 5, 10);
    assert_eq!(lock.db_id(), 255);
    assert_eq!(lock.timeout().value(), 5);
    assert_eq!(lock.timeout().flags(), TIMEOUT_FLAG_RCOUNT_IS_PRIORITY);
    assert_eq!(lock.expired().value(), 10);
    assert_eq!(lock.expired().flags(), EXPRIED_FLAG_MILLISECOND_TIME);

    let root_lock = client.lock("factory-lock", 1, 2);
    assert_eq!(root_lock.db_id(), 0);
}

#[cfg(feature = "aio")]
#[test]
fn async_database_supports_full_db_range_and_default_flags() {
    let client = ruslock::aio::Client::with_options("127.0.0.1:1", ClientOptions::default());
    let db0 = client.select_database(0);
    let db255 = client.select_database(255);
    assert_eq!(db0.db_id(), 0);
    assert_eq!(db255.db_id(), 255);

    db255.set_default_timeout_flags(TIMEOUT_FLAG_RCOUNT_IS_PRIORITY);
    db255.set_default_expired_flags(EXPRIED_FLAG_MILLISECOND_TIME);
    let lock = db255.lock("factory-lock", 5, 10);
    assert_eq!(lock.db_id(), 255);
    assert_eq!(lock.timeout().value(), 5);
    assert_eq!(lock.timeout().flags(), TIMEOUT_FLAG_RCOUNT_IS_PRIORITY);
    assert_eq!(lock.expired().value(), 10);
    assert_eq!(lock.expired().flags(), EXPRIED_FLAG_MILLISECOND_TIME);

    let root_lock = client.lock("factory-lock", 1, 2);
    assert_eq!(root_lock.db_id(), 0);
}
