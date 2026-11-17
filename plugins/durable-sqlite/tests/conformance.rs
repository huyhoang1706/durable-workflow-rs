mod common;

use std::sync::Arc;

use durable_spi::Store;
use durable_sqlite::SqliteStore;

/// A file database in its own temp directory. The directory is leaked for the
/// rest of the test process, since the store outlives this function.
async fn file_store() -> Arc<dyn Store> {
    let dir = Box::leak(Box::new(common::TempDir::new()));
    let store = SqliteStore::open(dir.file("durable.db"))
        .await
        .expect("open");
    store.migrate().await.expect("migrate");
    Arc::new(store)
}

async fn memory_store() -> Arc<dyn Store> {
    Arc::new(SqliteStore::in_memory())
}

mod file {
    use super::*;
    durable_testkit::store_conformance!(file_store());
}

mod memory {
    use super::*;
    durable_testkit::store_conformance!(memory_store());
}
