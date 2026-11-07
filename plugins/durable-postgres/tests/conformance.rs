mod common;

use std::sync::Arc;

use durable_postgres::PostgresStore;
use durable_spi::Store;

async fn make_store() -> Arc<dyn Store> {
    let store = PostgresStore::from_pool(common::pool().await).schema(&common::unique_schema());
    store.migrate().await.expect("migrate");
    Arc::new(store)
}

durable_testkit::store_conformance!(make_store());
