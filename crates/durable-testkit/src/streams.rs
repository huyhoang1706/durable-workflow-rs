//! `StreamStore` conformance cases. Each takes a fresh, empty store.

use std::{sync::Arc, time::Duration};

use durable_spi::*;
use futures::future::BoxFuture;
use serde_json::{Value, json};

/// A store under test plus a way to let `Duration` of its TTL clock pass:
/// a manual clock advance for in-memory stores, a real sleep for Redis.
pub struct StreamHarness {
    pub store: Arc<dyn StreamStore>,
    pub elapse: Box<dyn Fn(Duration) -> BoxFuture<'static, ()> + Send + Sync>,
}

impl StreamHarness {
    /// Elapses time with real sleeps.
    pub fn real_time(store: Arc<dyn StreamStore>) -> Self {
        Self {
            store,
            elapse: Box::new(|d| Box::pin(tokio::time::sleep(d))),
        }
    }
}

fn run() -> RunId {
    uuid::Uuid::now_v7()
}

fn data(step: &str, attempt: u32, v: Value) -> NewChunk {
    NewChunk::data(step, attempt, v)
}

async fn read_all(store: &Arc<dyn StreamStore>, run: RunId, name: &str) -> Vec<StreamChunk> {
    store.read(run, name, None, 10_000).await.unwrap()
}

fn values(chunks: &[StreamChunk]) -> Vec<Value> {
    chunks
        .iter()
        .filter_map(|c| match &c.body {
            ChunkBody::Data(v) => Some(v.clone()),
            _ => None,
        })
        .collect()
}

pub async fn append_then_read_in_order(h: StreamHarness) {
    let (store, run) = (h.store, run());
    store
        .append(
            run,
            "t",
            vec![data("s", 1, json!("a")), data("s", 1, json!("b"))],
        )
        .await
        .unwrap();
    store
        .append(run, "t", vec![data("s", 1, json!("c"))])
        .await
        .unwrap();
    let chunks = read_all(&store, run, "t").await;
    assert_eq!(values(&chunks), [json!("a"), json!("b"), json!("c")]);
    assert!(
        chunks
            .iter()
            .all(|c| c.step_key.as_deref() == Some("s") && c.attempt == 1)
    );
    let cursors: std::collections::HashSet<_> = chunks.iter().map(|c| c.cursor.clone()).collect();
    assert_eq!(cursors.len(), 3, "cursors are unique");
}

pub async fn order_is_preserved_under_concurrent_appenders(h: StreamHarness) {
    let (store, run) = (h.store, run());
    const WRITERS: usize = 8;
    const EACH: usize = 25;
    let tasks: Vec<_> = (0..WRITERS)
        .map(|w| {
            let store = store.clone();
            tokio::spawn(async move {
                for j in 0..EACH {
                    store
                        .append(run, "t", vec![data(&format!("w{w}"), 1, json!(j))])
                        .await
                        .unwrap();
                }
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }
    let chunks = read_all(&store, run, "t").await;
    assert_eq!(chunks.len(), WRITERS * EACH);
    for w in 0..WRITERS {
        let mine: Vec<u64> = chunks
            .iter()
            .filter(|c| c.step_key.as_deref() == Some(&format!("w{w}")))
            .map(|c| match &c.body {
                ChunkBody::Data(v) => v.as_u64().unwrap(),
                other => panic!("unexpected {other:?}"),
            })
            .collect();
        assert_eq!(
            mine,
            (0..EACH as u64).collect::<Vec<_>>(),
            "writer {w} keeps its order"
        );
    }
    // Reading in pages by cursor yields the same sequence.
    let mut paged = vec![];
    let mut after: Option<StreamCursor> = None;
    loop {
        let page = store.read(run, "t", after.as_ref(), 37).await.unwrap();
        let Some(last) = page.last() else { break };
        after = Some(last.cursor.clone());
        paged.extend(page);
    }
    assert_eq!(paged, chunks);
}

pub async fn read_honours_after_and_limit(h: StreamHarness) {
    let (store, run) = (h.store, run());
    let batch: Vec<_> = (0..5).map(|i| data("s", 1, json!(i))).collect();
    store.append(run, "t", batch).await.unwrap();
    let all = read_all(&store, run, "t").await;
    let first_two = store.read(run, "t", None, 2).await.unwrap();
    assert_eq!(values(&first_two), [json!(0), json!(1)]);
    let rest = store
        .read(run, "t", Some(&first_two[1].cursor), 100)
        .await
        .unwrap();
    assert_eq!(values(&rest), [json!(2), json!(3), json!(4)]);
    let none = store
        .read(run, "t", Some(&all[4].cursor), 100)
        .await
        .unwrap();
    assert!(none.is_empty());
    // The cursor returned by append is the last chunk's.
    let last = store
        .append(run, "t", vec![data("s", 1, json!(5))])
        .await
        .unwrap();
    let tail = store
        .read(run, "t", Some(&all[4].cursor), 100)
        .await
        .unwrap();
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].cursor, last);
}

pub async fn attempt_markers_round_trip(h: StreamHarness) {
    let (store, run) = (h.store, run());
    store
        .append(
            run,
            "t",
            vec![
                NewChunk::attempt_started("gen", 1),
                data("gen", 1, json!("x")),
                NewChunk::attempt_failed("gen", 1),
                NewChunk::attempt_started("gen", 2),
                data("gen", 2, json!("y")),
            ],
        )
        .await
        .unwrap();
    let chunks = read_all(&store, run, "t").await;
    let shape: Vec<_> = chunks
        .iter()
        .map(|c| (c.step_key.as_deref(), c.attempt, c.body.clone()))
        .collect();
    assert_eq!(
        shape,
        [
            (Some("gen"), 1, ChunkBody::AttemptStarted),
            (Some("gen"), 1, ChunkBody::Data(json!("x"))),
            (Some("gen"), 1, ChunkBody::AttemptFailed),
            (Some("gen"), 2, ChunkBody::AttemptStarted),
            (Some("gen"), 2, ChunkBody::Data(json!("y"))),
        ]
    );
}

pub async fn append_after_closed_fails(h: StreamHarness) {
    let (store, run) = (h.store, run());
    store
        .append(run, "t", vec![data("s", 1, json!(1)), NewChunk::closed()])
        .await
        .unwrap();
    let err = store
        .append(run, "t", vec![data("s", 1, json!(2))])
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Closed), "{err:?}");
    let chunks = read_all(&store, run, "t").await;
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[1].body, ChunkBody::Closed);
    assert!(store.info(run, "t").await.unwrap().unwrap().closed);
}

pub async fn close_all_is_idempotent(h: StreamHarness) {
    let (store, run) = (h.store, run());
    let other = self::run();
    store
        .append(run, "a", vec![data("s", 1, json!(1))])
        .await
        .unwrap();
    store
        .append(run, "b", vec![data("s", 1, json!(2))])
        .await
        .unwrap();
    store
        .append(other, "a", vec![data("s", 1, json!(3))])
        .await
        .unwrap();
    store.close_all(run).await.unwrap();
    store.close_all(run).await.unwrap();
    for name in ["a", "b"] {
        let chunks = read_all(&store, run, name).await;
        assert_eq!(chunks.len(), 2, "one Closed marker on {name}");
        assert_eq!(chunks[1].body, ChunkBody::Closed);
    }
    // Other runs are untouched.
    assert!(!store.info(other, "a").await.unwrap().unwrap().closed);
    // Closing a run with no streams is fine.
    store.close_all(self::run()).await.unwrap();
}

pub async fn info_and_list_report_streams(h: StreamHarness) {
    let (store, run) = (h.store, run());
    assert_eq!(store.info(run, "t").await.unwrap(), None);
    assert!(store.list(run).await.unwrap().is_empty());
    store
        .append(
            run,
            "b",
            vec![data("s", 1, json!(1)), data("s", 1, json!(2))],
        )
        .await
        .unwrap();
    store
        .append(run, "a", vec![data("s", 1, json!(1))])
        .await
        .unwrap();
    store
        .append(run, "a", vec![NewChunk::closed()])
        .await
        .unwrap();
    let mut list = store.list(run).await.unwrap();
    list.sort_by(|x, y| x.stream.cmp(&y.stream));
    assert_eq!(
        list,
        [
            StreamInfo {
                stream: "a".into(),
                len: 2,
                closed: true
            },
            StreamInfo {
                stream: "b".into(),
                len: 2,
                closed: false
            },
        ]
    );
    assert_eq!(store.info(run, "b").await.unwrap(), Some(list[1].clone()));
}

pub async fn streams_are_scoped_per_run_and_name(h: StreamHarness) {
    let store = h.store;
    let (r1, r2) = (run(), run());
    store
        .append(r1, "t", vec![data("s", 1, json!(1))])
        .await
        .unwrap();
    assert!(read_all(&store, r2, "t").await.is_empty());
    assert!(read_all(&store, r1, "other").await.is_empty());
    assert!(store.list(r2).await.unwrap().is_empty());
}

pub async fn unknown_stream_reads_as_empty(h: StreamHarness) {
    let (store, run) = (h.store, run());
    assert!(read_all(&store, run, "nope").await.is_empty());
    let after = StreamCursor("0-0".into());
    assert!(
        store
            .read(run, "nope", Some(&after), 10)
            .await
            .unwrap()
            .is_empty()
    );
}

/// Needs a harness built with a TTL of 1 second.
pub async fn ttl_expires_an_idle_stream_and_append_refreshes_it(h: StreamHarness) {
    let (store, run) = (h.store, run());
    let ms = Duration::from_millis;
    store
        .append(run, "t", vec![data("s", 1, json!(1))])
        .await
        .unwrap();
    (h.elapse)(ms(600)).await;
    // An append refreshes the TTL, so the stream outlives its first deadline.
    store
        .append(run, "t", vec![data("s", 1, json!(2))])
        .await
        .unwrap();
    (h.elapse)(ms(600)).await;
    assert_eq!(read_all(&store, run, "t").await.len(), 2);
    (h.elapse)(ms(1600)).await;
    assert!(read_all(&store, run, "t").await.is_empty());
    assert_eq!(store.info(run, "t").await.unwrap(), None);
    assert!(store.list(run).await.unwrap().is_empty());
    // An expired stream starts afresh and is open again.
    store
        .append(run, "t", vec![data("s", 1, json!(3))])
        .await
        .unwrap();
    assert_eq!(values(&read_all(&store, run, "t").await), [json!(3)]);
}

/// The TTL a harness must be built with for
/// [`ttl_expires_an_idle_stream_and_append_refreshes_it`].
pub const CONFORMANCE_TTL: Duration = Duration::from_secs(1);

/// Generates one `#[tokio::test]` per `StreamStore` conformance case.
/// `$make` is a function `Duration -> impl Future<Output = StreamHarness>`
/// taking the stream TTL the store must be configured with.
#[macro_export]
macro_rules! stream_conformance {
    ($make:expr) => {
        mod stream_conformance {
            #[allow(unused_imports)]
            use super::*;
            $crate::__stream_cases!(
                $make,
                [
                    append_then_read_in_order,
                    order_is_preserved_under_concurrent_appenders,
                    read_honours_after_and_limit,
                    attempt_markers_round_trip,
                    append_after_closed_fails,
                    close_all_is_idempotent,
                    info_and_list_report_streams,
                    streams_are_scoped_per_run_and_name,
                    unknown_stream_reads_as_empty,
                ]
            );

            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn ttl_expires_an_idle_stream_and_append_refreshes_it() {
                $crate::streams::ttl_expires_an_idle_stream_and_append_refreshes_it(
                    $make($crate::streams::CONFORMANCE_TTL).await,
                )
                .await
            }
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __stream_cases {
    ($make:expr, [$($case:ident),* $(,)?]) => {
        $(
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn $case() {
                $crate::streams::$case($make(::std::time::Duration::from_secs(3600)).await).await
            }
        )*
    };
}
