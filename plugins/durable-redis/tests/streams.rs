//! Redis-specific stream behaviour: key TTLs, trimming and data loss.

mod common;

use durable_core::{ChunkBody, NewChunk, StreamStore};
use durable_redis::RedisStreams;
use redis::AsyncCommands;
use serde_json::json;
use std::time::Duration;

fn run() -> uuid::Uuid {
    uuid::Uuid::now_v7()
}

fn chunk(i: u32) -> NewChunk {
    NewChunk::data("s", 1, json!(i))
}

#[tokio::test]
async fn every_key_of_a_run_gets_the_ttl_after_an_append() {
    let mut conn = common::conn().await;
    let prefix = common::unique_prefix();
    let streams = RedisStreams::new(conn.clone())
        .prefix(&prefix)
        .ttl(Duration::from_secs(90));
    let run = run();
    streams.append(run, "tokens", vec![chunk(1)]).await.unwrap();
    streams
        .append(run, "tokens", vec![NewChunk::closed()])
        .await
        .unwrap();
    let keys: Vec<String> = conn.keys(format!("{prefix}:s:*")).await.unwrap();
    assert_eq!(keys.len(), 3, "stream, names and closed sets: {keys:?}");
    for key in keys {
        let ttl: i64 = conn.pttl(&key).await.unwrap();
        assert!((1..=90_000).contains(&ttl), "{key} has pttl {ttl}");
    }
}

#[tokio::test]
async fn maxlen_trims_old_entries() {
    let streams = RedisStreams::new(common::conn().await)
        .prefix(common::unique_prefix())
        .max_len(100);
    let run = run();
    for i in 0..30 {
        let batch: Vec<_> = (0..20).map(|j| chunk(i * 20 + j)).collect();
        streams.append(run, "t", batch).await.unwrap();
    }
    let info = streams.info(run, "t").await.unwrap().unwrap();
    assert!(info.len < 600, "trimmed: {}", info.len);
    assert!(info.len >= 100, "keeps at least max_len: {}", info.len);
    // The newest chunk survives.
    let all = streams.read(run, "t", None, 10_000).await.unwrap();
    assert_eq!(all.last().unwrap().body, ChunkBody::Data(json!(599)));
}

#[tokio::test]
async fn flushall_mid_stream_does_not_fail_appends_or_reads() {
    let mut conn = common::conn().await;
    let streams = RedisStreams::new(conn.clone()).prefix(common::unique_prefix());
    let run = run();
    streams.append(run, "t", vec![chunk(1)]).await.unwrap();
    // Only this test's server-wide effect matters, so delete just our keys
    // instead of FLUSHALL (other tests share the server).
    let keys: Vec<String> = conn.keys("*:s:*").await.unwrap();
    for k in keys.iter().filter(|k| k.contains(&run.to_string())) {
        let _: () = conn.del(k).await.unwrap();
    }
    assert!(streams.read(run, "t", None, 10).await.unwrap().is_empty());
    assert_eq!(streams.info(run, "t").await.unwrap(), None);
    streams.append(run, "t", vec![chunk(2)]).await.unwrap();
    assert_eq!(streams.read(run, "t", None, 10).await.unwrap().len(), 1);
}

#[tokio::test]
async fn names_with_colons_and_unicode_do_not_collide_with_bookkeeping_keys() {
    let streams = RedisStreams::new(common::conn().await).prefix(common::unique_prefix());
    let run = run();
    for name in ["closed", "names", "a:b", "tökens"] {
        streams.append(run, name, vec![chunk(1)]).await.unwrap();
    }
    let mut names: Vec<_> = streams
        .list(run)
        .await
        .unwrap()
        .into_iter()
        .map(|i| i.stream)
        .collect();
    names.sort();
    assert_eq!(names, ["a:b", "closed", "names", "tökens"]);
    assert!(!streams.info(run, "closed").await.unwrap().unwrap().closed);
}

#[tokio::test]
async fn a_malformed_cursor_is_rejected() {
    let streams = RedisStreams::new(common::conn().await).prefix(common::unique_prefix());
    let err = streams
        .read(
            run(),
            "t",
            Some(&durable_core::StreamCursor("nope".into())),
            10,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, durable_core::Error::Config(_)), "{err:?}");
}

#[tokio::test]
async fn a_batch_with_chunks_after_closed_is_rejected() {
    let streams = RedisStreams::new(common::conn().await).prefix(common::unique_prefix());
    let run = run();
    let res = streams
        .append(run, "t", vec![chunk(1), NewChunk::closed(), chunk(2)])
        .await;
    assert!(res.is_err(), "{res:?}");
    assert!(streams.read(run, "t", None, 10).await.unwrap().is_empty());
}
