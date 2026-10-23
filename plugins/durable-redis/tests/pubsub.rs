//! `RedisPubSub` multiplexes every subscription onto one connection. This
//! binary has its own Redis container, so it can count connections exactly.

mod common;

use std::time::{Duration, Instant};

use durable_core::PubSub;
use durable_redis::RedisPubSub;
use futures::StreamExt;

/// Both tests inspect or kill every pub/sub connection on the server.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn pubsub_clients(conn: &mut redis::aio::ConnectionManager) -> usize {
    let list: String = redis::cmd("CLIENT")
        .arg("LIST")
        .arg("TYPE")
        .arg("pubsub")
        .query_async(conn)
        .await
        .unwrap();
    list.lines().filter(|l| !l.is_empty()).count()
}

async fn active_channels(conn: &mut redis::aio::ConnectionManager, prefix: &str) -> usize {
    let chans: Vec<String> = redis::cmd("PUBSUB")
        .arg("CHANNELS")
        .arg(format!("{prefix}:*"))
        .query_async(conn)
        .await
        .unwrap();
    chans.len()
}

async fn next(s: &mut futures::stream::BoxStream<'static, Vec<u8>>) -> Vec<u8> {
    tokio::time::timeout(Duration::from_secs(3), s.next())
        .await
        .expect("message arrives")
        .expect("stream open")
}

#[tokio::test]
async fn subscriptions_share_one_connection_and_unsubscribe_on_drop() {
    let _serial = SERIAL.lock().await;
    let mut admin = common::conn().await;
    let prefix = common::unique_prefix();
    let ps = RedisPubSub::new(common::conn().await, common::client().await).prefix(&prefix);

    let mut streams = vec![];
    for i in 0..20 {
        streams.push(ps.subscribe(&format!("c{i}")).await.unwrap());
    }
    let mut extra = vec![];
    for _ in 0..3 {
        extra.push(ps.subscribe("c0").await.unwrap());
    }
    // Before: one connection per subscribe call (23 here).
    assert_eq!(pubsub_clients(&mut admin).await, 1);
    assert_eq!(active_channels(&mut admin, &prefix).await, 20);

    ps.publish("c0", b"zero".to_vec()).await.unwrap();
    ps.publish("c5", b"five".to_vec()).await.unwrap();
    assert_eq!(next(&mut streams[0]).await, b"zero");
    for s in &mut extra {
        assert_eq!(next(s).await, b"zero");
    }
    assert_eq!(next(&mut streams[5]).await, b"five");

    // A channel stays subscribed while any of its streams lives.
    drop(streams);
    let until = Instant::now() + Duration::from_secs(5);
    while active_channels(&mut admin, &prefix).await != 1 {
        assert!(Instant::now() < until, "channels were not unsubscribed");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    ps.publish("c0", b"still".to_vec()).await.unwrap();
    assert_eq!(next(&mut extra[0]).await, b"still");

    drop(extra);
    while active_channels(&mut admin, &prefix).await != 0 {
        assert!(Instant::now() < until, "last channel was not unsubscribed");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Re-subscribing after the last stream went away works.
    let mut again = ps.subscribe("c0").await.unwrap();
    ps.publish("c0", b"back".to_vec()).await.unwrap();
    assert_eq!(next(&mut again).await, b"back");
    assert_eq!(pubsub_clients(&mut admin).await, 1);
}

#[tokio::test]
async fn streams_end_when_the_connection_drops_and_the_next_subscribe_reconnects() {
    let _serial = SERIAL.lock().await;
    let mut admin = common::conn().await;
    let prefix = common::unique_prefix();
    let ps = RedisPubSub::new(common::conn().await, common::client().await).prefix(&prefix);
    let mut s = ps.subscribe("x").await.unwrap();

    let _: () = redis::cmd("CLIENT")
        .arg("KILL")
        .arg("TYPE")
        .arg("pubsub")
        .query_async(&mut admin)
        .await
        .unwrap();
    let ended = tokio::time::timeout(Duration::from_secs(3), s.next())
        .await
        .expect("stream ends after the connection is killed");
    assert!(ended.is_none());

    let mut s = ps.subscribe("x").await.unwrap();
    ps.publish("x", b"hi".to_vec()).await.unwrap();
    assert_eq!(next(&mut s).await, b"hi");
}
