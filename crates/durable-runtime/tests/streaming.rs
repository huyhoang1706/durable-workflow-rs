mod common;

use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicUsize, Ordering::SeqCst},
    },
    time::Duration,
};

use common::*;
use durable_core::{Backend, EventKind, RunId};
use durable_memory::{MemoryCache, MemoryPubSub, MemoryQueue, MemoryStore};
use durable_runtime::*;
use durable_testkit::{ManualClock, t0};
use futures::StreamExt;
use serde_json::{Value, json};

async fn collect_watch(h: &Harness, id: RunId, after: u64) -> Vec<RunUpdate> {
    let feed = h.client.watch(id).after(after).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), feed.map(|u| u.unwrap()).collect())
        .await
        .expect("watch ends after the terminal event")
}

fn events_of(updates: &[RunUpdate]) -> Vec<durable_core::Event> {
    updates
        .iter()
        .filter_map(|u| match u {
            RunUpdate::Event(e) => Some(e.clone()),
            _ => None,
        })
        .collect()
}

async fn read_stream(h: &Harness, id: RunId, name: &str) -> Vec<StreamItem<String>> {
    let feed = h.client.stream::<String>(id, name).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), feed.map(|i| i.unwrap()).collect())
        .await
        .expect("stream ends")
}

fn texts(items: &[StreamItem<String>]) -> Vec<String> {
    items
        .iter()
        .filter_map(|i| match &i.body {
            StreamBody::Data(s) => Some(s.clone()),
            StreamBody::AttemptFailed => None,
        })
        .collect()
}

/// A workflow whose `gen` step streams the words of `input`.
fn token_worker(h: &Harness) -> WorkerBuilder {
    h.worker()
        .register_fn("tokens", |ctx: WfAsyncCtx, words: Vec<String>| async move {
            let text: String = ctx
                .step("gen", move || async move {
                    let w = step::current().stream::<String>("tokens");
                    for word in &words {
                        w.write(word)?;
                    }
                    Ok::<_, StepError>(words.concat())
                })
                .await?;
            Ok(text)
        })
}

#[tokio::test]
async fn watch_yields_every_event_in_order_and_ends_at_the_terminal() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register_fn("two", |ctx: WfAsyncCtx, _: Value| async move {
                let a: i64 = ctx.step("a", || async { Ok::<_, StepError>(1) }).await?;
                let b: i64 = ctx
                    .step("b", move || async move { Ok::<_, StepError>(a + 1) })
                    .await?;
                Ok(b)
            }),
    );
    let id = h.start("two", json!(null)).await;
    let updates = collect_watch(&h, id, 0).await;
    let events = events_of(&updates);
    assert_eq!(events, h.client.events(id).await.unwrap());
    let seqs: Vec<_> = events.iter().map(|e| e.seq).collect();
    assert_eq!(seqs, (1..=events.len() as u64).collect::<Vec<_>>());
    assert!(events.last().unwrap().kind.is_terminal());
    assert!(matches!(updates.last(), Some(RunUpdate::Event(_))));
}

#[tokio::test]
async fn watch_resumes_after_a_seq_without_gaps_or_duplicates() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register_fn("two", |ctx: WfAsyncCtx, _: Value| async move {
                ctx.step("a", || async { Ok::<_, StepError>(1) }).await?;
                ctx.step("b", || async { Ok::<_, StepError>(2) }).await?;
                Ok(())
            }),
    );
    let id = h.start("two", json!(null)).await;
    h.succeeded(id).await;
    let all = h.client.events(id).await.unwrap();
    let tail = events_of(&collect_watch(&h, id, 2).await);
    assert_eq!(tail, all[2..]);
    assert!(events_of(&collect_watch(&h, id, all.len() as u64).await).is_empty());
}

#[tokio::test]
async fn watch_follows_a_live_run_and_reports_step_starts() {
    let mut h = Harness::new();
    let gate = Arc::new(tokio::sync::Notify::new());
    let g = gate.clone();
    h.spawn(
        h.worker()
            .register_fn("slow", move |ctx: WfAsyncCtx, _: Value| {
                let g = g.clone();
                async move {
                    ctx.step("work", move || async move {
                        g.notified().await;
                        Ok::<_, StepError>(7)
                    })
                    .await
                }
            }),
    );
    let id = h.start("slow", json!(null)).await;
    let mut feed = h.client.watch(id).await.unwrap();
    let mut seen = vec![];
    // Read until the step is reported running, then let it finish.
    let started = tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(u) = feed.next().await {
            let u = u.unwrap();
            let hit = matches!(&u, RunUpdate::StepStarted { key, attempt: 1 } if key == "work");
            seen.push(u);
            if hit {
                return true;
            }
        }
        false
    })
    .await
    .unwrap();
    assert!(started, "saw StepStarted: {seen:?}");
    gate.notify_one();
    let rest: Vec<_> = tokio::time::timeout(Duration::from_secs(10), feed.collect::<Vec<_>>())
        .await
        .unwrap();
    seen.extend(rest.into_iter().map(|u| u.unwrap()));
    let events = events_of(&seen);
    assert_eq!(events, h.client.events(id).await.unwrap());
}

#[tokio::test]
async fn watch_of_an_unknown_run_is_not_found() {
    let h = Harness::new();
    let err = h.client.watch(uuid::Uuid::now_v7()).await.err().unwrap();
    assert!(matches!(err, RunError::NotFound), "{err:?}");
}

#[tokio::test]
async fn a_step_streams_chunks_in_order_then_the_stream_closes_with_the_run() {
    let mut h = Harness::new();
    h.spawn(token_worker(&h));
    let words: Vec<String> = (0..200).map(|i| format!("w{i} ")).collect();
    let id = h.start("tokens", json!(words)).await;
    let items = read_stream(&h, id, "tokens").await;
    assert_eq!(texts(&items), words);
    assert!(
        items
            .iter()
            .all(|i| i.step_key.as_deref() == Some("gen") && i.attempt == 1)
    );
    // The durable output is the concatenation, and chunks are not in the log.
    assert_eq!(h.succeeded(id).await, json!(words.concat()));
    // The log holds only the run's own events, not a record per chunk.
    assert_eq!(h.client.events(id).await.unwrap().len(), 3);
    let streams = h.client.streams(id).await.unwrap();
    assert_eq!(streams.len(), 1);
    assert!(streams[0].closed, "the stream is closed once the run ends");
}

#[tokio::test]
async fn a_reader_can_resume_after_a_cursor() {
    let mut h = Harness::new();
    h.spawn(token_worker(&h));
    let id = h.start("tokens", json!(["a", "b", "c", "d"])).await;
    let items = read_stream(&h, id, "tokens").await;
    let rest: Vec<_> = h
        .client
        .stream::<String>(id, "tokens")
        .after(items[1].cursor.clone())
        .await
        .unwrap()
        .map(|i| i.unwrap())
        .collect()
        .await;
    assert_eq!(texts(&rest), ["c", "d"]);
}

#[tokio::test]
async fn a_failed_attempt_is_marked_and_the_retry_streams_afresh() {
    let mut h = Harness::new();
    let attempts = Arc::new(AtomicU32::new(0));
    let a = attempts.clone();
    h.spawn(
        h.worker()
            .register_fn("flaky", move |ctx: WfAsyncCtx, _: Value| {
                let a = a.clone();
                async move {
                    ctx.step("gen", move || async move {
                        let w = step::current().stream::<String>("tokens");
                        if a.fetch_add(1, SeqCst) == 0 {
                            w.write(&"x".to_string())?;
                            w.write(&"y".to_string())?;
                            return Err(StepError::retryable("boom"));
                        }
                        w.write(&"z".to_string())?;
                        Ok("z".to_string())
                    })
                    .retry(RetryPolicy::fixed(3, Duration::ZERO))
                    .await
                }
            }),
    );
    let id = h.start("flaky", json!(null)).await;
    assert_eq!(h.succeeded(id).await, json!("z"));
    let items = read_stream(&h, id, "tokens").await;
    let shape: Vec<_> = items
        .iter()
        .map(|i| match &i.body {
            StreamBody::Data(s) => format!("{s}@{}", i.attempt),
            StreamBody::AttemptFailed => format!("failed@{}", i.attempt),
        })
        .collect();
    assert_eq!(shape, ["x@1", "y@1", "failed@1", "z@2"]);
}

#[tokio::test]
async fn all_chunks_are_readable_before_the_step_event_appears_on_watch() {
    let mut h = Harness::new();
    h.spawn(token_worker(&h));
    // A long flush interval would otherwise delay the chunks past the commit.
    let words: Vec<String> = (0..10).map(|i| i.to_string()).collect();
    let id = h.start("tokens", json!(words)).await;
    let mut feed = h.client.watch(id).await.unwrap();
    while let Some(u) = feed.next().await {
        if let RunUpdate::Event(e) = u.unwrap()
            && matches!(e.kind, EventKind::StepSucceeded { .. })
        {
            let store = h.backend.streams.clone().unwrap();
            let chunks = store.read(id, "tokens", None, 100).await.unwrap();
            let data = chunks
                .iter()
                .filter(|c| matches!(c.body, durable_core::ChunkBody::Data(_)))
                .count();
            assert_eq!(data, 10, "every chunk precedes StepSucceeded");
            return;
        }
    }
    panic!("no StepSucceeded seen");
}

#[tokio::test]
async fn parallel_steps_on_one_stream_keep_per_writer_order() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register_fn("par", |ctx: WfAsyncCtx, _: Value| async move {
                let one = |name: &'static str| {
                    ctx.step(name, move || async move {
                        let w = step::current().stream::<String>("tokens");
                        for i in 0..50 {
                            w.write(&format!("{name}{i}"))?;
                            if i % 7 == 0 {
                                tokio::task::yield_now().await;
                            }
                        }
                        Ok::<_, StepError>(())
                    })
                };
                futures::future::try_join(one("a"), one("b")).await?;
                Ok(())
            }),
    );
    let id = h.start("par", json!(null)).await;
    let items = read_stream(&h, id, "tokens").await;
    for name in ["a", "b"] {
        let mine: Vec<_> = texts(&items)
            .into_iter()
            .filter(|t| t.starts_with(name))
            .collect();
        assert_eq!(
            mine,
            (0..50).map(|i| format!("{name}{i}")).collect::<Vec<_>>()
        );
    }
}

#[tokio::test]
async fn blocking_steps_can_stream() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register_fn("blocking", |ctx: WfAsyncCtx, _: Value| async move {
                ctx.step_blocking("gen", || {
                    let w = step::current().stream::<String>("tokens");
                    for i in 0..20 {
                        w.write(&i.to_string())?;
                    }
                    Ok::<_, StepError>(20)
                })
                .await
            }),
    );
    let id = h.start("blocking", json!(null)).await;
    let items = read_stream(&h, id, "tokens").await;
    assert_eq!(
        texts(&items),
        (0..20).map(|i| i.to_string()).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn forward_pipes_a_futures_stream_and_returns_the_items() {
    let mut h = Harness::new();
    h.spawn(
        h.worker()
            .register_fn("fwd", |ctx: WfAsyncCtx, _: Value| async move {
                ctx.step("gen", || async {
                    let src = futures::stream::iter(["he", "llo"].map(String::from));
                    let got = step::current()
                        .stream::<String>("tokens")
                        .forward(src)
                        .await?;
                    Ok::<_, StepError>(got.concat())
                })
                .await
            }),
    );
    let id = h.start("fwd", json!(null)).await;
    assert_eq!(h.succeeded(id).await, json!("hello"));
    assert_eq!(texts(&read_stream(&h, id, "tokens").await), ["he", "llo"]);
}

#[tokio::test]
async fn an_expired_stream_reads_as_empty_and_the_reader_ends() {
    let mut h = Harness::new();
    h.spawn(token_worker(&h));
    let id = h.start("tokens", json!(["a", "b"])).await;
    h.succeeded(id).await;
    h.clock.advance(chrono::Duration::hours(48));
    assert!(read_stream(&h, id, "tokens").await.is_empty());
    assert!(h.client.streams(id).await.unwrap().is_empty());
}

#[tokio::test]
async fn without_a_stream_store_writes_are_noops_and_the_run_still_succeeds() {
    let clock = ManualClock::new(t0());
    let backend = Backend::builder()
        .store(MemoryStore::new())
        .queue(MemoryQueue::new())
        .pubsub(MemoryPubSub::new())
        .cache(MemoryCache::new())
        .clock(clock.clone())
        .build()
        .unwrap();
    let mut h = Harness::with_backend(clock, backend);
    h.spawn(token_worker(&h));
    let id = h.start("tokens", json!(["a", "b"])).await;
    assert_eq!(h.succeeded(id).await, json!("ab"));
    assert!(read_stream(&h, id, "tokens").await.is_empty());
    assert!(h.client.streams(id).await.unwrap().is_empty());
}

#[tokio::test]
async fn a_chunk_over_max_payload_bytes_fails_the_step() {
    let mut h = Harness::new();
    h.spawn(h.worker().max_payload_bytes(64).register_fn(
        "big",
        |ctx: WfAsyncCtx, _: Value| async move {
            ctx.step("gen", || async {
                step::current()
                    .stream::<String>("t")
                    .write(&"x".repeat(100))?;
                Ok::<_, StepError>(())
            })
            .retry(RetryPolicy::none())
            .await
        },
    ));
    let id = h.start("big", json!(null)).await;
    let run = h.status(id, durable_core::RunStatus::Failed).await;
    assert!(run.error.unwrap().contains("max_payload_bytes"));
}

#[tokio::test]
async fn is_replaying_is_true_only_while_history_is_fed() {
    let mut h = Harness::new();
    let seen = Arc::new(parking_lot::Mutex::new(Vec::<(&'static str, bool)>::new()));
    let s = seen.clone();
    h.spawn(
        h.worker()
            .register_fn("replay", move |ctx: WfAsyncCtx, _: Value| {
                let s = s.clone();
                async move {
                    s.lock().push(("start", ctx.is_replaying()));
                    ctx.step("a", || async { Ok::<_, StepError>(1) }).await?;
                    s.lock().push(("after_a", ctx.is_replaying()));
                    ctx.sleep("nap", Duration::from_secs(3600)).await?;
                    s.lock().push(("after_nap", ctx.is_replaying()));
                    Ok(())
                }
            }),
    );
    let id = h.start("replay", json!(null)).await;
    h.suspended(id).await;
    h.advance(3601);
    h.succeeded(id).await;
    let seen = seen.lock().clone();
    // First execution: live. Second execution: `start` and `after_a` are
    // replayed; `after_nap` is live.
    assert_eq!(
        seen,
        [
            ("start", false),
            ("after_a", false),
            ("start", true),
            ("after_a", true),
            ("after_nap", false),
        ]
    );
}

#[tokio::test]
async fn is_replaying_is_exposed_to_sync_workflows() {
    let mut h = Harness::new();
    let replays = Arc::new(AtomicUsize::new(0));
    let r = replays.clone();
    h.spawn(
        h.worker()
            .register_sync_fn("sync", move |ctx: &WfSyncCtx, _: Value| {
                ctx.step("a", || Ok::<_, StepError>(1))?;
                if ctx.is_replaying() {
                    r.fetch_add(1, SeqCst);
                }
                ctx.sleep("nap", Duration::from_secs(3600))?;
                Ok(())
            }),
    );
    let id = h.start("sync", json!(null)).await;
    h.suspended(id).await;
    h.advance(3601);
    h.succeeded(id).await;
    assert_eq!(replays.load(SeqCst), 1);
}
