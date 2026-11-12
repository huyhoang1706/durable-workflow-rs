// `Backend.connect`: backends by URL. The Postgres and Redis cases run when
// DURABLE_TEST_DATABASE_URL (and DURABLE_TEST_REDIS_URL) point at servers,
// e.g. the ones `docker compose up -d` starts:
//
//   DURABLE_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/durable \
//   DURABLE_TEST_REDIS_URL=redis://localhost:6379 npm test

import { afterEach, describe, expect, test } from "vitest";
import { z } from "zod";

import { Backend, Client, Worker, workflow, type ConnectOptions, type Workflow } from "../ts/index.ts";

const DATABASE_URL = process.env.DURABLE_TEST_DATABASE_URL;
const REDIS_URL = process.env.DURABLE_TEST_REDIS_URL;

const workers: Worker[] = [];

afterEach(async () => {
  await Promise.all(workers.splice(0).map((w) => w.shutdown()));
});

async function startWorker(backend: Backend, workflows: Workflow<any, any>[]): Promise<Worker> {
  const worker = new Worker(backend, workflows, { pollIntervalMs: 20, reconcileIntervalMs: 100, drainTimeoutMs: 1_000 });
  workers.push(worker);
  await worker.start();
  return worker;
}

async function waitFor(what: string, f: () => Promise<boolean>, ms = 10_000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!(await f())) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 20));
  }
}

async function awaiting(client: Client, runId: string, name: string): Promise<boolean> {
  const run = (await client.getRun(runId)) as { awaiting?: string[] } | null;
  return run?.awaiting?.includes(name) ?? false;
}

/** Steps, a timer and a signal: what every backend has to carry. */
function onboarding(calls: string[]) {
  return workflow("ts_connect_onboarding", async (ctx, user: { email: string }) => {
    const id = await ctx.step("create", { output: z.string(), execute: () => (calls.push("create"), `u-${user.email}`) });
    await ctx.sleep("cool-off", 30);
    const verified = await ctx.waitForSignal("verified", { payload: z.boolean() });
    await ctx.step("welcome", () => void calls.push("welcome"));
    return { id, verified };
  });
}

async function roundTrip(backend: Backend): Promise<void> {
  const calls: string[] = [];
  const wf = onboarding(calls);
  await startWorker(backend, [wf]);
  const client = new Client(backend);
  const h = await client.start(wf, { email: "a@b.c" });
  await waitFor("the signal wait", () => awaiting(client, h.id, "verified"));
  await client.signal(h.id, "verified", true);
  expect(await h.result()).toStrictEqual({ id: "u-a@b.c", verified: true });
  expect(calls).toStrictEqual(["create", "welcome"]);
}

let seq = 0;
/** A namespace no other test (or earlier run) uses, so queues and runs never mix. */
function freshNamespace(): string {
  return `ts_${process.pid}_${Date.now().toString(36)}_${seq++}`;
}

describe("memory://", () => {
  test("runs workflows like Backend.memory()", async () => {
    const backend = await Backend.connect("memory://");
    expect(backend.namespace).toBe("default");
    await roundTrip(backend);
  });

  test("takes a namespace", async () => {
    const backend = await Backend.connect("memory://", { namespace: "tenant_a" });
    expect(backend.namespace).toBe("tenant_a");
  });
});

describe("invalid URLs and options", () => {
  const rejects = (url: string, opts: ConnectOptions, message: RegExp) =>
    expect(Backend.connect(url, opts)).rejects.toThrow(message);

  test("unknown schemes are named, without the rest of the URL", async () => {
    const err = await Backend.connect("mysql://root:hunter2@db/app").catch((e: Error) => e);
    expect(err).toBeInstanceOf(Error);
    expect((err as Error).message).toMatch(/unsupported backend URL scheme `mysql:`/);
    expect((err as Error).message).not.toContain("hunter2");
    await rejects("localhost:5432", {}, /unsupported backend URL scheme `localhost:`/);
    await rejects("", {}, /invalid backend URL/);
    await rejects("/var/run/postgresql", {}, /invalid backend URL/);
  });

  test("Postgres options are refused for memory://", async () => {
    await rejects("memory://", { redisUrl: "redis://localhost" }, /`redisUrl` only applies to a `postgres:\/\/` backend/);
    await rejects("memory://", { schema: "x" }, /`schema` only applies/);
    await rejects("memory://", { migrate: false }, /`migrate` only applies/);
    await rejects("memory://", { streamTtlMs: 1_000 }, /`streamTtlMs` only applies/);
    await rejects("memory://", { connectTimeoutMs: 1_000 }, /`connectTimeoutMs` only applies/);
    await rejects("memory://", { namespace: "" }, /namespace must not be empty/);
  });

  test("options are checked before connecting", async () => {
    // Nothing listens on port 1: these must fail on the option, not the connection.
    const url = "postgres://u:p@127.0.0.1:1/db";
    await rejects(url, { schema: "drop table x;" }, /invalid schema name `drop table x;`/);
    await rejects(url, { schema: "1abc" }, /invalid schema name/);
    await rejects(url, { redisUrl: "localhost:6379" }, /invalid redisUrl/);
    await rejects(url, { streamTtlMs: 0 }, /streamTtlMs must be positive/);
    await rejects(url, { connectTimeoutMs: 0 }, /connectTimeoutMs must be positive/);
    await rejects(url, { namespace: "" }, /namespace must not be empty/);
  });

  test("an unreachable database rejects after connectTimeoutMs", async () => {
    const started = Date.now();
    const err = await Backend.connect("postgres://u:hunter2@127.0.0.1:1/db", { connectTimeoutMs: 300 }).catch((e: Error) => e);
    expect(Date.now() - started).toBeLessThan(4_000);
    expect(err).toBeInstanceOf(Error);
    expect((err as Error).message).not.toContain("hunter2");
  });
});

// One schema per test file run: migrations and definitions never collide with
// another run's.
const schema = `ts_test_${process.pid}_${Date.now().toString(36)}`;

describe.skipIf(!DATABASE_URL)("postgres://", () => {
  const connect = (opts: ConnectOptions = {}) => Backend.connect(DATABASE_URL!, { schema, ...opts });

  test("runs steps, timers and signals", async () => {
    const backend = await connect({ namespace: freshNamespace() });
    await roundTrip(backend);
  });

  test("a run survives a restart: a new backend on the same database finishes it", async () => {
    const namespace = freshNamespace();
    const calls: string[] = [];
    const wf = onboarding(calls);

    const first = await connect({ namespace });
    const worker = await startWorker(first, [wf]);
    const h = await new Client(first).start(wf, { email: "x@y.z" });
    await waitFor("the signal wait", () => awaiting(new Client(first), h.id, "verified"));
    await worker.shutdown();

    // Without Redis the queue lives in the process; the new backend's
    // reconciler rebuilds it from Postgres.
    const second = await connect({ namespace, migrate: false });
    await startWorker(second, [wf]);
    const client = new Client(second);
    await client.signal(h.id, "verified", false);
    expect(await client.handle(h.id).result()).toStrictEqual({ id: "u-x@y.z", verified: false });
    expect(calls).toStrictEqual(["create", "welcome"]);
  });

  test("namespaces do not see each other's runs", async () => {
    const a = await connect({ namespace: freshNamespace() });
    const b = await connect({ namespace: freshNamespace() });
    const quick = workflow("ts_connect_quick", async (ctx) => ctx.step("one", () => 1));
    await startWorker(a, [quick]);
    const h = await new Client(a).start(quick, null);
    expect(await h.result()).toBe(1);
    expect(await new Client(b).getRun(h.id)).toBeNull();
    expect(await new Client(a).getRun(h.id)).not.toBeNull();
  });
});

describe.skipIf(!DATABASE_URL || !REDIS_URL)("postgres:// with Redis", () => {
  const connect = (opts: ConnectOptions = {}) => Backend.connect(DATABASE_URL!, { schema, redisUrl: REDIS_URL!, ...opts });

  test("runs steps, timers and signals", async () => {
    const backend = await connect({ namespace: freshNamespace() });
    await roundTrip(backend);
  });

  test("a client on another connection reaches the worker through Redis", async () => {
    const namespace = freshNamespace();
    const calls: string[] = [];
    const wf = onboarding(calls);
    await startWorker(await connect({ namespace }), [wf]);

    // A separate backend, as another process would have: the run is
    // dispatched through the Redis queue, the signal nudges through pub/sub.
    const client = new Client(await connect({ namespace, migrate: false }));
    const h = await client.start(wf, { email: "r@e.d" });
    await waitFor("the signal wait", () => awaiting(client, h.id, "verified"));
    await client.signal(h.id, "verified", true);
    expect(await h.result()).toStrictEqual({ id: "u-r@e.d", verified: true });
    expect(calls).toStrictEqual(["create", "welcome"]);
  });
});
