// The onboarding example, end to end: the HTTP API, the workflow and its
// replay. The Postgres and Redis cases run when DURABLE_TEST_DATABASE_URL (and
// DURABLE_TEST_REDIS_URL) are set; `npm run test:servers` starts both.

import type { AddressInfo } from "node:net";
import type { Server } from "node:http";

import { Backend, Client, Worker, replay, workflow, type ConnectOptions } from "@durable/typescript";
import { afterEach, describe, expect, test } from "vitest";

import { createApp } from "../src/app.ts";
import { onboarding, type OnboardingOptions, type Services } from "../src/onboarding.ts";

const DATABASE_URL = process.env.DURABLE_TEST_DATABASE_URL;
const REDIS_URL = process.env.DURABLE_TEST_REDIS_URL;

/** Services that record what the workflow did. `failSends` fails that many sends first. */
function recording(failSends = 0) {
  const created: string[] = [];
  const mails: { to: string; subject: string; key: string }[] = [];
  const sendKeys: string[] = [];
  let failures = failSends;
  const services: Services = {
    accounts: {
      async create(email, key) {
        created.push(email);
        return `acct-${key.split(":")[0]!.slice(0, 8)}`;
      },
    },
    mailer: {
      async send(to, subject, key) {
        sendKeys.push(key);
        if (failures > 0) {
          failures--;
          throw new Error("mail provider unavailable");
        }
        mails.push({ to, subject, key });
      },
    },
  };
  return { services, created, mails, sendKeys };
}

interface App {
  worker: Worker;
  server: Server;
  client: Client;
  url: string;
}

const running: App[] = [];

afterEach(async () => {
  await Promise.all(running.splice(0).map(stop));
});

async function stop(app: App): Promise<void> {
  const i = running.indexOf(app);
  if (i >= 0) running.splice(i, 1);
  await new Promise((r) => app.server.close(r));
  await app.worker.shutdown();
}

/** What `src/main.ts` does, on a backend of the test's choosing and a free port. */
async function start(backend: Backend, services: Services, opts: OnboardingOptions = {}): Promise<App> {
  const worker = new Worker(backend, [onboarding(services, opts)], {
    pollIntervalMs: 20,
    reconcileIntervalMs: 100,
    drainTimeoutMs: 1_000,
  });
  await worker.start();
  const client = new Client(backend);
  const server = createApp(client);
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  const app = { worker, server, client, url: `http://127.0.0.1:${(server.address() as AddressInfo).port}` };
  running.push(app);
  return app;
}

async function call(app: App, method: string, path: string, body?: unknown): Promise<{ status: number; body: any }> {
  const res = await fetch(app.url + path, {
    method,
    headers: { "content-type": "application/json" },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
  return { status: res.status, body: await res.json() };
}

async function waitFor(what: string, f: () => Promise<boolean>, ms = 10_000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!(await f())) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 20));
  }
}

const waitingForVerification = (app: App, runId: string) =>
  waitFor("the verification wait", async () => (await call(app, "GET", `/runs/${runId}`)).body.awaiting?.includes("verified"));

async function finished(app: App, runId: string): Promise<any> {
  await waitFor("the run to finish", async () =>
    ["succeeded", "failed", "cancelled"].includes((await call(app, "GET", `/runs/${runId}`)).body.status),
  );
  return (await call(app, "GET", `/runs/${runId}`)).body;
}

describe("on the memory backend", () => {
  test("sign up, verify, done", async () => {
    const r = recording();
    const app = await start(Backend.memory(), r.services);

    const signup = await call(app, "POST", "/signup", { email: "ann@example.com" });
    expect(signup.status).toBe(201);
    const runId: string = signup.body.run_id;

    // Signing up again finds the same run.
    const again = await call(app, "POST", "/signup", { email: "ann@example.com" });
    expect(again).toStrictEqual({ status: 200, body: { run_id: runId } });

    await waitingForVerification(app, runId);
    expect((await call(app, "POST", `/runs/${runId}/verify`, { verified: true })).status).toBe(202);

    const run = await finished(app, runId);
    expect(run.status).toBe("succeeded");
    expect(run.output).toStrictEqual({ accountId: expect.stringMatching(/^acct-/), status: "verified" });
    expect(r.created).toStrictEqual(["ann@example.com"]);
    expect(r.mails.map((m) => m.subject)).toStrictEqual(["Welcome!"]);
    // The provider sees the step's idempotency key: `run_id:key`.
    expect(r.mails[0]!.key).toBe(`${runId}:send-welcome`);
  });

  test("no verification before the deadline sends a reminder", async () => {
    const r = recording();
    const app = await start(Backend.memory(), r.services, { verifyWithinMs: 200 });
    const { body } = await call(app, "POST", "/signup", { email: "bob@example.com" });

    const run = await finished(app, body.run_id);
    expect(run.output.status).toBe("reminded");
    expect(r.mails.map((m) => m.subject)).toStrictEqual(["Welcome!", "Please verify your address"]);
  });

  test("a failing mail provider is retried with the same idempotency key", async () => {
    const r = recording(2);
    const app = await start(Backend.memory(), r.services);
    const { body } = await call(app, "POST", "/signup", { email: "cy@example.com" });

    await waitingForVerification(app, body.run_id);
    expect(r.sendKeys).toStrictEqual(Array(3).fill(`${body.run_id}:send-welcome`));
    expect(r.mails).toHaveLength(1);
    expect(r.created).toHaveLength(1);
  });

  test("bad requests", async () => {
    const app = await start(Backend.memory(), recording().services);
    expect((await call(app, "POST", "/signup", { email: "not an address" })).status).toBe(400);
    expect((await call(app, "POST", "/signup", "nope")).status).toBe(400);
    const missing = "0190d1a0-0000-7000-8000-000000000000";
    expect((await call(app, "GET", `/runs/${missing}`)).status).toBe(404);
    expect((await call(app, "POST", `/runs/${missing}/verify`, { verified: true })).status).toBe(404);
    expect((await call(app, "GET", "/runs/../etc/passwd")).status).toBe(404);

    const { body } = await call(app, "POST", "/signup", { email: "dee@example.com" });
    expect((await call(app, "POST", `/runs/${body.run_id}/verify`, { verified: "yes" })).status).toBe(400);
  });

  test("a finished run's history replays to the same result, and a changed workflow is caught", async () => {
    const r = recording();
    const app = await start(Backend.memory(), r.services);
    const input = { email: "eve@example.com" };
    const { body } = await call(app, "POST", "/signup", input);
    await waitingForVerification(app, body.run_id);
    await call(app, "POST", `/runs/${body.run_id}/verify`, { verified: true });
    const run = await finished(app, body.run_id);
    const history = await app.client.events(body.run_id);

    // Replay runs no steps: it reads their results from history.
    const quiet = recording();
    const same = await replay(onboarding(quiet.services), body.run_id, input, history);
    expect(same.fault).toBeNull();
    expect(same.result).toStrictEqual({ ok: run.output });
    expect(quiet.created).toHaveLength(0);
    expect(quiet.sendKeys).toHaveLength(0);

    // Deploying code that no longer matches a run's history is a fault, not a silent divergence.
    // Here: a new version naps before doing what the old one did.
    const v1 = onboarding(recording().services);
    const v2 = workflow(v1.name, async (ctx, i: { email: string }) => {
      await ctx.sleep("first-a-nap", 10);
      return v1.body(ctx, i);
    });
    const drift = await replay(v2, body.run_id, input, history);
    expect(drift.fault).toMatch(/non-determinism/);
  });
});

// One schema per test file run, so runs from earlier runs never mix in.
const schema = `onboarding_${process.pid}_${Date.now().toString(36)}`;
let seq = 0;

function connect(opts: ConnectOptions = {}): Promise<Backend> {
  return Backend.connect(DATABASE_URL!, { schema, namespace: `ns_${seq++}`, ...opts });
}

describe.skipIf(!DATABASE_URL)("on Postgres", () => {
  test("a run waiting for verification survives a restart", async () => {
    const r = recording();
    const namespace = `restart_${Date.now().toString(36)}`;
    const first = await start(await connect({ namespace }), r.services);
    const { body } = await call(first, "POST", "/signup", { email: "fay@example.com" });
    await waitingForVerification(first, body.run_id);
    await stop(first);

    // A new process: new connections, a new worker, the same database.
    const second = await start(await connect({ namespace, migrate: false }), r.services);
    expect((await call(second, "POST", `/runs/${body.run_id}/verify`, { verified: true })).status).toBe(202);
    const run = await finished(second, body.run_id);
    expect(run.output.status).toBe("verified");
    // Finished steps did not run again after the restart.
    expect(r.created).toHaveLength(1);
    expect(r.mails.map((m) => m.subject)).toStrictEqual(["Welcome!"]);
  });
});

describe.skipIf(!DATABASE_URL || !REDIS_URL)("on Postgres with Redis", () => {
  test("two app processes share the work", async () => {
    const namespace = `shared_${Date.now().toString(36)}`;
    const opts = { namespace, redisUrl: REDIS_URL! };
    const a = recording();
    const b = recording();
    const appA = await start(await connect(opts), a.services);
    const appB = await start(await connect({ ...opts, migrate: false }), b.services);

    const emails = Array.from({ length: 8 }, (_, i) => `user${i}@example.com`);
    const ids: string[] = [];
    for (const [i, email] of emails.entries()) {
      // Sign-ups land on either app; either worker may run them.
      const { body } = await call(i % 2 ? appB : appA, "POST", "/signup", { email });
      ids.push(body.run_id);
    }
    for (const [i, id] of ids.entries()) {
      await waitingForVerification(appA, id);
      await call(i % 2 ? appA : appB, "POST", `/runs/${id}/verify`, { verified: true });
    }
    for (const id of ids) expect((await finished(appB, id)).output.status).toBe("verified");

    // Each account was created exactly once, by whichever worker ran it.
    expect([...a.created, ...b.created].sort()).toStrictEqual([...emails].sort());
  });
});
