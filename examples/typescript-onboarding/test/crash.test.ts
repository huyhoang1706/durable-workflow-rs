// Crash and resume with the real app: `node src/main.ts`, killed with SIGKILL
// while it sends the welcome mail, then started again. Needs Postgres and
// Redis (DURABLE_TEST_DATABASE_URL / DURABLE_TEST_REDIS_URL; `npm run
// test:servers` starts both).

import { spawn, type ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";

import { afterEach, describe, expect, test } from "vitest";

const DATABASE_URL = process.env.DURABLE_TEST_DATABASE_URL;
const REDIS_URL = process.env.DURABLE_TEST_REDIS_URL;
const MAIN = fileURLToPath(new URL("../src/main.ts", import.meta.url));

interface App {
  process: ChildProcess;
  url: string;
  /** Everything the app printed so far. */
  output(): string;
}

const apps = new Set<ChildProcess>();

afterEach(() => {
  for (const p of apps) p.kill("SIGKILL");
  apps.clear();
});

/** Start the app as `npm start` would; resolves once it listens. */
function startApp(env: Record<string, string>): Promise<App> {
  const child = spawn(process.execPath, [MAIN], {
    env: { ...process.env, PORT: "0", ...env },
    stdio: ["ignore", "pipe", "inherit"],
  });
  apps.add(child);
  let out = "";
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`the app did not start:\n${out}`)), 20_000);
    child.stdout!.setEncoding("utf8");
    child.stdout!.on("data", (d: string) => {
      out += d;
      const m = /listening on (http:\/\/\S+)/.exec(out);
      if (m) {
        clearTimeout(timer);
        resolve({ process: child, url: m[1]!, output: () => out });
      }
    });
    child.once("exit", (code, signal) => {
      clearTimeout(timer);
      reject(new Error(`the app exited (${code ?? signal}):\n${out}`));
    });
  });
}

function kill(app: App): Promise<void> {
  return new Promise((resolve) => {
    app.process.once("exit", () => resolve());
    app.process.kill("SIGKILL");
  });
}

async function call(app: App, method: string, path: string, body?: unknown): Promise<{ status: number; body: any }> {
  const res = await fetch(app.url + path, {
    method,
    headers: { "content-type": "application/json" },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
  return { status: res.status, body: await res.json() };
}

async function waitFor(what: string, f: () => boolean | Promise<boolean>, ms = 20_000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!(await f())) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 20));
  }
}

const count = (text: string, needle: string) => text.split(needle).length - 1;

describe.skipIf(!DATABASE_URL || !REDIS_URL)("crash and resume", () => {
  test("an app killed mid-step is restarted and finishes the run, without redoing finished steps", async () => {
    const env = {
      DATABASE_URL: DATABASE_URL!,
      REDIS_URL: REDIS_URL!,
      DURABLE_SCHEMA: `onboarding_crash_${process.pid}_${Date.now().toString(36)}`,
      DURABLE_NAMESPACE: "crash",
      // The restarted app takes over the dead one's run once its lease lapses.
      DURABLE_LEASE_MS: "1000",
    };

    // The first process sends mails slowly: kill it in the middle of one.
    const first = await startApp({ ...env, DEMO_MAIL_DELAY_MS: "60000" });
    const { body } = await call(first, "POST", "/signup", { email: "gus@example.com" });
    const runId: string = body.run_id;
    await waitFor("the account step", () => first.output().includes("account created"));
    await new Promise((r) => setTimeout(r, 200));
    await kill(first);
    expect(first.output()).not.toContain("mail to=");

    const second = await startApp(env);
    await waitFor("the verification wait", async () =>
      ((await call(second, "GET", `/runs/${runId}`)).body.awaiting ?? []).includes("verified"),
    );
    await call(second, "POST", `/runs/${runId}/verify`, { verified: true });
    await waitFor("the run to finish", async () => (await call(second, "GET", `/runs/${runId}`)).body.status === "succeeded");
    expect((await call(second, "GET", `/runs/${runId}`)).body.output.status).toBe("verified");

    const all = first.output() + second.output();
    // The account step had finished before the crash: it was not run again.
    expect(count(all, "account created")).toBe(1);
    // The interrupted mail step was: once, with the same idempotency key.
    expect(count(all, "mail to=")).toBe(1);
    expect(second.output()).toContain(`idempotency_key=${runId}:send-welcome`);

    // Ctrl-C (SIGTERM here) stops the app cleanly.
    const code = await new Promise((resolve) => {
      second.process.once("exit", (c) => resolve(c));
      second.process.kill("SIGTERM");
    });
    expect(code).toBe(0);
  });
});
