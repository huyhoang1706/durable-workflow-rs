// The onboarding app: an HTTP API and an embedded worker in one process.
//
//   node src/main.ts                                  # memory backend: nothing survives a restart
//
//   docker compose up -d                              # from the repository root
//   DATABASE_URL=postgres://postgres:postgres@localhost/durable REDIS_URL=redis://localhost node src/main.ts
//
//   curl -XPOST localhost:3000/signup -H 'content-type: application/json' -d '{"email":"ann@example.com"}'
//   curl -XPOST localhost:3000/runs/<run_id>/verify -H 'content-type: application/json' -d '{"verified":true}'
//   curl localhost:3000/runs/<run_id>
//
// With Postgres, stop the process while a run waits and start it again: the
// run resumes where it was. Run several processes on the same database and
// Redis and they share the work.
//
// Other settings: PORT (3000; 0 picks a free one), DURABLE_NAMESPACE
// (onboarding-demo), DURABLE_SCHEMA (durable), DURABLE_LEASE_MS (how soon
// another process takes over the runs of one that crashed; 30 s) and
// DEMO_MAIL_DELAY_MS (a slow mail provider, to crash the app mid-send).

import type { AddressInfo } from "node:net";

import { Backend, Client, Worker } from "@durable/typescript";

import { createApp } from "./app.ts";
import { onboarding } from "./onboarding.ts";
import { demoServices } from "./services.ts";

const env = process.env;
const ms = (v: string | undefined) => (v === undefined ? undefined : Number(v));

const backend = await Backend.connect(env.DATABASE_URL ?? "memory://", {
  redisUrl: env.REDIS_URL,
  namespace: env.DURABLE_NAMESPACE ?? "onboarding-demo",
  schema: env.DURABLE_SCHEMA,
});

const services = demoServices({ mailDelayMs: ms(env.DEMO_MAIL_DELAY_MS) });
const worker = new Worker(backend, [onboarding(services)], { leaseMs: ms(env.DURABLE_LEASE_MS) });
await worker.start();

const server = createApp(new Client(backend));
server.listen(Number(env.PORT ?? 3000), "127.0.0.1", () => {
  const { port } = server.address() as AddressInfo;
  console.log(`listening on http://127.0.0.1:${port}`);
});

let stopping = false;
async function stop(): Promise<void> {
  if (stopping) return;
  stopping = true;
  server.close();
  // Waits for running steps, then hands the other runs back to the backend.
  await worker.shutdown();
}
process.once("SIGINT", stop);
process.once("SIGTERM", stop);
