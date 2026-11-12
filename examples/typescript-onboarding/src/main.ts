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

import { Backend, Client, Worker } from "@durable/typescript";

import { createApp } from "./app.ts";
import { onboarding } from "./onboarding.ts";
import { demoServices } from "./services.ts";

const backend = await Backend.connect(process.env.DATABASE_URL ?? "memory://", {
  redisUrl: process.env.REDIS_URL,
  namespace: "onboarding-demo",
});

const worker = new Worker(backend, [onboarding(demoServices())]);
await worker.start();

const server = createApp(new Client(backend));
const port = Number(process.env.PORT ?? 3000);
server.listen(port, "127.0.0.1", () => console.log(`listening on http://127.0.0.1:${port}`));

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
