# typescript-onboarding

User onboarding written in TypeScript on durable-workflow. One process serves an HTTP API and runs an embedded worker.

The workflow (`src/onboarding.ts`):

1. creates the account (a step);
2. sends a welcome mail (a step, retried up to 5 times, 30 s per attempt);
3. waits up to a day for the `verified` signal, without holding a worker;
4. sends a reminder if the user has not verified by then.

Every side effect is a step that gets an idempotency key (`run_id:key`). After a crash or a redeploy, the run picks
up again by replaying its history, and steps that already finished do not run again.

## Run it

Build the SDK first, then install the example:

```bash
cd ../../bindings/typescript && npm ci && npm run build:native && npm run build
```

```bash
npm install
```

With the memory backend, nothing survives a restart:

```bash
npm start
```

With Postgres and Redis from the repository's `docker-compose.yml`:

```bash
docker compose up -d
```

```bash
DATABASE_URL=postgres://postgres:postgres@localhost/durable REDIS_URL=redis://localhost npm start
```

Then:

```bash
curl -XPOST localhost:3000/signup -H 'content-type: application/json' -d '{"email":"ann@example.com"}'
```

```bash
curl -XPOST localhost:3000/runs/<run_id>/verify -H 'content-type: application/json' -d '{"verified":true}'
```

```bash
curl localhost:3000/runs/<run_id>
```

On Postgres, stop the process while a run waits for verification, then start it again: the run continues where it
stopped. Several processes on the same database and Redis share the runs between them.

## Test it

`npm test` runs the cases on the memory backend. `npm run test:servers` also runs the Postgres and Redis cases, on
throwaway containers (Docker must be running):

- a run waiting for verification survives a restart;
- two app processes share the work;
- the real app (`node src/main.ts`), killed with SIGKILL while it sends the welcome mail, is started again and finishes
  the run, and the account step is not run a second time.

The tests also replay a finished run's history through the workflow. The replay must give the same result without
running any step. A changed version of the workflow must be caught as non-determinism.
