// The app's HTTP API, on `node:http` so the example needs no framework:
//
//   POST /signup              {"email": "..."}   start (or find) the user's onboarding run
//   POST /runs/:id/verify     {"verified": true} send the `verified` signal
//   GET  /runs/:id                               the run as stored
//
// The handlers only talk to the `Client`; the worker can run in this process
// or in another one on the same backend.

import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";

import { Client, RateLimitedError } from "@durable/typescript";

import { ONBOARDING, Signup } from "./onboarding.ts";

class HttpError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

async function readJson(req: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req as AsyncIterable<Buffer>) {
    size += chunk.length;
    if (size > 64 * 1024) throw new HttpError(413, "body too large");
    chunks.push(chunk);
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8") || "null");
  } catch {
    throw new HttpError(400, "body is not JSON");
  }
}

function send(res: ServerResponse, status: number, body: unknown): void {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify(body));
}

// Run ids are UUIDs; anything else is a 404 before it reaches the backend.
const RUN = /^\/runs\/([0-9a-f-]{36})(\/verify)?$/;

export function createApp(client: Client): Server {
  async function handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const path = new URL(req.url ?? "/", "http://localhost").pathname;

    if (req.method === "POST" && path === "/signup") {
      const body = Signup.safeParse(await readJson(req));
      if (!body.success) throw new HttpError(400, "expected {\"email\": \"...\"}");
      // One run per address: signing up twice returns the first run.
      const run = await client.start(ONBOARDING, body.data, { idempotencyKey: `signup:${body.data.email}` });
      return send(res, run.existing ? 200 : 201, { run_id: run.id });
    }

    const m = RUN.exec(path);
    if (m) {
      const runId = m[1]!;
      if (req.method === "POST" && m[2]) {
        const body = (await readJson(req)) as { verified?: unknown } | null;
        if (typeof body?.verified !== "boolean") throw new HttpError(400, "expected {\"verified\": true|false}");
        if ((await client.getRun(runId)) === null) throw new HttpError(404, "no such run");
        await client.signal(runId, "verified", body.verified);
        return send(res, 202, {});
      }
      if (req.method === "GET" && !m[2]) {
        const run = await client.getRun(runId);
        if (run === null) throw new HttpError(404, "no such run");
        return send(res, 200, run);
      }
    }
    throw new HttpError(404, "not found");
  }

  return createServer((req, res) => {
    handle(req, res).catch((e: unknown) => {
      if (e instanceof HttpError) return send(res, e.status, { error: e.message });
      if (e instanceof RateLimitedError) return send(res, 429, { retry_at: e.retryAt.toISOString() });
      console.error(e);
      send(res, 500, { error: "internal error" });
    });
  });
}
