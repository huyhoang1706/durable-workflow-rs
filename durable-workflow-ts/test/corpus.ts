// The replay conformance corpus (`crates/durable-testkit/corpus`), read from
// TypeScript, and the two checks every SDK runs against it.

import { readdirSync, readFileSync } from "node:fs";
import { isDeepStrictEqual } from "node:util";

import { native } from "../ts/native.ts";
import type { ReplayOutcome } from "../ts/replay.ts";
import type { Event, Finish, Json, Pending, TraceOp } from "../ts/wire.ts";

export interface Case {
  name: string;
  description: string;
  workflow: string;
  input: Json;
  run_id: string;
  history: Event[];
  trace: TraceOp[];
  frontier: Pending[];
  fault: string | null;
  result: Finish | null;
}

const dir = new URL("../../crates/durable-testkit/corpus/", import.meta.url);

export function loadAll(): Case[] {
  return readdirSync(dir)
    .filter((f) => f.endsWith(".json"))
    .sort()
    .map((f) => JSON.parse(readFileSync(new URL(f, dir), "utf8")) as Case);
}

const show = (v: unknown) => JSON.stringify(v);

/**
 * Check 1: drive a bare native `Replayer` with the host calls in `c.trace` and
 * check that it answers each one as recorded, then that it ends with the
 * recorded frontier and fault. The port of `durable_testkit::corpus::play`.
 */
export function play(c: Case): void {
  const r = new native.Replayer(c.run_id, c.history);
  r.record();
  const handles: [string, number][] = [];
  c.trace.forEach((op, i) => {
    switch (op.op) {
      case "user_key":
        r.userKey(op.name);
        break;
      case "check_name":
        r.checkName(op.name);
        break;
      case "key_for":
        r.keyFor(op.name);
        break;
      case "issue":
        handles.push([op.key, r.issue(op.key, op.command)]);
        break;
      case "release": {
        const h = handles.find(([k]) => k === op.key);
        if (!h) throw new Error(`op #${i}: release of \`${op.key}\`, which was never issued`);
        r.release(h[1]);
        break;
      }
      case "random":
        r.randomU64();
        break;
      case "uuid":
        r.uuid();
        break;
      case "set_fault":
        r.setFault(op.message);
        break;
      case "interrupt":
        r.interrupt();
        break;
      case "cancel":
        r.cancel();
        break;
      case "advance":
        r.advance();
        r.takeResolved();
        break;
    }
    const got = r.takeTrace();
    if (!isDeepStrictEqual(got, [op])) {
      throw new Error(`op #${i}: expected ${show(op)}, the replayer recorded ${show(got)}`);
    }
  });
  const frontier = r.frontier();
  if (!isDeepStrictEqual(frontier, c.frontier)) {
    throw new Error(`frontier: expected ${show(c.frontier)}, got ${show(frontier)}`);
  }
  if (r.fault() !== c.fault) throw new Error(`fault: expected ${show(c.fault)}, got ${show(r.fault())}`);
}

/** Messages the host writes are the SDK's own; only their place in the trace is compared. */
function comparable(op: TraceOp): TraceOp {
  return op.op === "set_fault" ? { op: "set_fault", message: "" } : op;
}

/**
 * Check 2: what replaying `c.history` through an SDK's port of the workflow
 * did must match the case. Traces must be equal (host `set_fault` messages
 * aside), and so must the frontier and the fault, unless the host raised it.
 * `ok` values must be equal; for errors only the kind is compared.
 */
export function compare(c: Case, got: ReplayOutcome): void {
  const n = Math.max(c.trace.length, got.trace.length);
  for (let i = 0; i < n; i++) {
    const want = c.trace[i];
    const have = got.trace[i];
    if (!isDeepStrictEqual(want && comparable(want), have && comparable(have))) {
      throw new Error(`trace op #${i}: expected ${show(want)}, got ${show(have)}`);
    }
  }
  if (!isDeepStrictEqual(got.frontier, c.frontier)) {
    throw new Error(`frontier: expected ${show(c.frontier)}, got ${show(got.frontier)}`);
  }
  const hostFault = c.trace.some((op) => op.op === "set_fault");
  if (!hostFault && got.fault !== c.fault) {
    throw new Error(`fault: expected ${show(c.fault)}, got ${show(got.fault)}`);
  }
  if ((c.fault === null) !== (got.fault === null)) {
    throw new Error(`fault: expected ${show(c.fault)}, got ${show(got.fault)}`);
  }
  const want = c.result;
  const have = got.result;
  const same =
    want === null || have === null
      ? want === have
      : "ok" in want
        ? "ok" in have && isDeepStrictEqual(have.ok, want.ok)
        : "err" in have && have.err.kind === want.err.kind;
  if (!same) throw new Error(`result: expected ${show(want)}, got ${show(have)}`);
}
