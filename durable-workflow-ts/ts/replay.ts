// Replay a stored history through a workflow offline, with recording on,
// through the engine's own entry point (`Worker::replay` in Rust): the same
// loop `rebuild` runs, with no store and no step execution. The conformance
// tests check SDKs with it.

import type { Workflow } from "./execution.ts";
import type { Event, Finish, Pending, TraceOp } from "./wire.ts";
import { Backend, Worker } from "./worker.ts";

export interface ReplayOutcome {
  trace: TraceOp[];
  /** Commands still unresolved once history ran out. */
  frontier: Pending[];
  fault: string | null;
  /** `null` while the body still waits on the frontier. */
  result: Finish | null;
}

/** `history` is the run's log as stored; it is normalized the way the engine does. */
export async function replay(
  workflow: Workflow<any, any>,
  runId: string,
  input: unknown,
  history: Event[],
): Promise<ReplayOutcome> {
  return new Worker(Backend.memory(), [workflow]).replay(workflow, runId, input, history);
}
