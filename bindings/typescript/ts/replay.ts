// Replay a stored history through a workflow offline, with recording on: the
// same loop the engine runs (`durable_runtime::host::replay`), with no store
// and no step execution. The conformance tests check bindings with it.

import { Execution, type Workflow } from "./execution.ts";
import { native } from "./native.ts";
import type { Event, Finish, Pending, TraceOp } from "./wire.ts";

export interface ReplayOutcome {
  trace: TraceOp[];
  /** Commands still unresolved once history ran out. */
  frontier: Pending[];
  fault: string | null;
  /** `null` while the body still waits on the frontier. */
  result: Finish | null;
}

/** `history` must already be normalized for replay. */
export async function replay(
  workflow: Workflow<any, any>,
  runId: string,
  input: unknown,
  history: Event[],
): Promise<ReplayOutcome> {
  const r = new native.Replayer(runId, history);
  r.record();
  const exec = new Execution(workflow, r, input);
  let result: Finish | null = null;
  while (r.advance() === "run_code") {
    const status = await exec.activate(r.takeResolved());
    if (status.status === "finished") result = status.result;
    if (r.fault() !== null) break;
  }
  return { trace: r.takeTrace(), frontier: r.frontier(), fault: r.fault(), result };
}
