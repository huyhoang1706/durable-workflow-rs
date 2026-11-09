// Loads the Node-API addon (`durable.node`, built by `npm run build:native`).
// Set DURABLE_NATIVE to load it from elsewhere.

import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";

import type { Abort, Advance, Command, Event, Json, Pending, Resolution, TraceOp, WireRetryPolicy } from "./wire.ts";

/** One execution's replay state machine. Every method is one synchronous call into Rust. */
export interface NativeReplayer {
  runId(): string;
  record(): void;
  takeTrace(): TraceOp[];

  userKey(name: string): string;
  checkName(name: string): void;
  keyFor(name: string): string;
  issue(key: string, command: Command): number;
  isIssued(key: string): boolean;
  keyOf(handle: number): string | null;
  release(handle: number): void;
  resolution(handle: number): Resolution | null;
  abort(): Abort | null;
  now(): string;
  randomU64(): string;
  uuid(): string;
  isReplaying(): boolean;
  setFault(message: string): void;

  advance(): Advance;
  takeResolved(): { handle: number; resolution: Resolution }[];
  pushLive(events: Event[]): void;
  frontier(): Pending[];
  fault(): string | null;
  interrupt(): void;
  cancel(): void;
}

interface NativeModule {
  Replayer: new (runId: string, history: Event[]) => NativeReplayer;
  defaultRetryPolicy(): WireRetryPolicy;
}

const path = process.env.DURABLE_NATIVE ?? fileURLToPath(new URL("../durable.node", import.meta.url));

export const native: NativeModule = createRequire(import.meta.url)(path);

export type { Json };
