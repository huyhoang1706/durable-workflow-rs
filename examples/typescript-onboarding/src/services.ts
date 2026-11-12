// Stand-ins for the app's own clients. A real app passes its database and
// its mail provider's SDK here.

import { randomUUID } from "node:crypto";

import type { Services } from "./onboarding.ts";

export interface DemoOptions {
  /** Each mail takes this long to send (default 0). */
  mailDelayMs?: number | undefined;
  log?: (line: string) => void;
}

/** Accounts in memory, idempotent on the step's key; mails printed to stdout once sent. */
export function demoServices({ mailDelayMs = 0, log = console.log }: DemoOptions = {}): Services {
  const accounts = new Map<string, string>();
  return {
    accounts: {
      async create(email, idempotencyKey) {
        let id = accounts.get(idempotencyKey);
        if (id === undefined) {
          id = randomUUID();
          accounts.set(idempotencyKey, id);
          log(`account created id=${id} email=${email}`);
        }
        return id;
      },
    },
    mailer: {
      async send(to, subject, idempotencyKey) {
        if (mailDelayMs > 0) await new Promise((r) => setTimeout(r, mailDelayMs));
        log(`mail to=${to} subject=${JSON.stringify(subject)} idempotency_key=${idempotencyKey}`);
      },
    },
  };
}
