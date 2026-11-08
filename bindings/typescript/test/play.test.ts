// Check 1: the native Replayer, driven through the FFI, answers every corpus
// case's host calls as recorded.

import assert from "node:assert/strict";
import { test } from "node:test";

import { loadAll, play } from "./corpus.ts";

const cases = loadAll();

test("the corpus is there", () => {
  assert.ok(cases.length >= 22, `found ${cases.length} cases`);
});

for (const c of cases) {
  test(`play ${c.name}`, () => play(c));
}
