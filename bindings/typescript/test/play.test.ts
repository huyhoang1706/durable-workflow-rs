// Check 1: the native Replayer, driven through the FFI, answers every corpus
// case's host calls as recorded.

import { expect, test } from "vitest";

import { loadAll, play } from "./corpus.ts";

const cases = loadAll();

test("the corpus is there", () => {
  expect(cases.length).toBeGreaterThanOrEqual(22);
});

for (const c of cases) {
  test(`play ${c.name}`, () => play(c));
}
