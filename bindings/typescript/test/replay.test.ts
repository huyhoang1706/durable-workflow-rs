// Check 2: TypeScript ports of the corpus workflows replay each history
// through the real host loop (`Execution`) and reproduce what the Rust SDK
// recorded.

import assert from "node:assert/strict";
import { test } from "node:test";

import { replay } from "../ts/index.ts";
import { compare, loadAll } from "./corpus.ts";
import { ports } from "./workflows.ts";

const cases = loadAll();

test("every corpus case has a TypeScript port", () => {
  const missing = cases.map((c) => c.name).filter((n) => !(n in ports));
  assert.deepEqual(missing, []);
});

for (const c of cases) {
  const port = ports[c.name];
  if (!port) continue;
  test(`replay ${c.name}`, async () => {
    assert.equal(port.name, c.workflow);
    compare(c, await replay(port, c.run_id, c.input, c.history));
  });
}
