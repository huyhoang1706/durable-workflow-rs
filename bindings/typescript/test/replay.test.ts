// Check 2: TypeScript ports of the corpus workflows replay each history
// through the real host loop (`Execution`) and reproduce what the Rust SDK
// recorded.

import { expect, test } from "vitest";

import { replay } from "../ts/index.ts";
import { compare, loadAll } from "./corpus.ts";
import { ports } from "./workflows.ts";

const cases = loadAll();

test("every corpus case has a TypeScript port", () => {
  const missing = cases.map((c) => c.name).filter((n) => !(n in ports));
  expect(missing).toStrictEqual([]);
});

for (const c of cases) {
  const port = ports[c.name];
  if (!port) continue;
  test(`replay ${c.name}`, async () => {
    expect(port.name).toBe(c.workflow);
    compare(c, await replay(port, c.run_id, c.input, c.history));
  });
}
