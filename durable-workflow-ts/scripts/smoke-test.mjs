// Install packed tarballs into an empty project and run a workflow there, as
// a user of the published packages would:
//
//   node scripts/smoke-test.mjs durable-typescript-0.1.0.tgz durable-typescript-linux-x64-gnu-0.1.0.tgz
//
// Checks that the main package loads its addon from the platform package
// (not from a checkout) and that a workflow runs on the engine.
import { execFileSync } from "node:child_process";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const tarballs = process.argv.slice(2).map((p) => resolve(p));
if (tarballs.length === 0) {
  console.error("usage: node scripts/smoke-test.mjs <package.tgz>...");
  process.exit(2);
}

const dir = mkdtempSync(join(tmpdir(), "durable-smoke-"));
writeFileSync(join(dir, "package.json"), JSON.stringify({ name: "smoke", private: true, type: "module" }));
const npm = process.platform === "win32" ? "npm.cmd" : "npm";
execFileSync(npm, ["install", "--no-audit", "--no-fund", ...tarballs], { cwd: dir, stdio: "inherit", shell: process.platform === "win32" });

writeFileSync(
  join(dir, "smoke.mjs"),
  `
import { Backend, Client, Worker, workflow } from "@durable/typescript";
const hello = workflow("hello", async (ctx, name) => {
  const greeting = await ctx.step("greet", () => "hello " + name);
  await ctx.sleep("nap", 20);
  return greeting;
});
const backend = Backend.memory();
const worker = new Worker(backend, [hello], { pollIntervalMs: 20 });
await worker.start();
const out = await (await new Client(backend).start(hello, "smoke")).result();
await worker.shutdown();
if (out !== "hello smoke") throw new Error("unexpected output: " + JSON.stringify(out));
console.log("smoke test passed:", out);
`,
);
// DURABLE_NATIVE must not point the loader elsewhere.
const env = { ...process.env };
delete env.DURABLE_NATIVE;
execFileSync(process.execPath, ["smoke.mjs"], { cwd: dir, stdio: "inherit", env });
