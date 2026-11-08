// Build the Node-API addon with cargo and copy it to `durable.node`.
// `node scripts/build-native.mjs --release` builds an optimized addon.
import { execFileSync } from "node:child_process";
import { copyFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const pkg = join(here, "..");
const release = process.argv.includes("--release");

execFileSync("cargo", ["build", "-p", "durable-typescript", ...(release ? ["--release"] : [])], {
  cwd: pkg,
  stdio: "inherit",
});
const meta = JSON.parse(
  execFileSync("cargo", ["metadata", "--format-version", "1", "--no-deps"], { cwd: pkg, encoding: "utf8" }),
);
const file = {
  linux: "libdurable_typescript.so",
  darwin: "libdurable_typescript.dylib",
  win32: "durable_typescript.dll",
}[process.platform];
if (!file) throw new Error(`unsupported platform ${process.platform}`);
copyFileSync(join(meta.target_directory, release ? "release" : "debug", file), join(pkg, "durable.node"));
console.log("built durable.node");
