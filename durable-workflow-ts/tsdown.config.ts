import { defineConfig } from "tsdown";

export default defineConfig({
  entry: ["ts/index.ts"],
  format: "esm",
  platform: "node",
  // `dist/` sits next to `ts/`, so `../durable.<platform>.node` in
  // `native.ts` points at a local build in the package root either way.
  outDir: "dist",
  dts: true,
  sourcemap: true,
});
