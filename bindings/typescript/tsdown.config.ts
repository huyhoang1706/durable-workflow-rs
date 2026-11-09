import { defineConfig } from "tsdown";

export default defineConfig({
  entry: ["ts/index.ts"],
  format: "esm",
  platform: "node",
  // `dist/` sits next to `ts/`, so `../durable.node` in `native.ts` still
  // points at the addon in the package root.
  outDir: "dist",
  dts: true,
  sourcemap: true,
});
