import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    // The native addon is a process-wide singleton; separate processes keep
    // test files isolated.
    pool: "forks",
  },
});
