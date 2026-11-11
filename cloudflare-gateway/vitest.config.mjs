import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [
    cloudflareTest({
      wrangler: {
        configPath: "./wrangler.test.jsonc",
      },
      miniflare: {
        bindings: {
          // Runtime-only test secret. This is not a deployable Worker secret.
          AETHERFLOW_INGRESS_TOKEN: "test-only-ingress-secret-0123456789abcdef0123456789abcdef",
        },
      },
    }),
  ],
  test: {
    include: ["runtime.test.mjs"],
    testTimeout: 15_000,
    hookTimeout: 15_000,
  },
});
