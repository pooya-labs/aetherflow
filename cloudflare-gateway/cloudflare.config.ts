import { bindings, defineConfig, exports } from "cf/config";
import * as entrypoint from "./index.mjs" with { type: "cf-worker" };

export default defineConfig({
  accountId: "593f6245708f02254aa8ed0f1417f739",
  worker: {
    name: "aetherflow-gateway",
    entrypoint,
    compatibilityDate: "2026-11-08",
    workersDev: false,
    previewUrls: false,
    env: {
      // Replace this placeholder only after the AetherFlow API has a verified HTTPS origin.
      AETHERFLOW_ORIGIN: bindings.text("https://origin-not-configured.invalid"),
      AETHERFLOW_INGRESS_TOKEN: bindings.secret(),
      AETHERFLOW_GATEWAY_BURST: bindings.text("60"),
      AETHERFLOW_GATEWAY_PER_SECOND: bindings.text("20"),
      CLIENT_RATE_LIMITER: bindings.durableObject({
        worker: "aetherflow-gateway",
        exportName: "ClientRateLimiter",
      }),
    },
    exports: {
      ClientRateLimiter: exports.durableObject({ storage: "sqlite" }),
    },
    observability: {
      enabled: true,
      logs: {
        enabled: true,
        headSamplingRate: 0.2,
        invocationLogs: true,
      },
      traces: {
        enabled: false,
      },
    },
  },
});
