(function (root) {
  "use strict";
  const isCanonicalPages = root.location.hostname === "pooya-labs.github.io";
  root.AETHERFLOW_RUNTIME_CONFIG = Object.freeze({
    mode: isCanonicalPages ? "cloudflare-free" : "manual",
    apiBase: "https://aetherflow-free-runtime.pooyagame141.workers.dev",
    bootstrapPath: "/api/v1/demo/bootstrap"
  });
})(window);
