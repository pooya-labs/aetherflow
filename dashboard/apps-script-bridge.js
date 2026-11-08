(function (root) {
  "use strict";

  var pending = new Map();
  var active = false;
  var started = false;
  var sessionReady = false;
  var parentOrigin = "";
  var nextId = 0;
  var bootstrapPromise = null;

  function isAppsScriptRuntime() {
    try {
      return new URLSearchParams(root.location.search).get("aetherflowRuntime") === "apps-script";
    } catch (ignored) {
      return false;
    }
  }

  function trustedParent(origin) {
    try {
      var parsed = new URL(origin);
      return parsed.protocol === "https:" &&
        (parsed.hostname === "script.google.com" ||
         parsed.hostname === "script.googleusercontent.com" ||
         parsed.hostname.endsWith(".googleusercontent.com"));
    } catch (ignored) {
      return false;
    }
  }

  function makeUuid() {
    var bytes = new Uint8Array(16);
    if (root.crypto && root.crypto.getRandomValues) {
      root.crypto.getRandomValues(bytes);
    } else {
      for (var index = 0; index < bytes.length; index += 1) bytes[index] = Math.floor(Math.random() * 256);
    }
    bytes[6] = (bytes[6] & 15) | 64;
    bytes[8] = (bytes[8] & 63) | 128;
    var hex = Array.prototype.map.call(bytes, function (value) {
      return ("0" + value.toString(16)).slice(-2);
    }).join("");
    return hex.slice(0, 8) + "-" + hex.slice(8, 12) + "-" + hex.slice(12, 16) + "-" +
      hex.slice(16, 20) + "-" + hex.slice(20);
  }

  function browserId() {
    var key = "aetherflow.apps-script.browser.v1";
    try {
      var existing = root.localStorage.getItem(key);
      if (existing && /^[a-f0-9-]{36}$/i.test(existing)) return existing;
      var generated = makeUuid();
      root.localStorage.setItem(key, generated);
      return generated;
    } catch (ignored) {
      return makeUuid();
    }
  }

  function onMessage(event) {
    if (event.source !== root.parent || !trustedParent(event.origin)) return;
    if (!event.data || typeof event.data !== "object") return;
    var message = event.data;

    if (message.type === "AETHERFLOW_BRIDGE_INIT_ERROR") {
      sessionReady = false;
      var failure = new Error(typeof message.message === "string" ? message.message : "Apps Script initialization failed.");
      pending.forEach(function (item) {
        root.clearTimeout(item.timeout);
        item.reject(failure);
      });
      pending.clear();
      if (bootstrapReject) bootstrapReject(failure);
      return;
    }

    if (message.type === "AETHERFLOW_BRIDGE_INIT") {
      var payload = message.payload;
      if (!payload || typeof payload.sessionToken !== "string" ||
          !payload.workspace || typeof payload.workspace.id !== "string") return;
      parentOrigin = event.origin;
      sessionReady = true;
      if (bootstrapResolve) bootstrapResolve({
        workspace: payload.workspace,
        user: payload.user || { name: "AetherFlow Demo Operator" },
        expiresInSeconds: Number(payload.expiresInSeconds) || 21600,
        mode: payload.mode || "apps-script-demo"
      });
      return;
    }

    if (message.type === "AETHERFLOW_BRIDGE_RESPONSE" && typeof message.id === "string") {
      var item = pending.get(message.id);
      if (!item) return;
      pending.delete(message.id);
      root.clearTimeout(item.timeout);
      var response = message.response;
      if (!response || typeof response.status !== "number") {
        item.reject(new Error("The Apps Script bridge returned an invalid response."));
      } else {
        item.resolve(response);
      }
    }
  }

  var bootstrapResolve = null;
  var bootstrapReject = null;

  function start() {
    if (!active) return Promise.reject(new Error("The Apps Script bridge is not active."));
    if (sessionReady) {
      return Promise.resolve({ mode: "apps-script-demo" });
    }
    if (bootstrapPromise) return bootstrapPromise;
    if (!started) {
      root.addEventListener("message", onMessage);
      started = true;
    }
    bootstrapPromise = new Promise(function (resolve, reject) {
      bootstrapResolve = resolve;
      bootstrapReject = reject;
      root.parent.postMessage({
        type: "AETHERFLOW_BRIDGE_HELLO",
        clientId: browserId()
      }, "*");
      root.setTimeout(function () {
        if (sessionReady) return;
        var error = new Error("The Apps Script bridge timed out. Reload the public demo.");
        bootstrapPromise = null;
        bootstrapResolve = null;
        bootstrapReject = null;
        reject(error);
      }, 25000);
    });
    return bootstrapPromise;
  }

  function request(request) {
    if (!active || !sessionReady || !parentOrigin) {
      return Promise.reject(new Error("The Apps Script demo session is not ready."));
    }
    if (!request || typeof request !== "object" || typeof request.method !== "string" || typeof request.path !== "string") {
      return Promise.reject(new Error("The API request is invalid."));
    }
    var id = "gas-" + (++nextId) + "-" + makeUuid().slice(0, 8);
    return new Promise(function (resolve, reject) {
      var timeout = root.setTimeout(function () {
        pending.delete(id);
        reject(new Error("The Apps Script request timed out. Try again."));
      }, 30000);
      pending.set(id, { resolve: resolve, reject: reject, timeout: timeout });
      root.parent.postMessage({
        type: "AETHERFLOW_BRIDGE_REQUEST",
        id: id,
        request: {
          method: request.method,
          path: request.path,
          body: request.body === undefined ? {} : request.body
        }
      }, parentOrigin);
    });
  }

  active = isAppsScriptRuntime();
  root.AetherFlowAppsScriptBridge = Object.freeze({
    isActive: function () { return active; },
    start: start,
    request: request
  });
})(window);
