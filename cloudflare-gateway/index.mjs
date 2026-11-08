import { DurableObject } from "cloudflare:workers";

import gateway, { ClientRateLimiter as ClientRateLimiterLogic } from "./worker.mjs";

/** Cloudflare-native Durable Object wrapper with SQLite-backed state. */
export class ClientRateLimiter extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.logic = new ClientRateLimiterLogic(ctx);
  }

  async fetch(request) {
    return this.logic.fetch(request);
  }

  async alarm() {
    return this.logic.alarm();
  }
}

export default gateway;
