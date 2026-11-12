const workflowStub = new URL("./test-workflow-entrypoint.mjs", import.meta.url).href;

export async function resolve(specifier, context, nextResolve) {
  if (specifier === "cloudflare:workers") {
    return { url: workflowStub, shortCircuit: true };
  }
  return nextResolve(specifier, context);
}
