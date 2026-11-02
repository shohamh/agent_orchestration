import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { defineConfig } from "@trigger.dev/sdk";

// Traces always go to the Trigger.dev dashboard. If AGENT_TRACES_OTLP_ENDPOINT is set as a project
// environment variable (e.g. http://jaeger:4318, see ../docker-compose.otel.yml), runs also export
// their spans to that shared OTLP backend. Evaluated inside each run at runtime.
const agentTracesEndpoint = process.env.AGENT_TRACES_OTLP_ENDPOINT;

export default defineConfig({
  // Shown in the dashboard: Project settings. Replace with your own project's ref.
  project: process.env.TRIGGER_PROJECT_REF ?? "proj_ykistxdfgcxwuhlrizyo",
  runtime: "node",
  dirs: ["./src/trigger"],
  maxDuration: 300,
  telemetry: {
    exporters: agentTracesEndpoint
      ? [new OTLPTraceExporter({ url: `${agentTracesEndpoint}/v1/traces` })]
      : [],
  },
  retries: {
    enabledInDev: false,
    default: { maxAttempts: 3, minTimeoutInMs: 1000, maxTimeoutInMs: 10000, factor: 2 },
  },
  build: {
    // Offline: build-env entries become `--build-arg`s of the deploy image build, so this
    //  - points the `npm i` inside the build at the internal npm mirror (NPM_MIRROR_URL), and
    //  - swaps the `# syntax=docker/dockerfile:1` frontend (normally pulled from Docker Hub)
    //    for the mirrored copy (BUILDKIT_SYNTAX_IMAGE).
    // Both come from trigger.dev/offline-deploy.env (written by offline/load.sh). No-op online.
    extensions: process.env.NPM_MIRROR_URL
      ? [
          {
            name: "offline",
            onBuildComplete(context) {
              context.addLayer({
                id: "offline",
                build: {
                  env: {
                    NPM_CONFIG_REGISTRY: process.env.NPM_MIRROR_URL!,
                    ...(process.env.BUILDKIT_SYNTAX_IMAGE
                      ? { BUILDKIT_SYNTAX: process.env.BUILDKIT_SYNTAX_IMAGE }
                      : {}),
                  },
                },
              });
            },
          },
        ]
      : [],
  },
});
