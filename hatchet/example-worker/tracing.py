"""OpenTelemetry tracing for Hatchet workers.

- Always: spans go to the Hatchet engine's built-in collector (same address/token as the
  client) and show up on each run in the Hatchet dashboard.
- Optionally: if OTEL_EXPORTER_OTLP_ENDPOINT is set (e.g. http://jaeger:4318), the same spans are
  also exported over OTLP/HTTP to a shared backend (see ../../observability).

Call setup_tracing() once, before creating the worker. Use `tracer` for your own spans
(LLM calls, tool calls, device I/O...).
"""

import os

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from hatchet_sdk.opentelemetry.instrumentor import HatchetInstrumentor

tracer = trace.get_tracer("agent")


def setup_tracing(service_name: str) -> None:
    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))

    if os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

        # Reads OTEL_EXPORTER_OTLP_ENDPOINT / _HEADERS from the environment.
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))

    trace.set_tracer_provider(provider)
    # Adds the exporter to the Hatchet engine and instruments task runs / triggers.
    HatchetInstrumentor(tracer_provider=provider).instrument()
