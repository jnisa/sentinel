# Tracer configuration features

from opentelemetry.sdk.trace.export import ConsoleSpanExporter

from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
 

class TracerExporterType:
    CONSOLE: ConsoleSpanExporter

class TracerProcessorType:
    BATCH: BatchSpanProcessor
    SIMPLE: SimpleSpanProcessor