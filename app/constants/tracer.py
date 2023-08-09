# Tracer configuration features

from opentelemetry.sdk.trace.export import ConsoleSpanExporter
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
 

# TODO. consider if these class should have a function to list all the available options
class TracerExporterType:
    CONSOLE = ConsoleSpanExporter

class TracerProcessorType:
    BATCH = BatchSpanProcessor
    SIMPLE = SimpleSpanProcessor