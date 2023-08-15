# Tracer configuration features

from opentelemetry.sdk.trace.export import ConsoleSpanExporter
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
 
from azure.monitor.opentelemetry.exporter import AzureMonitorTraceExporter
from azure.monitor.opentelemetry.exporter import AzureMonitorLogExporter

# TODO. consider if these class should have a function to list all the available options
class TracerExporterType:
    CONSOLE = ConsoleSpanExporter
    # TODO. duly adequate the following exporters
    # TODO. call the function from the azure client
    # AZURE_TRACE = AzureMonitorTraceExporter

class TracerProcessorType:
    BATCH = BatchSpanProcessor
    SIMPLE = SimpleSpanProcessor