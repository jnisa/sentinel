# Tracer configuration features

from telescope.utils.azure import AzureClient

from opentelemetry.sdk.trace.export import ConsoleSpanExporter
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
 
from azure.monitor.opentelemetry.exporter import AzureMonitorTraceExporter

# TODO. consider if these class should have a function to list all the available options
class TracerExporterType:
    CONSOLE = ConsoleSpanExporter
    # TODO. duly adequate the following exporters
    # TODO. call the function from the azure client
    AZURE_TRACE = lambda key_vault, secret: AzureClient(key_vault).get_az_monitor_exporter(secret)

class TracerProcessorType:
    BATCH = BatchSpanProcessor
    SIMPLE = SimpleSpanProcessor