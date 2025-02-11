# Class that will be used to grant the monitorization of pipelines
# currently running on the platform targeted by this project.

from typing import Optional

from opentelemetry.sdk.trace import Tracer
from opentelemetry.sdk.trace import TracerProvider

from telescope.constants.tracer import TracerProcessorType
from telescope.constants.tracer import TracerExporterType


class PipelineTracer:
    """
    Any kind of pipeline that is running on the platform targeted by this project will be
    monitored by this class, by a tracer.

    There are multiple monitorization hierarchies targeted by this project and this is
    the first level of monitorization. Is the one that aggregates the remaining ones - these
    being the services and processes.
    """

    def __init__(
            self, 
            processor_type: str = 'BATCH', 
            exporter_type: str = 'CONSOLE',
            key_vault_id: Optional[str] = None,
            app_insights_secret: Optional[str] = None
        ) -> Tracer:
        """
        Initialize the PipelineTracer class.

        :param processor_type: The type of processor that will be used by the tracer.
        :param exporter_type: The type of exporter that will be used by the tracer.
        :param key_vault_id: The id of the Azure Key Vault that will be used by the tracer.
        :param app_insights_secret: The secret that will be used by the tracer to connect to the
        application insights service.
        """

        self._processor_type = processor_type
        self._exporter_type = exporter_type
        self._key_vault_id = key_vault_id
        self._app_insights_secret = app_insights_secret

        provider = TracerProvider()

        processor = self._create_processor(self._set_exporter_type())
        provider.add_span_processor(processor)

        self.baseline_trace = provider
    
    @property
    def processor_type(self) -> TracerProcessorType:
        """
        Retrieve the processor type under usage.
        """

        return getattr(TracerProcessorType, self._processor_type)
    
    @property
    def exporter_type(self) -> TracerExporterType:
        """
        Retrieve the exporter type under usage.
        """

        return getattr(TracerExporterType, self._exporter_type)

    def get_tracer(self, tracer_id: str = __name__) -> Tracer:
        """
        Get a opentelemetry tracer.

        From the global baseline_trace - that is created by this class, taking into acccount
        the configuration features provided by the user - we can create a tracer that will
        be used to monitorize the pipeline with the tracer_id provided.

        In case we want to retrieve an existing tracer - instead of creating a new one - this
        function can also be used to that effect.

        :param tracer_id: the id of the tracer that we want to retrieve
        :return: a tracer configured according the arguments provided to this class
        """

        return self.baseline_trace.get_tracer(tracer_id)
    
    def _set_exporter_type(self) -> TracerExporterType:
        """
        Sets the type of exporter that the tracer will use.

        Any tracer that is created by this class will have an exporter attached to it. This
        exporter will be used to send the data that is being collected by the tracer to a
        specific location. Using this method will set the type of exporter that the tracer
        will use.

        :return: the exporter type configured according the arguments provided to this class
        """

        try:
            if self._exporter_type == 'AZURE_TRACE':
                return getattr(TracerExporterType, self._exporter_type)(self._key_vault_id, self._app_insights_secret)
            else:
                return getattr(TracerExporterType, self._exporter_type)()
        except:
            raise Exception(
                "Invalid exporter type provided. The only types available are: CONSOLE and AZURE_TRACE"
            )  

    def _create_processor(self, exporter: TracerExporterType) -> TracerProcessorType:
        """
        Attach a processor to the tracer.

        The processor will be used to process the data that is being collected by the tracer. This
        function attaches the processor to the baseline_trace.
        
        :param exporter: the exporter type that will be used by the processor
        :return: a processor configured according the arguments provided to this class
        """

        try:
            return getattr(TracerProcessorType, self._processor_type)(exporter)
        except:
            raise Exception(
                "Invalid processor type provided. The only types available are: BATCH and SIMPLE"
            )