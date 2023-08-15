# Class that will be used to grant the monitorization of pipelines
# currently running on the platform targeted by this project.

from typing import Optional

from opentelemetry import trace
from opentelemetry.sdk.trace import Tracer
from opentelemetry.sdk.trace import TracerProvider

from app.constants.tracer import TracerProcessorType
from app.constants.tracer import TracerExporterType


class PipelineTracer:
    """
    Any kind of pipeline that is running on the platform targeted by this project will be
    monitored by this class.

    There are multiple monitorization hierarchies targeted by this project and this is
    the first level of monitorization. Is the one that aggregates the remaining ones - these
    being: service and process.
    """

    def __init__(
            self, 
            tracer_id: str, 
            processor_type: str = 'BATCH', 
            exporter_type: str = 'CONSOLE'
        ) -> Tracer:
        """
        Initialize the PipelineTracer class.

        :param tracer_id: The id of the tracer that will be used to monitorize the pipeline.
        :param processor_type: The type of processor that will be used by the tracer.
        :param exporter_type: The type of exporter that will be used by the tracer.
        """

        self._tracer_id = tracer_id
        self._processor_type = processor_type
        self._exporter_type = exporter_type

        provider = TracerProvider()
        processor = self._create_processor(self._set_exporter_type())
        provider.add_span_processor(processor)

        trace.set_tracer_provider(provider)
        self.baseline_trace = trace

    @property
    def tracer_id(self) -> Optional[str]:
        """
        Retrieve the tracer_id under usage.
        """

        return self._tracer_id
    
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

    def get_tracer(self) -> Tracer:
        """
        Get a opentelemetry tracer.

        From the global tracer_provider - that is created by this class, taking into acccount
        the configuration features provided by the user - we can create a tracer that will
        be used to monitorize the pipeline with the tracer_id provided.

        In case we want to retrieve an existing tracer - instead of creating a new one - we
        should use this function. Also, if for any reason we want to modify one of the
        configuration features of the tracer, we can use this function to get access to the
        that specific tracer and then modify it.

        :return: a tracer configured according the arguments provided to this class
        """

        return self.baseline_trace.get_tracer(self._tracer_id)
    
    def _set_exporter_type(self) -> TracerExporterType:
        """
        Sets the type of exporter that the tracer will use.

        Any tracer that is created by this class will have an exporter attached to it. This
        exporter will be used to send the data that is being collected by the tracer to a
        specific location. This function will be used to set the type of exporter that the
        tracer will use.

        :return: the exporter type configured according the arguments provided to this class
        """

        try:
            return getattr(TracerExporterType, self._exporter_type)
        except:
            raise Exception(
                "Invalid exporter type provided. The only types available are: CONSOLE and MEMORY"
            )  

    def _create_processor(self, exporter: TracerExporterType) -> TracerProcessorType:
        """
        Attach a processor to the tracer.

        The processor will be used to process the data that is being collected by the tracer.
        This function will be used to create a processor and attach it to the tracer.
        
        :param exporter: the exporter that will be used by the processor
        :return: a processor configured according the arguments provided to this class
        """

        try:
            return getattr(TracerProcessorType, self._processor_type)(exporter)
        except:
            raise Exception(
                "Invalid processor type provided. The only types available are: BATCH and SIMPLE"
            )