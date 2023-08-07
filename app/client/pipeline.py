# Class that will be used to grant the monitorization of pipelines
# currently running on the platform targeted by this project.


class PipelineTracer:
    """
    Any kind of pipeline that is running on the platform targeted by this project will be
    monitored by this class.

    There are multiple monitorization hierarchies targeted by this project and this is
    the first level of monitorization. Is the one that aggregates the remaining ones - these
    being: service and process.
    """

    def __init__(self) -> None:
        """
        Initialize the PipelineWatchdog class.
        """

        pass

    def create_tracer(self) -> None:
        """
        Create a tracer to monitor the pipeline that is currently running.

        Each and every pipeline that is running on the platform should have a dedicated tracer
        i.e. the tracer of the streaming pipeline that register all the CDC data should not be
        the same as the one that is monitoring the data ingestion pipeline.

        :param arg1: TBD
        :return: TBD
        """

        pass

    def get_tracer(self) -> None:
        """
        Grab an existing tracer.

        In case we want to retrieve an existing tracer - instead of creating a new one - we
        should use this function. Also, if for any reason we want to modify one of the
        configuration features of the tracer, we can use this function to get access to the
        that specific tracer and then modify it.

        :param arg1: TBD
        :return: TBD
        """

        pass

    def _set_exporter_type(self) -> None:
        """
        Sets the type of exporter that the tracer will use.

        Any tracer that is created by this class will have an exporter attached to it. This
        exporter will be used to send the data that is being collected by the tracer to a
        specific location. This function will be used to set the type of exporter that the
        tracer will use.

        :param arg1: TBD
        :return: TBD
        """

        pass

    def _create_processor(self) -> None:
        """
        Attach a processor to the tracer.

        The processor will be used to process the data that is being collected by the tracer.
        This function will be used to create a processor and attach it to the tracer.

        :param arg1: TBD
        :return: TBD
        """

        pass
