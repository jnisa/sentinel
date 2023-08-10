# Class that will be used to grant the monitorization of the services
# leveraged by the pipelines currently running on the platform targeted.

from typing import List

from ..constants.span import SpanStatus
from ..constants.span import SpanKind

# TODO. replace the current_span variable with a span context variable.

class ServiceSpan:
    """
    TO BE DEFINED.
    """

    def __init__(self, tracer_id: str, span_id: str):
        """
        Initialize the ServicesSpan class.

        :param tracer_id: The id of the tracer that in which all the spans will be created.

        """

        self._tracer_id = tracer_id
        self._span_id = span_id

        # create a span

    def _create_span(self) -> None:
        """
        Create a span.

        :param arg1: TBD
        :return: TBD
        """

        pass

    # OVERALL DONE
    # TODO. check if there's a way to set the attributes of the span in a more efficient way.
    # TODO. replace the current_span variable with a span context variable.
    def set_attributes(self, attributes: list) -> None:
        """
        Set the attributes of the services span.

        If there's any attributes that we want the service span to take into account, e.g.
        (quote consumed, jobs running, RAM under usage, etc.), this is the function that we
        should use to set them.

        An example of an input to this function would be:
        input: [{'quote_consumed': 100}, {'jobs_running': 10}, {'ram_under_usage': 50}]

        :param attributes: dictionary containing the name of the attribute (key) and the value
        :return: TBD
        """

        for attribute in attributes:
            for key, value in attribute.items():
                current_span.set_attribute(key, value)

    # OVERALL DONE
    # TODO. establish what will be the data type of the variable returned
    # TODO. replace the current_span variable with a span context variable.
    def add_events(self, events: List[str]):
        """
        Add events to the span.

        Events are human-readable text that can represent "something happened" during its lifetime.
        All in all, events  can be seen a primitive type of log.

        :param events: list of events that we want to add to the span
        :return: TBD
        """

        current_span.add_event(*events)

    # TODO. this function needs to receive the trace
    # TODO. reflect if this is function is really needed since we have the method built-in in the 
    # context of opentelemetry.
    def get_current_span(self) -> None:
        """
        Retrieves the observability span under usage.

        :param arg1: TBD
        :return: TBD
        """

        pass


    # TODO. this function needs to receive the trace
    # TODO. similarly to what happens with the function above, think if this function is really needed
    def get_span_context(self) -> None:
        """
        Gets the context of the span under usage.

        :param arg1: TBD
        :return: TBD
        """

        pass

    # OVERALL DONE
    # TODO. establish what will be the data type of the variable returned
    def set_span_status(self, span_status: str = 'UNSET') -> None:
        """
        Set the status of the span.

        A Span can have three different statuts: OK, ERROR, and UNSET. Typically the status
        is tipycally used to specify that a span has not completed successfully - resulting
        in an ERROR.
        The default status value is set to UNSET.

        :param span_status: the status of the span
        :return: TBD
        """

        try:
            return current_span.set_status(getattr(SpanStatus, span_status))
        except:
            raise Exception(
                "Invalid status type provided. Check the documentation for the supported types."
            )

    # OVERALL DONE
    # TODO. establish what will be the data type of the variable returned
    def set_kind(self, span_kind: str = 'INTERNAL') -> None:
        """
        Set the kind of the span.

        A Span can be of many kinds: Client, Server, Internal, Producer, and/or Consumer.
        This function will set the default kind to be Client.

        :param span_kind: the kind of the span
        :return: TBD
        """

        try:
            return current_span.set_kind(getattr(SpanKind, span_kind))
        except:
            raise Exception(
                "Invalid kind type provided. Check the documentation for the supported types."
            )

    def create_child_span(self) -> None:
        """
        Create a child span.

        Span can be have parent-child relationships. This function will be used to create a
        child span, from a provided parent span.

        :param arg1: TBD
        :return: TBD
        """

        pass

    # TODO. one of the arguments should be the span context of the span that we want to link to.
    def _create_link(self) -> None:
        """
        Establish a link between two observability spans.

        This function requires the trace link context of the span that we want to link to. Step
        that can be done by using get_span_context function followed by the creation of the link.

        :param arg1: TBD
        :return: TBD
        """

        pass