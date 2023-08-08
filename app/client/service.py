# Class that will be used to grant the monitorization of the services
# leveraged by the pipelines currently running on the platform targeted.

from params.span import SpanStatus, SpanKind

class ServiceSpan:
    def __init__(self) -> None:
        """
        Initialize the ServicesSpan class.
        """

        pass

    def set_attributes(self, attributes: dict) -> None:
        """
        Set the attributes of the services span.

        If there's any attributes that we want the service span to take into account, e.g.
        (quote consumed, jobs running, RAM under usage, etc.), this is the function that we
        should use to set them.

        :param attributes: dictionary containing the name of the attribute (key) and the value
        :return: TBD
        """

        pass

    def get_current_span(self) -> None:
        """
        Retrieves the observability span under usage.

        :param arg1: TBD
        :return: TBD
        """

        pass

    def add_events(self) -> None:
        """
        Add events to the span.

        :param arg1: TBD
        :return: TBD
        """

        pass

    def get_span_context(self) -> None:
        """
        Gets the context of the span under usage.

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

    def set_span_status(self, span_status: str = SpanStatus.unset) -> None:
        """
        Set the status of the span.

        A Span can have three different statuts: OK, ERROR, and UNSET. This function will be
        responsible for setting the status to one the three possible values.
        This function will set the default status to be UNSET.

        :param span_status: the status of the span
        :return: TBD
        """

        pass

    def set_kind(self, span_kind: str = SpanKind.client) -> None:
        """
        Set the kind of the span.

        A Span can be of many kinds: Client, Server, Internal, Producer, and/or Consumer.
        This function will set the default kind to be Client.

        :param span_kind: the kind of the span
        :return: TBD
        """

        pass

    def create_child_span(self) -> None:
        """
        Create a child span.

        Span can be have parent-child relationships. This function will be used to create a
        child span, from a provided parent span.

        :param arg1: TBD
        :return: TBD
        """

        pass