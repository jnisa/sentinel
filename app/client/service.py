# Class that will be used to grant the monitorization of the services
# leveraged by the pipelines currently running on the platform targeted.

from typing import List

from opentelemetry.trace import Status
from opentelemetry.trace import StatusCode
from opentelemetry.sdk.trace import Span


# TODO. complete the documentation of this class
class ServiceSpan:
    """
    TO BE DEFINED.

    The following modules can then be added to this class if needed:
    - create_child_span;
    - _create_link;
    - get_span_context;

    In case we want to use these methods in the future, we can go this commit:
    https://github.com/jnisa/sentinel/blob/0a8ceeaa6c773e2ff37541ce8b9e5bf90a98ea98/app/client/service.py
    where the structure is already in implemented.
    """

    def __init__(self, tracer_id: str, span_id: Span):
        """
        Initialize the ServicesSpan class.

        :param tracer_id: The id of the tracer that in which all the spans will be created.
        :param span_id: The id of the span that will be used to monitorize the services.
        """

        self.tracer_id = tracer_id
        self.current_span = span_id

    # TODO. check if there's a way to set the attributes of the span in a more efficient way.
    def set_attributes(self, attributes: List[dict]):
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
                self.current_span.set_attribute(key, value)

    def add_events(self, events: List[str]):
        """
        Add events to the span.

        Events are human-readable text that can represent "something happened" during its lifetime.
        All in all, events  can be seen a primitive type of log.

        :param events: list of events that we want to add to the span
        """

        for event in events:
            self.current_span.add_event(event)
