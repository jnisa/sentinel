# Class that will be used to grant the monitorization of the services
# leveraged by the pipelines currently running on the platform targeted.

from typing import List
from typing import Dict

from opentelemetry.trace import Status
from opentelemetry.trace import StatusCode
from opentelemetry.sdk.trace import Span


# TODO. complete the documentation of this class
class ServiceSpan:
    """
    Class that is a wrapper of the actions that can be performed on a span.

    TODO. As future work we can add more methods to this class, e.g.:
    - create_child_span - it's possible to have parent-child relationships between spans;
    - _create_link - it's possible to establish connections between spans, however this is not
    the same as the parent-child relationship;
    - get_span_context - function to the retrieve the span context which would then be used to
    create the links between spans;

    In case we want to use these methods in the future, we can go this commit:
    https://github.com/jnisa/sentinel/blob/0a8ceeaa6c773e2ff37541ce8b9e5bf90a98ea98/app/client/service.py
    where the structure is already implemented.
    """


    # TODO. ajust the documentation of this method
    # TODO. check if there's a way to set the attributes of the span in a more efficient way.
    def set_attributes(current_span: Span, attributes: List[Dict]):
        """
        Set the attributes of the services.

        If there's any attributes that we want the service span to take into account, e.g.
        (quote consumed, jobs running, RAM under usage, etc.), this is the function that we
        should use to set them.

        An example of an input to this function would be:
            >>> ServiceSpan.set_attributes(
            >>>     current_span = databricks_span,
            >>>     attributes = [{'quote_consumed': 100}, {'jobs_running': 10}, {'ram_under_usage': 50}]
            >>> )

        :param current_span: span that we want to add the attributes to
        :param attributes: dictionary containing the name of the attribute (key) and the value
        """

        for attribute in attributes:
            for key, value in attribute.items():
                current_span.set_attribute(key, value)

    def add_events(current_span: Span, events: List[str]):
        """
        Add events to the span.

        Events are human-readable text that can represent that "something happened" during its
        lifetime. All in all, events can be seen a primitive type of log.

        :param current_span: span that we want to add the events to
        :param events: list of events that we want to add to the span
        """

        for event in events:
            current_span.add_event(event)