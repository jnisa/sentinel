# Class that will be used to grant the monitorization of the services
# leveraged by the pipelines currently running on the platform targeted.


class ServicesSpan:
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
        Retrieves the span under usage.

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