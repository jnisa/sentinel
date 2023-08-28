# Class that will be used to define the attributes that will be used to monitorize the
# Spark variables

from typing import Union
from typing import Optional

from opentelemetry.sdk.trace import Tracer

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.rdd import RDD

from telescope.client.auxiliars import get_id
from telescope.client.spark.attributes import get_attributes

# TODO. things to be added:
# 1. pass the tracer as a parameter to the decorator;
# 2. the class should be able to handle multiple spark resources at the same time;
# 3. duly handle the configuration parameters provided (e.g. service_id, var_id, etc.)

class TelescopeSparkResources:
    """
    Class that will handle all the attributes that will be used to monitor any Spark
    tasks running on the platform.

    These can be not only attributes related with the objects that are being targeted 
    on the multiple operations performed, but it can also be attributes related with
    the Spark operations.

    :param object_span: Span that will be used to monitorize the object that is being targeted
    :param tracer_id: The id of the tracer where the span is settled
    :param service_id: The id of the service that is under monitorization
    """

    def __init__(
            self,
            tracer: Tracer,
            service_id: str,
            object_span: Union[DataFrame, RDD, SparkSession] = None,
            var_id: Optional[str] = None,
        ):
        """
        Handler/Initialization function that can be seen as the heart of the operation.

        This function will be responsible for setting up the attributes that will be used throuhgout
        a first analysis to the type of object_span received. After that, it will call the functions
        that will be responsible for setting up the observability attributes.

        It's important to note that this class can also be used as a decorator. In this case, the
        object_span will be None and the class will be used to monitorize the resources targeted by
        the function that received the decorator, otherwise it will be used to monitorize the object
        provided.
        
        :param tracer: on top of which the spans will be created
        :param service_id: the id of the service where this object is being used
        :param object_span: the object that will submitted to the monitorization process (can be
        a dataframe, a RDD, a SparkSession, etc.)
        :param var_id: the id of the variable that is under monitorization
        """

        self._service_id = service_id
        self._tracer = tracer
        self._object_span = object_span
        self._var_id = var_id

        # TODO. replace the get_id by a join
        # if the object_span is a dataframe, then call the df related functions
        if isinstance(self._object_span, DataFrame):
            # TODO. not sure about this but recheck
            self._df_attributes(get_id(self._service_id, 'df', self._var_id))

        # if the object_span is a SparkSession, then call the spark_session related functions
        elif isinstance(self._object_span, SparkSession):
            # TODO. not sure about this but recheck
            self._ss_attributes(get_id(self._service_id, 'SparkSession', self._var_id))

        # if the object_span is a RDD, then call the rdd related functions
        elif isinstance(self._object_span, RDD):
            # TODO. not sure about this but recheck
            self._rdd_attributes(get_id(self._service_id, 'rdd', self._var_id))

        else:
            raise Exception('The object_span is not a valid object type to be monitored.')

    @property
    def service_id(self) -> Optional[str]:
        """
        Getter function for the service_id attribute.

        :return: the service_id attribute
        """

        return self._service_id
    
    @property
    def object_type(self) -> Union[DataFrame, RDD, SparkSession]:
        """
        Getter function for the object_type attribute.

        :return: the object_type attribute
        """

        return type(self._object_span)

    def _df_attributes(self, span_id: str):
        """
        Get attributes from a provided dataframe.

        :param span_id: the id of the span that will be used to set the attributes.
        """

        with self._tracer.start_as_current_span(name=span_id) as span:

            span.set_attributes(get_attributes.df(self._object_span))

    def _ss_attributes(self, span_id: str):
        """
        Get all the configuration specs from the Spark Session used.

        :param span_id: the id of the span that will be used to set the attributes.
        """

        with self._tracer.start_as_current_span(name=span_id) as span:

            span.set_attributes(get_attributes.ss(self._object_span))

    def _rdd_attributes(self, span_id: str):
        """
        Return RDD related attributes.

        :param span_id: the id of the span that will be used to set the attributes.
        """

        with self._tracer.start_as_current_span(name=span_id) as span:
            
            span.set_attributes(get_attributes.rdd(self._object_span))
