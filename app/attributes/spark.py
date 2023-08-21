# Class that will be used to define the attributes that will be used to monitorize the
# Spark operations

from typing import Union
from typing import Optional

from opentelemetry.sdk.trace import Tracer

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.rdd import RDD

from app.client.service import ServiceSpan
from app.attributes.auxiliars import get_id

# TODO. things to be added:
# 1. pass the tracer as a parameter to the decorator;
# 2. the class should be able to handle multiple spark resources at the same time;
# 3. duly handle the configuration parameters provided (e.g. service_id, var_id, etc.)

class SparkObservability:
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

    # TODO. we could probably set the object span default value to None
    # if object_span is None:
    #     it's neither a dataframe nor a rdd nor a Spark Session
    #     this means that it was called as a decorator
    #     >> call the function for the observability of the resource provided
    # else
    #     this means that it was called as a function and the objects provided need to be monitored
    #     >> do nothing (meaning: it was called as a decorator)

    def __init__(
            self,
            tracer: Tracer,
            service_id: str,
            var_id: Optional[str] = None,
            object_span: Union[DataFrame, RDD, SparkSession] = None,
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
        :param var_id: the id of the variable that is under monitorization
        :param object_span: the object that will submitted to the monitorization process (can be
        a dataframe, a RDD, a SparkSession, etc.)
        """

        self._service_id = service_id
        self._tracer = tracer
        self._object_span = object_span
        self._var_id = var_id

        if self._object_span is not None:

            # if the object_span is a dataframe, then call the df related functions
            if isinstance(self._object_span, DataFrame):
                # TODO. not sure about this but recheck
                df_span = self.df_attributes(self._object_span)
                df_span(self._object_span)

            # if the object_span is a SparkSession, then call the spark_session related functions
            elif isinstance(self._object_span, SparkSession):
                # TODO. not sure about this but recheck
                self._ss_specs(self._object_span, get_id(self._service_id, 'SparkSession', self._var_id))

            # if the object_span is a RDD, then call the rdd related functions
            elif isinstance(self._object_span, RDD):
                # TODO. not sure about this but recheck
                self._rdd_features(self._object_span, get_id(self._service_id, 'rdd', self._var_id))

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

    # TODO. this function can be used as a decorator
    # TODO. adjsut the documentation of this function
    def df_attributes(self, arg):
        """
        Get attributes from a provided dataframe.

        :param arg: a function that will be targeted by the decorator
        """

        def wrapper(*args):

            with self._tracer.start_as_current_span(name=self._var_id) as span:

                # TODO. add more attributes related with the dataframe
                # TODO. filter the attributes - e.g. we'll not be interested in getting a count
                # everytime as it might turn out to be expensive
                for df in args:
                    
                    attributes = [
                        {'columns': df.columns},
                        {'columns_count': len(df.columns)},
                        {'records_count': df.count()},
                    ]
                    ServiceSpan.set_attributes(span, attributes)

                    # TODO. status seems to make more sense on requests, not on attributes
                    # TODO. evaluate if this operation also makes sense on the other functions
                    # span.set_span_status(Status(StatusCode.OK))

        if callable(arg):
            return wrapper(arg)
        else:
            # TODO. fix this
            return None

    def ss_attributes(self, ss: SparkSession):
        """
        Get all the configuration specs from the Spark Session used.

        :param ss: the SparkSession that will be used to retrieve the version.
        :param span_id: the id of the span that will be used to set the attributes.
        """

        def wrapper(*args):

            with self._tracer.start_as_current_span(name=self._var_id) as span:

                # TODO. filter the attributes that only add noise
                attributes = [{conf: val} for conf, val in ss.sparkContext.getConf().getAll()]
                ServiceSpan.set_attributes(span, attributes)

        return wrapper

    # TODO. this function can be used as a decorator
    def rdd_attributes(self, rdd: RDD):
        """
        Return RDD related attributes.

        :param rdd: the RDD that will be used to retrieve the attributes.
        :param span_id: the id of the span that will be used to set the attributes.
        """

        def wrapper(*args):

            with self._tracer.start_as_current_span(name=self._var_id) as span:
                
                # TODO. add more attributes related with the RDD
                attributes = [
                    {'id': rdd.name()},
                    {'count': rdd.count()},
                    {'partitions': rdd.getNumPartitions()}
                ]
                ServiceSpan.set_attributes(span, attributes)
