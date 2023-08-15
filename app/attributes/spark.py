# Class that will be used to define the attributes that will be used to monitorize the
# Spark operations

from typing import Union
from typing import Optional

from opentelemetry.trace import Status
from opentelemetry.trace import StatusCode
from opentelemetry.sdk.trace import Tracer

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.rdd import RDD

from app.client.service import ServiceSpan

class SparkObservability:
    """
    Class that will handle all the attributes that will be used to monitor any Spark
    tasks running on our platform.

    These can be not only attributes related with the objects that are being targeted 
    on the multiple operations performed, but it can also be attributes related with
    the Spark operations.

    :param object_span: Span that will be used to monitorize the object that is being targeted
    :param tracer_id: The id of the tracer where the span is settled
    :param service_id: The id of the service that is under monitorization
    """

    def __init__(
            self,
            object_span: Union[DataFrame, RDD, SparkSession],
            tracer: Tracer,
            service_id: str,
            var_id: str = None
        ):
        """
        Handler/Initialization function that can be seen as the heart of the operation.

        This function will be responsible for setting up the attributes that will be used throuhgout
        a first analysis to the type of object_span received. After that, it will call the functions
        that will be responsible for setting up the observability attributes related with the 
        object_span.
        
        :param object_span: the object that will submitted to the monitorization process (can be
        a dataframe, a RDD, a SparkSession, etc.)
        :param tracer: on top of which the spans will be created
        :param service_id: the id of the service where this object is being used
        :param var_id: the id of the variable that is under monitorization
        """

        self._service_id = service_id
        self._tracer = tracer
        self._object_span = object_span
        self._var_id = var_id

        is_not_null = lambda x: x is not None

        # if the object_span is a dataframe, then call the df related functions
        if isinstance(object_span, DataFrame):
            span_id = '.'.join(filter(
                is_not_null, 
                [f'{self._service_id}', 'df', f'{self._var_id}']
            ))
            self._df_features(object_span, span_id)

        # if the object_span is a SparkSession, then call the spark_session related functions
        elif isinstance(object_span, SparkSession):
            span_id = '.'.join(filter(
                is_not_null, 
                [f'{self._service_id}', 'SparkSession', f'{self._var_id}']
            ))
            self._ss_specs(object_span, span_id)

        # if the object_span is a RDD, then call the rdd related functions
        elif isinstance(object_span, RDD):
            span_id = '.'.join(filter(
                is_not_null, 
                [f'{self._service_id}', 'rdd', f'{self._var_id}']
            ))
            self._rdd_features(object_span, span_id)

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

    def _df_features(self, df: DataFrame, span_id: str):
        """
        Get the columns of a dataframe.

        After retriving the columns from the dataframe provided, the function will set
        the columns as attributes of the current_span.

        :param df: the dataframe that will be used to retrieve the columns.
        :param span_id: the id of the span that will be used to set the attributes.
        """

        with self._tracer.start_as_current_span(name=span_id) as span:

            # TODO. add more attributes related with the dataframe
            attributes = [
                {'columns': df.columns},
                {'columns_count': len(df.columns)},
                {'records_count': df.count()},
            ]
            ServiceSpan.set_attributes(span, attributes)

            # TODO. status seems to make more sense on requests, not on attributes
            # span.set_span_status(Status(StatusCode.OK))

    def _ss_specs(self, ss: SparkSession, span_id: str):
        """
        Get all the configuration specs that have to do with the Spark session that is being used
        on all the spark operations.

        :param ss: the SparkSession that will be used to retrieve the version.
        """

        with self._tracer.start_as_current_span(name=span_id) as span:

            attributes = [{conf: val} for conf, val in ss.sparkContext.getConf().getAll()]
            ServiceSpan.set_attributes(span, attributes)

            # TODO. status seems to make more sense on requests, not on attributes
            # span.set_span_status(Status(StatusCode.OK))


    def _rdd_features(self, rdd: RDD, span_id: str):
        """
        Return RDD related attributes.

        :param rdd: the RDD that will be used to retrieve the attributes.
        """

        with self._tracer.start_as_current_span(name=span_id) as span:
            
            # TODO. add more attributes related with the RDD
            attributes = [
                {'id': rdd.name()},
                {'count': rdd.count()},
                {'partitions': rdd.getNumPartitions()}
            ]
            ServiceSpan.set_attributes(span, attributes)

            # TODO. status seems to make more sense on requests, not on attributes
            # span.set_span_status(Status(StatusCode.OK))