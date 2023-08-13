# Class that will be used to define the attributes that will be used to monitorize the
# Spark operations

from typing import Optional
from typing import List, Dict

from app.client.pipeline import PipelineTracer
from app.client.service import ServiceSpan

from opentelemetry.trace import Status
from opentelemetry.trace import StatusCode

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.rdd import RDD


class SparkObs:
    """
    Class that will handle all the attributes that will be used to monitor any Spark
    tasks running on our platform.

    These can be not only attributes related with the objects that are being targeted 
    on the multiple operations performed, but it can also be attributes related with
    the Spark cluster itself, e.g. (number of nodes, number of cores, etc.).
    """

    tracer = None
    service_id = None

    def __init__(self, object_span, tracer_id: str, service_id: str) -> None:
        """
        Initialize the SparkAttributes class.
        
        :param object_span: the object that will submitted to the monitorization process (can be
        a dataframe, a RDD, a SparkSession, etc.)
        :param tracer_id: the tracer that is allocated to the pipeline (check the documentation on
        the hierarchy established)
        :param service_id: the service that is being monitored
        """

        SparkObs.tracer = PipelineTracer(tracer_id).get_tracer()
        SparkObs.service_id = service_id

        # setup the attributes of the class
        self._service_id = service_id
        self._object_span = object_span
        self._tracer_id = tracer_id

        # if the object_span is a dataframe, then call the df related functions
        if isinstance(object_span, DataFrame):
            # TODO. add some events here
            self._get_df_columns(object_span)
            self._df_rows_count(object_span)

        # if the object_span is a SparkSession, then call the spark_session related functions
        elif isinstance(object_span, SparkSession):
            # TODO. add some events here
            # TODO. replace this pass by the functions that are meant to be called
            pass

        # if the object_span is a RDD, then call the rdd related functions
        elif isinstance(object_span, RDD):
            # TODO. add some events here
            # TODO. replace this pass by the functions that are meant to be called
            pass

        else:
            raise Exception('The object_span is not a valid object to be monitored.')

    @property
    def object_span(self) -> Optional[str]:
        """
        Retrieve the object_span under usage.
        """

        return self._object_span

    @property
    def service_id(self) -> Optional[str]:
        """
        Retrieve the service_id under usage
        """

        return self._service_id

    @tracer.start_as_current_span(name=f'{service_id}.df_columns')
    def _get_df_columns(self, df: DataFrame) -> List[Dict]:
        """
        Get the columns of a dataframe.

        After retriving the columns from the dataframe provided, the function will set
        the columns as attributes of the current_span.

        :param df: the dataframe that will be used to retrieve the columns.
        """

        span_id = f'{self.service_id}.df_columns'
        current_span = ServiceSpan(self._tracer_id, span_id)

        try:
            attributes = [
                {'columns': df.columns},
                {'columns_count': len(df.columns)}
            ]
            current_span.set_attributes(attributes)
            current_span.set_span_status(Status(StatusCode.OK))
        except:
            current_span.set_span_status(Status(StatusCode.ERROR))

    @tracer.start_as_current_span(name=f'{service_id}.df_rows_count')
    def _df_rows_count(self, df: DataFrame) -> List[Dict]:
        """
        Determine the number of rows of a dataframe.

        After determining the number of rows of the dataframe provided, the function will
        set the number of rows as attributes of the current_span.

        :param df: the dataframe that will be used to determine the number of rows.
        :return an integer that represents the number of records that a pyspark dataframe contains.
        """

        span_id = f'{self.service_id}.df_rows_count'
        current_span = tracer.get_current_span()
        
        try:
            attributes = [
                {'records_number': df.count()}
            ]
            current_span.set_attributes(attributes)
            current_span.set_span_status(Status(StatusCode.OK))
        except:
            current_span.set_span_status(Status(StatusCode.ERROR))

    # TODO. add more attributes related with the SparkSession
    # TODO. add more atributes related with the RDD