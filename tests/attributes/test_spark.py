# Unit Tests to the Spark Attributes

from unittest import TestCase
from unittest import mock
from unittest.mock import patch 
from unittest.mock import MagicMock
from unittest.mock import call

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.rdd import RDD

from opentelemetry.trace.status import Status
from opentelemetry.trace.status import StatusCode
from opentelemetry.sdk.trace import Span

from app.attributes.spark import spark_observability
from app.client.pipeline import PipelineTracer
from app.client.service import ServiceSpan

from opentelemetry.sdk.trace import Tracer


class TestSparkAttributes(TestCase):

    @patch('opentelemetry.sdk.trace.Tracer')
    def test_spark_observability_basic(self, mock_tracer_class):

        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame([(1, 2, 3)], ['a', 'b', 'c'])

        mock_span_instance = MagicMock(spec=Span)
        mock_tracer_class.start_as_current_span.return_value = mock_span_instance

        service_id = 'Databricks'
        spark_observability(df, mock_tracer_class, service_id)

        expected = [
            call.set_attributes(mock_span_instance, [{'columns': ['a', 'b', 'c']}]),
            call.set_attributes(mock_span_instance, [{'columns_count': 3}]),
            # call(spark_observability.),
            # call(),
            # call(df.colums)
        ]
        actual = mock_span_instance.mock_calls

        self.assertCountEqual(actual, expected)


    # @patch('opentelemetry.sdk.trace.Tracer')
    # def test_spark_observability_basic(self, mock_tracer_class):

    #     spark = SparkSession.builder.getOrCreate()
    #     df = spark.createDataFrame([(1, 2, 3)], ['a', 'b', 'c'])

    #     mock_span_instance = MagicMock(spec=Span)
    #     mock_span_instance.set_status.return_value = MagicMock(spec=Status)

    #     mock_enter = MagicMock()
    #     mock_enter.return_value = mock_span_instance

    #     mock_tracer_instance = mock_tracer_class.return_value
    #     mock_tracer_instance.start_as_current_span.return_value.__enter__ = mock_enter

    #     # Simulate the _df_rows_count function with a MagicMock
    #     mock_df_rows_count = MagicMock()
    #     # Set up the return value or side effects for the mocked function
    #     mock_df_rows_count.return_value = [{'records_number': 1}]

    #     # Assign the mocked function to the local namespace where it's used
    #     with patch('app.attributes.spark._df_rows_count', mock_df_rows_count):
    #         service_id = 'Databricks'
    #         spark_observability(df, mock_tracer_instance, service_id)

    #         expected = [
    #             call.set_attributes(mock_span_instance, [{'columns': ['a', 'b', 'c']}]),
    #             call.set_attributes(mock_span_instance, [{'columns_count': 3}]),
    #             call.set_attributes(mock_span_instance, [{'records_number': 1}]),  # Mocked call
    #         ]
    #         actual = mock_span_instance.mock_calls

    #         self.assertCountEqual(actual, expected)


    # TODO. test the exception case
    # def test_get_df_columns_complex(self):
    #     pass

    # def test_df_rows_count_basic(self):
    #     pass

    # def test_df_rows_count_complex(self):
    #     pass
