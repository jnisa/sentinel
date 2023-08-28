# Unit Tests to the Spark Operations

from unittest import TestCase
from unittest.mock import MagicMock

from pyspark import SparkContext
from pyspark.sql import SparkSession

from opentelemetry.trace.status import Status
from opentelemetry.trace.status import StatusCode

from app.client.spark.operations import TelescopeSparkOperations

from opentelemetry.sdk.trace import Span
from opentelemetry.sdk.trace import Tracer


class TestTelescopeSparkOperations(TestCase):

    def test_spark_observability__init__(self):

        service_id = 'test_service_id'

        mock_span = MagicMock(spec=Span)
        mock_span.set_span_status = MagicMock()
        mock_tracer = MagicMock(spec=Tracer)
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span

        observability = TelescopeSparkOperations(mock_tracer, service_id)

        self.assertEqual(service_id, observability._service_id)

    def test_service_id_property(self):

        service_id = 'test_service_id'

        mock_span = MagicMock(spec=Span)
        mock_span.set_span_status = MagicMock()
        mock_tracer = MagicMock(spec=Tracer)
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span

        observability = TelescopeSparkOperations(mock_tracer, service_id)

        expected = service_id
        actual = observability.service_id

        self.assertEqual(actual, expected)

    def test_df_operation(self):
        service_id = 'test_service_id'

        spark = SparkSession.builder.getOrCreate()
        df1 = spark.createDataFrame([(1, 'Bear'), (2, 'John')], ['ID', 'FirstName'])
        df2 = spark.createDataFrame([(1, 'Grylls'), (2, 'Cavanagh')], ['ID', 'LastName'])

        mock_span = MagicMock(spec=Span)
        mock_span.set_span_status = MagicMock()
        mock_tracer = MagicMock(spec=Tracer)
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span

        telescope_operations = TelescopeSparkOperations(mock_tracer, service_id)

        @telescope_operations.df_operation('test_inner_join')
        def inner_join_test(df1, df2):
            return df1.join(df2, df1.ID == df2.ID, 'inner')
        
        inner_join_test(df1, df2)  # Call the decorated function

        # stop the spark session
        spark.stop()

        # check if the attributes are set
        expected = [
            ({
                'df': 'df1', 
                'columns': ['ID', 'FirstName'], 
                'count': 2, 
                'dtypes': [('ID', 'bigint'), ('FirstName', 'string')]
            }), 
            ({
                'df': 'df2', 
                'columns': ['ID', 'LastName'], 
                'count': 2, 
                'dtypes': [('ID', 'bigint'), ('LastName', 'string')]
            }), 
            ({
                'df': 'df_result', 
                'columns': ['ID', 'FirstName', 'ID', 'LastName'], 
                'count': 2, 
                'dtypes': [('ID', 'bigint'), ('FirstName', 'string'), ('ID', 'bigint'), ('LastName', 'string')]
            })
        ]

        actual = []
        for call_args in mock_span.set_attributes.call_args_list:
            actual.extend(call_args[0])

        self.assertEqual(actual, expected)

    def test_rdd_operation(self):
        service_id = 'test_service_id'

        spark = SparkContext('local', 'test_rdd')
        rdd = spark.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9])

        mock_span = MagicMock(spec=Span)
        mock_span.set_span_status = MagicMock()
        mock_tracer = MagicMock(spec=Tracer)
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span

        telescope_operations = TelescopeSparkOperations(mock_tracer, service_id)

        @telescope_operations.rdd_operation('test_even_filter')
        def is_even(rdd):
            return rdd.filter(lambda x: x % 2 == 0)

        is_even(rdd)  # Call the decorated function

        # stop the spark session
        spark.stop()

        # check if the attributes are set
        expected = [
            {'rdd': 'rdd1', 'count': 9, 'partitions': 1}, 
            {'rdd': 'rdd_result', 'count': 4, 'partitions': 1} 
        ]

        actual = []
        for call_args in mock_span.set_attributes.call_args_list:
            actual.extend(call_args[0])

        self.assertEqual(actual, expected)
