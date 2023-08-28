# Unit Tests to the Spark Attributes

from unittest import TestCase
from unittest.mock import MagicMock

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.rdd import RDD

from opentelemetry.sdk.trace import Span

from telescope.client.spark.resources import TelescopeSparkResources

from opentelemetry.sdk.trace import Tracer


class TestTelescopeSparkResources(TestCase):

    def test_spark_observability__init__(self):
            
        object_span = 'test_str'

        mock_span = MagicMock(spec=Span)
        mock_tracer = MagicMock(spec=Tracer)
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span

        service_id = 'test_service_id'

        with self.assertRaises(Exception) as context:
            TelescopeSparkResources(object_span, mock_tracer, service_id)

        self.assertIn('The object_span is not a valid object type', str(context.exception))

    def test_service_id_property(self):

        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame([(1, 2, 3)], ['a', 'b', 'c'])

        mock_span = MagicMock(spec=Span)
        mock_tracer = MagicMock(spec=Tracer)
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span

        service_id = 'test_service_id'
        observability = TelescopeSparkResources(mock_tracer, service_id, df)

        actual = observability.service_id
        expected = service_id

        self.assertEqual(actual, expected)

    def test_object_type_property(self):
        
        spark = SparkSession.builder.getOrCreate()

        mock_span = MagicMock(spec=Span)
        mock_tracer = MagicMock(spec=Tracer)
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span

        service_id = 'test_service_id'
        observability = TelescopeSparkResources(mock_tracer, service_id, spark)

        actual = observability.object_type
        expected = type(spark)

        self.assertEqual(actual, expected)

    def test__df_attributes(self):

        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame([(1, 2, 3)], ['a', 'b', 'c'])

        mock_span = MagicMock(spec=Span)
        mock_tracer = MagicMock(spec=Tracer)
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span

        service_id = 'test_service_id'
        observability = TelescopeSparkResources(mock_tracer, service_id, df, 'df_test')

        span_id = f'{service_id}.df.df_test'
        observability._df_attributes(span_id)

        # stop the spark session
        spark.stop()

        # check if the attributes are set
        actual = mock_span.set_attributes.call_args_list[0][0][0]
        expected = {'columns': ['a', 'b', 'c'], 'count': 1}

        self.assertEqual(actual, expected)

    def test__ss_attributes(self):

        mock_spark_session = MagicMock(spec=SparkSession)

        mock_spark_conf = MagicMock()
        mock_spark_conf.getAll.return_value = [('spark.app.name', 'TestApp'), ('spark.executor.cores', '2')]

        mock_spark_context = MagicMock()
        mock_spark_context.getConf.return_value = mock_spark_conf

        mock_spark_session.sparkContext = mock_spark_context

        mock_span = MagicMock(spec=Span)
        mock_span.set_span_status = MagicMock()
        mock_tracer = MagicMock(spec=Tracer)
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span

        service_id = 'test_service_id'
        observability = TelescopeSparkResources(mock_tracer, service_id, mock_spark_session, 'test_session')

        span_id = f'{service_id}.SparkSession.test_session'
        observability._ss_attributes(span_id)

        # check if the attributes are set
        actual = [arg for args, _ in mock_span.set_attributes.call_args_list for arg in args]
        expected = {
            'spark.app.name': 'TestApp',
            'spark.executor.cores': '2'
        }

        self.assertEqual(actual[0], expected)

    def test__rdd_attributes(self):
            
        spark = SparkContext('local', 'test_rdd')
        rdd = spark.parallelize([(1, 2, 3), (4, 5, 6), (7, 8, 9)])

        mock_span = MagicMock(spec=Span)
        mock_tracer = MagicMock(spec=Tracer)
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span

        service_id = 'test_service_id'
        observability = TelescopeSparkResources(mock_tracer, service_id, rdd, 'rdd_test')

        span_id = f'{service_id}.rdd.test'
        observability._rdd_attributes(span_id)

        # stop the spark session
        spark.stop()

        # check if the attributes are set
        actual = [arg for args, _ in mock_span.set_attributes.call_args_list for arg in args]
        expected = {'count': 3, 'partitions': 1}

        self.assertEqual(actual[0], expected)
