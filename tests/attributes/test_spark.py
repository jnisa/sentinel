# Unit Tests to the Spark Attributes

from unittest import TestCase
from unittest.mock import patch 
from unittest.mock import MagicMock

from pyspark.sql import SparkSession

from opentelemetry.trace.status import Status
from opentelemetry.trace.status import StatusCode
from opentelemetry.sdk.trace import Span

from app.attributes.spark import SparkObservability

from opentelemetry.sdk.trace import Tracer


class TestSparkObservability(TestCase):

    def test_spark_observability__init__(self):
            
            object_span = 'test_str'
    
            mock_span = MagicMock(spec=Span)
            mock_span.set_span_status = MagicMock()
            mock_tracer = MagicMock(spec=Tracer)
            mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span
    
            service_id = 'databricks'
    
            with self.assertRaises(Exception) as context:
                SparkObservability(object_span, mock_tracer, service_id)

            self.assertIn('The object_span is not a valid object type', str(context.exception))

    def test_service_id_property(self):
        
        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame([(1, 2, 3)], ['a', 'b', 'c'])

        mock_span = MagicMock(spec=Span)
        mock_span.set_span_status = MagicMock()
        mock_tracer = MagicMock(spec=Tracer)
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span

        service_id = 'databricks'
        observability = SparkObservability(df, mock_tracer, service_id)

        expected = service_id
        actual = observability.service_id

        self.assertEqual(actual, expected)

    def test_object_type_property(self):
        
        spark = SparkSession.builder.getOrCreate()

        mock_span = MagicMock(spec=Span)
        mock_span.set_span_status = MagicMock()
        mock_tracer = MagicMock(spec=Tracer)
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span

        service_id = 'databricks'
        observability = SparkObservability(spark, mock_tracer, service_id)

        expected = type(spark)
        actual = observability.object_type

        self.assertEqual(actual, expected)

    @patch('app.client.service.ServiceSpan.set_attributes')
    def test__df_attributes(self, mock_set_attributes):

        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame([(1, 2, 3)], ['a', 'b', 'c'])

        mock_span = MagicMock(spec=Span)
        mock_span.set_span_status = MagicMock()
        mock_tracer = MagicMock(spec=Tracer)
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span

        service_id = 'databricks'
        observability = SparkObservability(df, mock_tracer, service_id)

        observability._df_features(df)

        # check if the attributes are set
        expected = [
            {'columns': ['a', 'b', 'c']},
            {'columns_count': 3},
            {'records_count': 1}
        ]
        actual = mock_set_attributes.call_args_list[0][0][1]
        self.assertEqual(actual, expected)
            
        # check the status is set -  TO BE CONSIDERED
        status = mock_span.set_span_status.call_args[0][0]
        self.assertEqual(status.status_code, StatusCode.OK)

    @patch('app.client.service.ServiceSpan.set_attributes')
    def test__ss_specs(self, mock_set_attributes):

        mock_spark_session = MagicMock(spec=SparkSession)

        mock_spark_conf = MagicMock()
        mock_spark_conf.getAll.return_value = [('spark.app.name', 'MyApp'), ('spark.executor.cores', '2')]

        mock_spark_context = MagicMock()
        mock_spark_context.getConf.return_value = mock_spark_conf

        mock_spark_session.sparkContext = mock_spark_context

        mock_span = MagicMock(spec=Span)
        mock_span.set_span_status = MagicMock()
        mock_tracer = MagicMock(spec=Tracer)
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span

        service_id = 'databricks'
        observability = SparkObservability(mock_spark_session, mock_tracer, service_id)

        observability._ss_specs(mock_spark_session)

        # check if the attributes are set
        expected = [
            {'spark.app.name': 'MyApp'},
            {'spark.executor.cores': '2'}
        ]
        actual = mock_set_attributes.call_args_list[0][0][1]
        self.assertEqual(actual, expected)

        # check the status is set -  TO BE CONSIDERED
        status = mock_span.set_span_status.call_args[0][0]
        self.assertEqual(status.status_code, StatusCode.OK)
