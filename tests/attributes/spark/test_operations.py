# Unit Tests to the Spark Operations

from unittest import TestCase
from unittest.mock import patch 
from unittest.mock import MagicMock

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.rdd import RDD

from opentelemetry.trace.status import Status
from opentelemetry.trace.status import StatusCode
from opentelemetry.sdk.trace import Span

from app.attributes.spark.operations import TelescopeSparkOperations

from opentelemetry.sdk.trace import Tracer


class TestTelescopeSparkOperations(TestCase):

    def test_spark_observability__init__(self):
            
            service_id = 'databricks'
            operation_id = 'test_join'
    
            mock_span = MagicMock(spec=Span)
            mock_span.set_span_status = MagicMock()
            mock_tracer = MagicMock(spec=Tracer)
            mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span
    
            observability = TelescopeSparkOperations(mock_tracer, service_id, operation_id)
    
            self.assertEqual(service_id, observability._service_id)
            self.assertEqual(operation_id, observability._operation_id)

    def test_service_id_property(self):

        service_id = 'databricks'
        operation_id = 'test_union'

        mock_span = MagicMock(spec=Span)
        mock_span.set_span_status = MagicMock()
        mock_tracer = MagicMock(spec=Tracer)
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span

        observability = TelescopeSparkOperations(mock_tracer, service_id, operation_id)

        expected = service_id
        actual = observability.service_id

        self.assertEqual(actual, expected)

    def test_operation_id_property(self):

        service_id = 'functionApp'
        operation_id = 'test_withColumn'

        mock_span = MagicMock(spec=Span)
        mock_span.set_span_status = MagicMock()
        mock_tracer = MagicMock(spec=Tracer)
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span

        observability = TelescopeSparkOperations(mock_tracer, service_id, operation_id)

        expected = operation_id
        actual = observability.operation_id

        self.assertEqual(actual, expected)