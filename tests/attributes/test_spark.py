# Unit Tests to the Spark Attributes

from unittest import TestCase
from unittest.mock import patch 
from unittest.mock import MagicMock

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.rdd import RDD

from opentelemetry.trace.status import Status
from opentelemetry.trace.status import StatusCode
from opentelemetry.sdk.trace import Span

from app.attributes.spark import SparkObservability

from opentelemetry.sdk.trace import Tracer


class TestSparkAttributes(TestCase):

    @patch('app.client.service.ServiceSpan.set_attributes')
    def test__spark_observability_df_basic(self, mock_set_attributes):

        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame([(1, 2, 3)], ['a', 'b', 'c'])

        mock_span = MagicMock(spec=Span)
        mock_span.set_span_status = MagicMock()
        mock_tracer = MagicMock(spec=Tracer)
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span

        service_id = 'databricks'
        observability = SparkObservability(df, mock_tracer, service_id)

        observability._df_columns(df)

        attr_columns = [
            {'columns': ['a', 'b', 'c']},
            {'columns_count': 3}
        ]
        attr_count = [
            {'records_number': 1}
        ]

        expected = [
            attr_columns,
            attr_count
        ]
        actual = [call_args[0][1] for call_args in mock_set_attributes.call_args_list]

        # check the attributes are set 
        for e in expected:
            self.assertIn(e, actual)
            
        # check the status is set
        status = mock_span.set_span_status.call_args[0][0]
        self.assertEqual(status.status_code, StatusCode.OK)
