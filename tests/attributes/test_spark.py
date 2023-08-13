# Unit Tests to the Spark Attributes

from unittest import TestCase
from unittest.mock import patch 
from unittest.mock import MagicMock
from unittest.mock import Mock

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.rdd import RDD

from opentelemetry.trace.status import Status
from opentelemetry.trace.status import StatusCode

# from app.attributes.spark import spark_observability

# class TestSparkAttributes(TestCase):

    # def test_spark_obs_handler(self):
    #     spark = SparkSession.builder.getOrCreate()
    #     df = spark.createDataFrame(
    #         [
    #             (1, "foo"),
    #             (2, "bar"),
    #         ],
    #         ["id", "label"]
    #     )

    #     tracer_id = 'test_tracer'
    #     service_id = 'test_service'
    #     spark_obs = spark_observability(df, tracer_id, service_id)

    # @patch('app.client.pipeline.PipelineTracer')
    # def test_get_df_columns_basic(self, mock_pipeline_tracer):

    #     spark = SparkSession.builder.getOrCreate()
    #     df = spark.createDataFrame(
    #         [
    #             (1, "foo"),
    #             (2, "bar"),
    #         ],
    #         ["id", "label"]
    #     )

    #     tracer_id = 'test_tracer'
    #     service_id = 'test_service'
    #     spark_obs = spark_observability(df, tracer_id, service_id)

    #     mock_pipeline_tracer.assert_called_once_with(tracer_id)
    #     mock_pipeline_tracer.return_value.get_tracer.assert_called_once_with()

    #     mock_tracer = mock_pipeline_tracer.return_value.get_tracer.return_value
    #     mock_tracer.add_event.assert_called_once_with(
    #         'get_df_columns',
    #         {
    #             'columns': ['id', 'label'],
    #             'columns_count': 2
    #         }
    #     )

    # def test_get_df_columns_complex(self):
    #     pass

    # def test_df_rows_count_basic(self):
    #     pass

    # def test_df_rows_count_complex(self):
    #     pass


    # def test_get_df_columns(self):

    #     # spark = SparkSession.builder.getOrCreate()
    #     # df = spark.createDataFrame(
    #     #     [
    #     #         (1, "foo"),
    #     #         (2, "bar"),
    #     #     ],
    #     #     ["id", "label"]
    #     # )


    #     mock_df = Mock(spec=DataFrame)
    #     tracer_id = "test_tracer"
    #     service_id = "test_service"

    #     with patch("app.client.ServiceSpan") as mock_service_span:
    #         spark_observability(mock_df, tracer_id, service_id)

    #         mock_service_span.assert_called_with(tracer_id, f'{service_id}.df_columns')
    #         mock_service_span.return_value.set_attributes.assert_called_once()
    #         mock_service_span.return_value.set_span_status.assert_called_once()
