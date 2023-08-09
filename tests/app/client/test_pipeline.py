# Unit Test to the PipelineTracer

from unittest import TestCase
from unittest import mock
# from unittest.mock import patch, MagicMock

from app.client.pipeline import PipelineTracer

class TestPipelineTracer(TestCase):

    @mock.patch("app.client.pipeline.TracerProvider")
    def test_pipeline_tracer__init__(self, mock_tracer_provider):

        tracer_id = "test_tracer_id"
        processor_type = "BATCH"
        exporter_type = "CONSOLE"

        pipeline_tracer = PipelineTracer(tracer_id, processor_type, exporter_type)

        mock_tracer_provider.return_value = mock.MagicMock()
        mock_tracer_provider.return_value.add_span_processor.return_value = mock.MagicMock()

        expected = "test_tracer_id"
        actual = pipeline_tracer.tracer_id

        self.assertEqual(actual, expected)

    def test_get_tracer_basic(self):
        pass

    def test_get_tracer_complex(self):
        pass

    def test__set_exporter_type_basic(self):
        pass

    def test__set_exporter_type_complex(self):
        pass

    def test__create_processor_basic(self):
        pass

    def test__create_processor_complex(self):
        pass