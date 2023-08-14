# Unit Test to the PipelineTracer

from unittest import TestCase
from unittest import mock
from unittest.mock import patch, MagicMock

from app.client.pipeline import PipelineTracer

from app.constants.tracer import TracerExporterType
from app.constants.tracer import TracerProcessorType

class TestPipelineTracer(TestCase):

    def test_pipeline_tracer__init__(self):

        tracer_id = "test_tracer_id"
        processor_type = "BATCH"
        exporter_type = "CONSOLE"

        pipeline_tracer = PipelineTracer(tracer_id, processor_type, exporter_type)

        expected = "test_tracer_id"
        actual = pipeline_tracer.tracer_id

        self.assertEqual(actual, expected)

    def test_processor_type_property(self):
        
        tracer_id = "test_tracer_id"

        pipeline_tracer = PipelineTracer(tracer_id)

        expected = TracerProcessorType.BATCH
        actual = pipeline_tracer.processor_type

        self.assertEqual(actual, expected)

    def test_exporter_type_property(self):
        
        tracer_id = "test_tracer_id"

        pipeline_tracer = PipelineTracer(tracer_id)

        expected = TracerExporterType.CONSOLE
        actual = pipeline_tracer.exporter_type

        self.assertEqual(actual, expected)

    def test_get_tracer(self):
        
        tracer_id = "test_tracer_id"

        pipeline_tracer = PipelineTracer(tracer_id)

        expected = "test_tracer_id"
        actual = pipeline_tracer.tracer_id

        self.assertEqual(actual, expected)

    def test__set_exporter_type_basic(self):
        
        trace_id = "test_trace_id"
        processor_type = "BATCH"
        exporter_type = "CONSOLE"

        pipeline_tracer = PipelineTracer(trace_id, processor_type, exporter_type)

        expected = TracerExporterType.CONSOLE
        actual = pipeline_tracer._set_exporter_type()

        self.assertEqual(actual, expected)

    def test__set_exporter_type_complex(self):
        
        trace_id = "test_trace_id"
        processor_type = "BATCH"
        exporter_type = "TEST_EXPORTER"

        with self.assertRaises(Exception) as context:
            PipelineTracer(trace_id, processor_type, exporter_type)

        self.assertIn("Invalid exporter type", str(context.exception))

    @patch("app.client.pipeline.TracerProvider")
    def test__create_processor_basic(self, mock_tracer_provider):

        trace_id = "test_trace_id"
        processor_type = "SIMPLE"
        exporter_type = "CONSOLE"

        mock_provider_instance = mock_tracer_provider.return_value
        mock_provider_instance.add_span_processor.return_value = MagicMock()

        pipeline_tracer = PipelineTracer(trace_id, processor_type, exporter_type)

        expected = TracerProcessorType.SIMPLE  # Class type
        actual = type(pipeline_tracer._create_processor(getattr(TracerExporterType, exporter_type)))

        self.assertEqual(actual, expected)

    @patch("app.client.pipeline.TracerProvider")
    def test__create_processor_complex(self, mock_tracer_provider):

        trace_id = "test_tracer_id"
        processor_type = "TEST_PROCESSOR"
        exporter_type = "CONSOLE"

        mock_provider_instance = mock_tracer_provider.return_value
        mock_provider_instance.add_span_processor.return_value = MagicMock()        

        with self.assertRaises(Exception) as context:
            PipelineTracer(trace_id, processor_type, exporter_type)

        self.assertIn("Invalid processor type", str(context.exception))