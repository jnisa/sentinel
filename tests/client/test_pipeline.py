# Unit Test to the PipelineTracer

from unittest import TestCase
from unittest.mock import patch, MagicMock

from opentelemetry.sdk.trace import Tracer
from opentelemetry.sdk.trace import TracerProvider

from app.client.pipeline import PipelineTracer

from app.constants.tracer import TracerExporterType
from app.constants.tracer import TracerProcessorType

class TestPipelineTracer(TestCase):

    def test_pipeline_tracer__init__(self):

        processor_type = "BATCH"
        exporter_type = "CONSOLE"

        pipeline_tracer = PipelineTracer(processor_type, exporter_type)

        actual = [pipeline_tracer._processor_type, pipeline_tracer._exporter_type]
        expected = ['BATCH', 'CONSOLE']

        self.assertEqual(actual, expected)

    def test_processor_type_property(self):
        
        pipeline_tracer = PipelineTracer()

        expected = TracerProcessorType.BATCH
        actual = pipeline_tracer.processor_type

        self.assertEqual(actual, expected)

    def test_exporter_type_property(self):

        pipeline_tracer = PipelineTracer()

        expected = TracerExporterType.CONSOLE
        actual = pipeline_tracer.exporter_type

        self.assertEqual(actual, expected)

    def test_get_tracer(self):
        
        tracer_id = "test_tracer_id"

        expected = type(TracerProvider().get_tracer(__name__))
        actual = type(PipelineTracer().get_tracer(tracer_id))

        self.assertEqual(actual, expected)

    def test__set_exporter_type_basic(self):
        
        processor_type = "BATCH"
        exporter_type = "CONSOLE"

        pipeline_tracer = PipelineTracer(processor_type, exporter_type)

        expected = type(TracerExporterType.CONSOLE())
        actual = type(pipeline_tracer._set_exporter_type())

        self.assertEqual(actual, expected)

    def test__set_exporter_type_complex(self):
        
        processor_type = "BATCH"
        exporter_type = "TEST_EXPORTER"

        with self.assertRaises(Exception) as context:
            PipelineTracer(processor_type, exporter_type)

        self.assertIn("Invalid exporter type", str(context.exception))

    @patch("app.client.pipeline.TracerProvider")
    def test__create_processor_basic(self, mock_tracer_provider):

        processor_type = "SIMPLE"
        exporter_type = "CONSOLE"

        mock_provider_instance = mock_tracer_provider.return_value
        mock_provider_instance.add_span_processor.return_value = MagicMock()

        pipeline_tracer = PipelineTracer(processor_type, exporter_type)

        expected = TracerProcessorType.SIMPLE  # Class type
        actual = type(pipeline_tracer._create_processor(getattr(TracerExporterType, exporter_type)))

        self.assertEqual(actual, expected)

    @patch("app.client.pipeline.TracerProvider")
    def test__create_processor_complex(self, mock_tracer_provider):

        processor_type = "TEST_PROCESSOR"
        exporter_type = "CONSOLE"

        mock_provider_instance = mock_tracer_provider.return_value
        mock_provider_instance.add_span_processor.return_value = MagicMock()        

        with self.assertRaises(Exception) as context:
            PipelineTracer(processor_type, exporter_type)

        self.assertIn("Invalid processor type", str(context.exception))