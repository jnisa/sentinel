# Unit Test to the ServiceSpan

from unittest import TestCase
from unittest import mock
from unittest.mock import Mock
from unittest.mock import patch
from unittest.mock import MagicMock

from opentelemetry.sdk.trace import Span

from app.client.service import ServiceSpan

class TestServiceSpan(TestCase):

    @patch('app.client.service.ServiceSpan')
    def test_set_attributes(self, mock_service_span):

        attributes = [
            {'quote_consumed': 100},
            {'jobs_running': 10}
        ]

        mock_span_instance = Mock(spec=Span)
        mock_service_span.return_value = mock_span_instance

        service_span = ServiceSpan("test_tracer_id", mock_span_instance)
        service_span.set_attributes(attributes)

        # Verify that the set_attribute method was called with the expected arguments
        expected_calls = [
            mock.call.set_attribute('quote_consumed', 100),
            mock.call.set_attribute('jobs_running', 10)
        ]

        self.assertEqual(mock_span_instance.mock_calls, expected_calls)
    
    @patch('app.client.service.ServiceSpan')
    def test_add_events_basic(self, mock_service_span):
            
            events = [
                'test_event_1',
                'test_event_2'
            ]
    
            mock_span_instance = Mock(spec=Span)
            mock_service_span.return_value = mock_span_instance
    
            service_span = ServiceSpan("test_tracer_id", mock_span_instance)
            service_span.add_events(events)
    
            # Verify that the add_event method was called with the expected arguments
            expected_calls = [
                mock.call.add_event('test_event_1'),
                mock.call.add_event('test_event_2')
            ]
    
            self.assertEqual(mock_span_instance.mock_calls, expected_calls)
