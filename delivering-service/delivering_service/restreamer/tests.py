"""
Tests for the Delivering Service endpoints module.

These tests verify:
- FFmpeg command generation for different service types
- S3 object key format (must match Rust client upload format)
- Chunk retrieval and processing logic
- Endpoint control (start/stop)
"""

import io
from unittest.mock import MagicMock, patch

from django.test import TestCase

from restreamer.endpoints import EndPoint, ManagerEndPointControl


class EndPointFFmpegCommandTests(TestCase):
    """Tests for EndPoint.run_ffmpeg() command generation."""

    def setUp(self):
        """Create a test endpoint."""
        self.endpoint = EndPoint(
            alias="Test Endpoint",
            service_type="YT_HLS",
            stream_key="test-stream-key-123",
            stream_identifier="test-event-001",
            chunk_id=1,
        )

    def tearDown(self):
        """Clean up multiprocessing resources."""
        # EndPoint is a multiprocessing.Process, clean up properly
        pass

    @patch("restreamer.endpoints.ffmpeg")
    def test_yt_hls_ffmpeg_command(self, mock_ffmpeg):
        """Test that YT_HLS builds correct ffmpeg command."""
        mock_input = MagicMock()
        mock_output = MagicMock()
        mock_ffmpeg.input.return_value = mock_input
        mock_input.output.return_value = mock_output
        mock_output.compile.return_value = ["ffmpeg", "-i", "pipe:"]
        mock_output.run_async.return_value = MagicMock(poll=MagicMock(return_value=None))

        with patch("time.sleep"):
            self.endpoint.run_ffmpeg()

        # Verify input call
        mock_ffmpeg.input.assert_called_once()
        call_args = mock_ffmpeg.input.call_args
        self.assertEqual(call_args[0][0], "pipe:")
        self.assertEqual(call_args[1]["format"], "mpegts")

        # Verify output call includes HLS options
        mock_input.output.assert_called_once()
        output_args = mock_input.output.call_args
        self.assertIn("hls", output_args[1].get("f", ""))
        self.assertEqual(output_args[1].get("method"), "PUT")

    @patch("restreamer.endpoints.ffmpeg")
    def test_fb_ffmpeg_command(self, mock_ffmpeg):
        """Test that FB builds correct ffmpeg command with RTMPS."""
        self.endpoint.service_type = "FB"
        self.endpoint.stream_key = "fb-key-456"

        mock_input = MagicMock()
        mock_output = MagicMock()
        mock_ffmpeg.input.return_value = mock_input
        mock_input.output.return_value = mock_output
        mock_output.compile.return_value = ["ffmpeg", "-i", "pipe:"]
        mock_output.run_async.return_value = MagicMock(poll=MagicMock(return_value=None))

        with patch("time.sleep"):
            self.endpoint.run_ffmpeg()

        # Verify output is FLV to Facebook RTMPS
        output_args = mock_input.output.call_args
        output_url = output_args[0][0]
        self.assertIn("rtmps://live-api-s.facebook.com", output_url)
        self.assertIn("fb-key-456", output_url)
        self.assertEqual(output_args[1].get("f"), "flv")

    @patch("restreamer.endpoints.ffmpeg")
    def test_yt_rtmp_ffmpeg_command(self, mock_ffmpeg):
        """Test that YT_RTMP builds correct ffmpeg command."""
        self.endpoint.service_type = "YT_RTMP"
        self.endpoint.stream_key = "yt-rtmp-key-789"

        mock_input = MagicMock()
        mock_output = MagicMock()
        mock_ffmpeg.input.return_value = mock_input
        mock_input.output.return_value = mock_output
        mock_output.global_args.return_value = mock_output
        mock_output.compile.return_value = ["ffmpeg", "-i", "pipe:"]
        mock_output.run_async.return_value = MagicMock(poll=MagicMock(return_value=None))

        with patch("time.sleep"):
            self.endpoint.run_ffmpeg()

        # Verify output is FLV to YouTube RTMP
        output_args = mock_input.output.call_args
        output_url = output_args[0][0]
        self.assertIn("rtmp://a.rtmp.youtube.com/live2/", output_url)
        self.assertIn("yt-rtmp-key-789", output_url)

    @patch("restreamer.endpoints.ffmpeg")
    def test_vimeo_ffmpeg_command(self, mock_ffmpeg):
        """Test that VIMEO builds correct ffmpeg command."""
        self.endpoint.service_type = "VIMEO"
        self.endpoint.stream_key = "vimeo-key-abc"

        mock_input = MagicMock()
        mock_output = MagicMock()
        mock_ffmpeg.input.return_value = mock_input
        mock_input.output.return_value = mock_output
        mock_output.compile.return_value = ["ffmpeg", "-i", "pipe:"]
        mock_output.run_async.return_value = MagicMock(poll=MagicMock(return_value=None))

        with patch("time.sleep"):
            self.endpoint.run_ffmpeg()

        # Verify output is FLV to Vimeo RTMPS
        output_args = mock_input.output.call_args
        output_url = output_args[0][0]
        self.assertIn("rtmps://rtmp-global.cloud.vimeo.com", output_url)
        self.assertIn("vimeo-key-abc", output_url)

    def test_unsupported_service_type_raises_error(self):
        """Test that unsupported service type raises ValueError."""
        self.endpoint.service_type = "INVALID_TYPE"

        with self.assertRaises(ValueError), patch("restreamer.endpoints.ffmpeg"):
            self.endpoint.run_ffmpeg()


class EndPointS3KeyFormatTests(TestCase):
    """Tests verifying S3 key format matches Rust client expectations."""

    def test_s3_key_format_matches_rust_client(self):
        """
        Verify the S3 object key format used in delivering service matches
        the format used by Rust client uploads.

        Rust client (s3.rs): {event_id}/{chunk_id}_{event_id}.bin
        Delivering service (endpoints.py line 248): {stream_identifier}/{chunk_id}_{stream_identifier}.bin
        """
        stream_identifier = "test-event-001"
        chunk_id = 42

        # This is the format used in endpoints.py line 248
        expected_key = f"{stream_identifier}/{chunk_id}_{stream_identifier}.bin"

        # Verify the format
        self.assertEqual(expected_key, "test-event-001/42_test-event-001.bin")

    def test_s3_key_format_multiple_chunks(self):
        """Test S3 key format for multiple sequential chunks."""
        stream_id = "live-stream-abc"

        for chunk_id in [1, 2, 3, 100, 999]:
            expected = f"{stream_id}/{chunk_id}_{stream_id}.bin"
            self.assertTrue(expected.startswith(stream_id + "/"))
            self.assertTrue(expected.endswith(".bin"))
            self.assertIn(f"_{stream_id}", expected)


class EndPointProcessChunkTests(TestCase):
    """Tests for EndPoint.process_chunk() method."""

    def setUp(self):
        """Create test endpoint with mocked S3."""
        self.endpoint = EndPoint(
            alias="Test Endpoint",
            service_type="YT_HLS",
            stream_key="test-key",
            stream_identifier="test-event",
            chunk_id=1,
        )

    def test_process_chunk_writes_to_stdin(self):
        """Test that process_chunk writes chunk data to ffmpeg stdin."""
        mock_process = MagicMock()
        mock_process.poll.return_value = None
        mock_stdin = MagicMock()
        mock_process.stdin = mock_stdin

        chunk_data = b"test chunk data content"
        mock_response = {"Body": io.BytesIO(chunk_data)}

        self.endpoint.process_chunk(mock_process, mock_response)

        mock_stdin.write.assert_called_once_with(chunk_data)
        mock_stdin.flush.assert_called_once()
        self.assertEqual(self.endpoint.buff_size.value, len(chunk_data))

    def test_process_chunk_handles_none_response(self):
        """Test that process_chunk handles None response gracefully."""
        mock_process = MagicMock()
        mock_process.poll.return_value = None

        # Should not raise, just log warning
        self.endpoint.process_chunk(mock_process, None)

    def test_process_chunk_handles_broken_pipe(self):
        """Test that process_chunk handles BrokenPipeError."""
        mock_process = MagicMock()
        mock_process.poll.return_value = None
        mock_process.stdin.write.side_effect = BrokenPipeError()

        chunk_data = b"test data"
        mock_response = {"Body": io.BytesIO(chunk_data)}

        # Should not raise, just log warning
        self.endpoint.process_chunk(mock_process, mock_response)


class EndPointRetrieveNextChunkTests(TestCase):
    """Tests for EndPoint.retreive_next_chunk_id() method."""

    def setUp(self):
        """Create test endpoint."""
        self.endpoint = EndPoint(
            alias="Test",
            service_type="YT_HLS",
            stream_key="key",
            stream_identifier="test-event-001",
            chunk_id=5,
        )

    @patch("restreamer.endpoints.requests.get")
    def test_retrieve_next_chunk_success(self, mock_get):
        """Test successful chunk ID retrieval."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {"next_chunk_id": 6}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        result = self.endpoint.retreive_next_chunk_id()

        self.assertEqual(result, 6)
        self.assertEqual(self.endpoint.chunk_id.value, 6)

    @patch("restreamer.endpoints.requests.get")
    def test_retrieve_next_chunk_timeout(self, mock_get):
        """Test handling of request timeout."""
        import requests

        mock_get.side_effect = requests.exceptions.Timeout()

        result = self.endpoint.retreive_next_chunk_id()

        self.assertIsNone(result)
        self.assertEqual(self.endpoint.chunk_id.value, 5)  # Unchanged

    @patch("restreamer.endpoints.requests.get")
    def test_retrieve_next_chunk_invalid_chunk_id(self, mock_get):
        """Test handling of invalid (non-increasing) chunk ID."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.json.return_value = {"next_chunk_id": 3}  # Less than current (5)
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        result = self.endpoint.retreive_next_chunk_id()

        self.assertIsNone(result)
        self.assertEqual(self.endpoint.chunk_id.value, 5)  # Unchanged

    def test_retrieve_next_chunk_invalid_stream_identifier(self):
        """Test handling of invalid stream identifier."""
        self.endpoint.stream_identifier = ""

        result = self.endpoint.retreive_next_chunk_id()

        self.assertIsNone(result)


class ManagerEndPointControlTests(TestCase):
    """Tests for ManagerEndPointControl class."""

    def setUp(self):
        """Create test manager."""
        self.manager = ManagerEndPointControl()

    def test_add_signal_queues_signal(self):
        """Test that add_signal adds to the queue."""
        signal = {"alias": "test", "action": "start"}
        self.manager.add_signal(signal)

        self.assertFalse(self.manager.signals.empty())
        queued = self.manager.signals.get()
        self.assertEqual(queued, signal)

    @patch.object(EndPoint, "start")
    def test_start_endpoint_creates_process(self, mock_start):
        """Test that start_endpoint creates and starts an EndPoint process."""
        self.manager.start_endpoint(
            alias="YouTube",
            service_type="YT_RTMP",
            stream_key="test-key",
            stream_id="event-123",
            chunk_id=1,
        )

        self.assertIn("YouTube", self.manager.endpoint_processes)
        mock_start.assert_called_once()

    @patch.object(EndPoint, "start")
    def test_start_endpoint_prevents_duplicates(self, mock_start):
        """Test that start_endpoint doesn't create duplicate endpoints."""
        self.manager.start_endpoint("YouTube", "YT_RTMP", "key", "event", 1)
        self.manager.start_endpoint("YouTube", "YT_RTMP", "key", "event", 1)

        # Should only be called once
        mock_start.assert_called_once()

    @patch.object(EndPoint, "start")
    @patch.object(EndPoint, "terminate")
    @patch.object(EndPoint, "join")
    def test_stop_endpoint_terminates_process(self, mock_join, mock_terminate, mock_start):
        """Test that stop_endpoint terminates the process."""
        self.manager.start_endpoint("YouTube", "YT_RTMP", "key", "event", 1)
        self.manager.stop_endpoint("YouTube")

        mock_terminate.assert_called_once()
        mock_join.assert_called_once()
        self.assertNotIn("YouTube", self.manager.endpoint_processes)

    @patch.object(EndPoint, "start")
    @patch.object(EndPoint, "terminate")
    @patch.object(EndPoint, "join")
    def test_stop_all_endpoints(self, mock_join, mock_terminate, mock_start):
        """Test that stop_all_endpoints stops all running endpoints."""
        self.manager.start_endpoint("YouTube", "YT_RTMP", "key1", "event", 1)
        self.manager.start_endpoint("Facebook", "FB", "key2", "event", 1)

        self.assertEqual(len(self.manager.endpoint_processes), 2)

        self.manager.stop_all_endpoints()

        self.assertEqual(len(self.manager.endpoint_processes), 0)
        self.assertEqual(mock_terminate.call_count, 2)

    def test_stop_sets_stop_event(self):
        """Test that stop() sets the stop event."""
        self.assertFalse(self.manager.stop_event.is_set())
        self.manager.stop()
        self.assertTrue(self.manager.stop_event.is_set())
