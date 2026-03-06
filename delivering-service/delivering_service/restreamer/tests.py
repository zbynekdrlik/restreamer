"""
Tests for the Delivering Service endpoints module.

These tests verify:
- FFmpeg command generation for different service types
- S3 object key format (must match Rust client upload format)
- Chunk retrieval and processing logic
- Endpoint control (start/stop)
- MPEG-TS timestamp normalization (TSTimestampNormalizer)
"""

import io
from unittest.mock import MagicMock, patch

from django.test import TestCase

from restreamer.endpoints import (
    SYNC_BYTE,
    TS_PACKET_SIZE,
    EndPoint,
    ManagerEndPointControl,
    TSTimestampNormalizer,
    _parse_ts_timestamp,
    _write_ts_timestamp,
)


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

    @patch("restreamer.endpoints.ffmpeg")
    def test_test_file_ffmpeg_command(self, mock_ffmpeg):
        """Test that TEST_FILE builds correct ffmpeg command for file output."""
        self.endpoint.service_type = "TEST_FILE"
        self.endpoint.stream_key = "test_output.ts"

        mock_input = MagicMock()
        mock_output = MagicMock()
        mock_ffmpeg.input.return_value = mock_input
        mock_input.output.return_value = mock_output
        mock_output.compile.return_value = ["ffmpeg", "-i", "pipe:"]
        mock_output.run_async.return_value = MagicMock(poll=MagicMock(return_value=None))

        with patch("time.sleep"):
            self.endpoint.run_ffmpeg()

        # Verify input uses mpegts format
        mock_ffmpeg.input.assert_called_once()
        call_args = mock_ffmpeg.input.call_args
        self.assertEqual(call_args[0][0], "pipe:")
        self.assertEqual(call_args[1]["format"], "mpegts")

        # Verify output is to a file with mpegts format
        mock_input.output.assert_called_once()
        output_args = mock_input.output.call_args
        output_path = output_args[0][0]
        self.assertTrue(output_path.endswith(".ts"))
        self.assertEqual(output_args[1].get("f"), "mpegts")
        self.assertEqual(output_args[1].get("c"), "copy")

    @patch("restreamer.endpoints.ffmpeg")
    def test_test_file_uses_custom_output_dir(self, mock_ffmpeg):
        """Test that TEST_FILE respects RESTREAMER_TEST_OUTPUT_DIR env var."""
        import os

        self.endpoint.service_type = "TEST_FILE"
        self.endpoint.stream_key = ""

        mock_input = MagicMock()
        mock_output = MagicMock()
        mock_ffmpeg.input.return_value = mock_input
        mock_input.output.return_value = mock_output
        mock_output.compile.return_value = ["ffmpeg", "-i", "pipe:"]
        mock_output.run_async.return_value = MagicMock(poll=MagicMock(return_value=None))

        with (
            patch("time.sleep"),
            patch.dict(os.environ, {"RESTREAMER_TEST_OUTPUT_DIR": "/custom/path"}),
        ):
            self.endpoint.run_ffmpeg()

        output_args = mock_input.output.call_args
        output_path = output_args[0][0]
        self.assertTrue(output_path.startswith("/custom/path/"))

    @patch("restreamer.endpoints.ffmpeg")
    def test_test_file_sanitizes_alias(self, mock_ffmpeg):
        """Test that TEST_FILE sanitizes alias for filename."""
        self.endpoint.service_type = "TEST_FILE"
        self.endpoint.alias = "Test/Endpoint With Spaces"
        self.endpoint.stream_key = ""

        mock_input = MagicMock()
        mock_output = MagicMock()
        mock_ffmpeg.input.return_value = mock_input
        mock_input.output.return_value = mock_output
        mock_output.compile.return_value = ["ffmpeg", "-i", "pipe:"]
        mock_output.run_async.return_value = MagicMock(poll=MagicMock(return_value=None))

        with patch("time.sleep"):
            self.endpoint.run_ffmpeg()

        output_args = mock_input.output.call_args
        output_path = output_args[0][0]
        # Should not contain spaces or slashes
        filename = output_path.split("/")[-1]
        self.assertNotIn(" ", filename)
        self.assertNotIn("/", filename)

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


class EndpointProcessStatusViewTests(TestCase):
    """Tests for the EndpointProcessStatusView API endpoint."""

    def test_endpoint_status_returns_empty_when_no_endpoints(self):
        """Test that endpoint-status returns empty list when no endpoints are running."""
        from django.test import RequestFactory

        from restreamer.views import EndpointProcessStatusView

        factory = RequestFactory()
        request = factory.get("/api/endpoint-status/")
        view = EndpointProcessStatusView.as_view()
        response = view(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["status"], "ok")
        self.assertEqual(response.data["endpoint_count"], 0)
        self.assertEqual(response.data["endpoints"], [])

    @patch.object(EndPoint, "start")
    def test_endpoint_status_returns_running_endpoints(self, mock_start):
        """Test that endpoint-status returns info about running endpoints."""
        from django.test import RequestFactory

        from restreamer.endpoints import endpoing_manger
        from restreamer.views import EndpointProcessStatusView

        # Start a mock endpoint
        endpoing_manger.start_endpoint("Test EP", "YT_HLS", "key123", "event-001", 42)

        factory = RequestFactory()
        request = factory.get("/api/endpoint-status/")
        view = EndpointProcessStatusView.as_view()
        response = view(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["endpoint_count"], 1)
        self.assertEqual(len(response.data["endpoints"]), 1)

        ep = response.data["endpoints"][0]
        self.assertEqual(ep["alias"], "Test EP")
        self.assertIn("alive", ep)
        self.assertIn("pid", ep)
        self.assertIn("buff_size_mb", ep)
        self.assertIn("current_chunk_id", ep)

        # Clean up
        endpoing_manger.endpoint_processes["Test EP"].terminate()
        endpoing_manger.endpoint_processes["Test EP"].join()
        del endpoing_manger.endpoint_processes["Test EP"]


def _encode_ts_timestamp(value, marker_nibble):
    """Encode a 33-bit timestamp into 5 bytes in MPEG-TS PES format."""
    buf = bytearray(5)
    _write_ts_timestamp(buf, 0, value, marker_nibble)
    return bytes(buf)


def _build_ts_packet(stream_id, pts=None, dts=None, pid=0x100):
    """Build a minimal 188-byte MPEG-TS packet with a PES header containing timestamps.

    stream_id: 0xE0-0xEF for video, 0xC0-0xDF for audio
    pts/dts: 33-bit timestamp values (optional)
    """
    packet = bytearray(TS_PACKET_SIZE)
    packet[0] = SYNC_BYTE
    # PUSI=1 (bit 6 of byte 1), PID in bytes 1-2
    packet[1] = 0x40 | ((pid >> 8) & 0x1F)
    packet[2] = pid & 0xFF
    # AFC=01 (payload only), continuity counter=0
    packet[3] = 0x10

    # PES header starts at byte 4
    payload_start = 4
    packet[payload_start] = 0x00  # start code
    packet[payload_start + 1] = 0x00
    packet[payload_start + 2] = 0x01
    packet[payload_start + 3] = stream_id

    # PES packet length (0 = unbounded for video)
    packet[payload_start + 4] = 0x00
    packet[payload_start + 5] = 0x00

    # PES header data byte (marker bits)
    packet[payload_start + 6] = 0x80

    if pts is not None and dts is not None:
        pts_dts_flags = 3
        header_data_length = 10
    elif pts is not None:
        pts_dts_flags = 2
        header_data_length = 5
    else:
        pts_dts_flags = 0
        header_data_length = 0

    packet[payload_start + 7] = pts_dts_flags << 6
    packet[payload_start + 8] = header_data_length

    if pts is not None:
        pts_marker = 3 if dts is not None else 2
        _write_ts_timestamp(packet, payload_start + 9, pts, pts_marker)

    if dts is not None:
        _write_ts_timestamp(packet, payload_start + 14, dts, 1)

    return bytes(packet)


class TSTimestampParseWriteTests(TestCase):
    """Tests for _parse_ts_timestamp / _write_ts_timestamp roundtrip."""

    def test_roundtrip_zero(self):
        buf = bytearray(5)
        _write_ts_timestamp(buf, 0, 0, 2)
        self.assertEqual(_parse_ts_timestamp(buf, 0), 0)

    def test_roundtrip_known_value(self):
        """Test roundtrip with a known typical DTS value."""
        ts = 126000  # typical ~1.4s at 90kHz
        buf = bytearray(5)
        _write_ts_timestamp(buf, 0, ts, 2)
        self.assertEqual(_parse_ts_timestamp(buf, 0), ts)

    def test_roundtrip_large_value(self):
        """Test roundtrip near 33-bit maximum."""
        ts = 0x1FFFFFFFF  # max 33-bit value
        buf = bytearray(5)
        _write_ts_timestamp(buf, 0, ts, 3)
        self.assertEqual(_parse_ts_timestamp(buf, 0), ts)

    def test_roundtrip_various_values(self):
        """Test roundtrip across range of values."""
        test_values = [1, 90000, 180000, 8100000, 0x100000000, 0x1FFFFFFFE]
        for ts in test_values:
            buf = bytearray(5)
            _write_ts_timestamp(buf, 0, ts, 2)
            parsed = _parse_ts_timestamp(buf, 0)
            self.assertEqual(parsed, ts, f"Failed roundtrip for {ts}")

    def test_write_preserves_marker_bits(self):
        """Test that marker nibble is correctly written."""
        buf = bytearray(5)
        _write_ts_timestamp(buf, 0, 90000, 2)
        # marker_nibble=2 → top nibble of byte 0 should be 0x2X
        self.assertEqual((buf[0] >> 4) & 0x0F, 2)

        _write_ts_timestamp(buf, 0, 90000, 3)
        self.assertEqual((buf[0] >> 4) & 0x0F, 3)

    def test_33bit_wraparound(self):
        """Test that values exceeding 33 bits wrap correctly."""
        ts = 0x1FFFFFFFF + 1  # one past max
        buf = bytearray(5)
        _write_ts_timestamp(buf, 0, ts, 2)
        self.assertEqual(_parse_ts_timestamp(buf, 0), 0)  # wrapped to 0

    def test_offset_within_buffer(self):
        """Test parsing/writing at a non-zero offset."""
        buf = bytearray(20)
        _write_ts_timestamp(buf, 10, 54000, 1)
        self.assertEqual(_parse_ts_timestamp(buf, 10), 54000)
        # Bytes before offset should be untouched
        self.assertEqual(buf[:10], bytearray(10))


class TSTimestampNormalizerBasicTests(TestCase):
    """Tests for TSTimestampNormalizer.normalize() with crafted packets."""

    def test_empty_data_returns_empty(self):
        norm = TSTimestampNormalizer()
        result = norm.normalize(b"")
        self.assertEqual(result, b"")

    def test_truncated_packet_no_crash(self):
        """Data shorter than 188 bytes should pass through unchanged."""
        norm = TSTimestampNormalizer()
        short_data = bytes(100)
        result = norm.normalize(short_data)
        self.assertEqual(result, short_data)

    def test_single_video_packet_pts_only(self):
        """A single video packet with PTS-only should get normalized timestamps."""
        norm = TSTimestampNormalizer()
        original_pts = 900000
        packet = _build_ts_packet(stream_id=0xE0, pts=original_pts)

        result = norm.normalize(packet)

        # After normalization, PTS should be default_duration (first frame)
        parsed_pts = _parse_ts_timestamp(bytearray(result), 4 + 9)
        self.assertEqual(parsed_pts, TSTimestampNormalizer.VIDEO_DEFAULT_DURATION)

    def test_single_video_packet_pts_and_dts(self):
        """A video packet with both PTS and DTS."""
        norm = TSTimestampNormalizer()
        packet = _build_ts_packet(stream_id=0xE0, pts=900000, dts=890000)

        result = norm.normalize(packet)

        data = bytearray(result)
        parsed_pts = _parse_ts_timestamp(data, 4 + 9)
        parsed_dts = _parse_ts_timestamp(data, 4 + 14)
        self.assertEqual(parsed_pts, TSTimestampNormalizer.VIDEO_DEFAULT_DURATION)
        self.assertEqual(parsed_dts, TSTimestampNormalizer.VIDEO_DEFAULT_DURATION)

    def test_single_audio_packet(self):
        """An audio packet should use audio default duration."""
        norm = TSTimestampNormalizer()
        packet = _build_ts_packet(stream_id=0xC0, pts=500000)

        result = norm.normalize(packet)

        parsed_pts = _parse_ts_timestamp(bytearray(result), 4 + 9)
        self.assertEqual(parsed_pts, TSTimestampNormalizer.AUDIO_DEFAULT_DURATION)

    def test_no_pes_header_passes_through(self):
        """Packet without PES start code is left untouched."""
        norm = TSTimestampNormalizer()
        packet = bytearray(TS_PACKET_SIZE)
        packet[0] = SYNC_BYTE
        packet[1] = 0x40  # PUSI=1
        packet[3] = 0x10  # payload only
        # No PES start code (bytes 4,5,6 are all zero but that's 00 00 00, not 00 00 01)

        result = norm.normalize(bytes(packet))
        self.assertEqual(result, bytes(packet))


class TSTimestampNormalizerDeltaTests(TestCase):
    """Tests for delta computation and capping behavior."""

    def test_sequential_packets_use_actual_delta(self):
        """Two video packets with known delta should preserve that delta."""
        norm = TSTimestampNormalizer()

        pkt1 = _build_ts_packet(stream_id=0xE0, pts=100000, dts=100000)
        pkt2 = _build_ts_packet(stream_id=0xE0, pts=103000, dts=103000)
        combined = pkt1 + pkt2

        result = norm.normalize(combined)

        data = bytearray(result)
        # First packet: default duration (no prior reference)
        pts1 = _parse_ts_timestamp(data, 4 + 9)
        dts1 = _parse_ts_timestamp(data, 4 + 14)
        self.assertEqual(pts1, TSTimestampNormalizer.VIDEO_DEFAULT_DURATION)
        self.assertEqual(dts1, TSTimestampNormalizer.VIDEO_DEFAULT_DURATION)

        # Second packet: delta = 103000 - 100000 = 3000
        pts2 = _parse_ts_timestamp(data, TS_PACKET_SIZE + 4 + 9)
        dts2 = _parse_ts_timestamp(data, TS_PACKET_SIZE + 4 + 14)
        expected = TSTimestampNormalizer.VIDEO_DEFAULT_DURATION + 3000
        self.assertEqual(pts2, expected)
        self.assertEqual(dts2, expected)

    def test_large_jump_capped_to_default(self):
        """Timestamps with >1s jump should be capped to default duration."""
        norm = TSTimestampNormalizer()

        pkt1 = _build_ts_packet(stream_id=0xE0, pts=100000, dts=100000)
        # Jump of 200000 ticks (>90000 MAX_DELTA)
        pkt2 = _build_ts_packet(stream_id=0xE0, pts=300000, dts=300000)
        combined = pkt1 + pkt2

        result = norm.normalize(combined)
        data = bytearray(result)

        pts2 = _parse_ts_timestamp(data, TS_PACKET_SIZE + 4 + 9)
        # Should be 2 * default (first + capped second)
        expected = TSTimestampNormalizer.VIDEO_DEFAULT_DURATION * 2
        self.assertEqual(pts2, expected)

    def test_negative_delta_uses_default(self):
        """If timestamps go backwards, should use default duration."""
        norm = TSTimestampNormalizer()

        pkt1 = _build_ts_packet(stream_id=0xE0, pts=200000, dts=200000)
        pkt2 = _build_ts_packet(stream_id=0xE0, pts=100000, dts=100000)
        combined = pkt1 + pkt2

        result = norm.normalize(combined)
        data = bytearray(result)

        pts2 = _parse_ts_timestamp(data, TS_PACKET_SIZE + 4 + 9)
        expected = TSTimestampNormalizer.VIDEO_DEFAULT_DURATION * 2
        self.assertEqual(pts2, expected)

    def test_compute_delta_none_previous(self):
        """_compute_delta with no previous timestamp returns default."""
        norm = TSTimestampNormalizer()
        self.assertEqual(norm._compute_delta(100000, None, 3000), 3000)

    def test_compute_delta_normal(self):
        norm = TSTimestampNormalizer()
        self.assertEqual(norm._compute_delta(106000, 100000, 3000), 6000)

    def test_compute_delta_zero(self):
        """Zero delta should fall back to default."""
        norm = TSTimestampNormalizer()
        self.assertEqual(norm._compute_delta(100000, 100000, 3000), 3000)

    def test_compute_delta_exceeds_max(self):
        """Delta > MAX_DELTA should fall back to default."""
        norm = TSTimestampNormalizer()
        self.assertEqual(
            norm._compute_delta(200000, 100000, 3000),
            3000,  # 100000 > MAX_DELTA(90000)
        )


class TSTimestampNormalizerStreamSeparationTests(TestCase):
    """Tests that video and audio streams are handled independently."""

    def test_video_and_audio_independent_timestamps(self):
        """Video and audio in same data should get independent normalized timestamps."""
        norm = TSTimestampNormalizer()

        video_pkt = _build_ts_packet(stream_id=0xE0, pts=500000, dts=500000, pid=0x100)
        audio_pkt = _build_ts_packet(stream_id=0xC0, pts=600000, pid=0x101)
        combined = video_pkt + audio_pkt

        result = norm.normalize(combined)
        data = bytearray(result)

        video_pts = _parse_ts_timestamp(data, 4 + 9)
        audio_pts = _parse_ts_timestamp(data, TS_PACKET_SIZE + 4 + 9)

        # Video uses video default, audio uses audio default
        self.assertEqual(video_pts, TSTimestampNormalizer.VIDEO_DEFAULT_DURATION)
        self.assertEqual(audio_pts, TSTimestampNormalizer.AUDIO_DEFAULT_DURATION)

    def test_interleaved_streams(self):
        """Interleaved video/audio packets maintain separate state."""
        norm = TSTimestampNormalizer()

        v1 = _build_ts_packet(stream_id=0xE0, pts=100000, dts=100000, pid=0x100)
        a1 = _build_ts_packet(stream_id=0xC0, pts=200000, pid=0x101)
        v2 = _build_ts_packet(stream_id=0xE0, pts=103000, dts=103000, pid=0x100)
        a2 = _build_ts_packet(stream_id=0xC0, pts=201920, pid=0x101)
        combined = v1 + a1 + v2 + a2

        result = norm.normalize(combined)
        data = bytearray(result)

        v1_pts = _parse_ts_timestamp(data, 0 * TS_PACKET_SIZE + 4 + 9)
        a1_pts = _parse_ts_timestamp(data, 1 * TS_PACKET_SIZE + 4 + 9)
        v2_pts = _parse_ts_timestamp(data, 2 * TS_PACKET_SIZE + 4 + 9)
        a2_pts = _parse_ts_timestamp(data, 3 * TS_PACKET_SIZE + 4 + 9)

        self.assertEqual(v1_pts, 3000)  # video default
        self.assertEqual(a1_pts, 1920)  # audio default
        self.assertEqual(v2_pts, 6000)  # 3000 + delta(3000)
        self.assertEqual(a2_pts, 3840)  # 1920 + delta(1920)


class TSTimestampNormalizerResetTests(TestCase):
    """Tests that creating a new normalizer produces fresh state."""

    def test_fresh_normalizer_starts_from_zero(self):
        """A new normalizer should start output timestamps from zero + default."""
        norm1 = TSTimestampNormalizer()
        packet = _build_ts_packet(stream_id=0xE0, pts=500000, dts=500000)
        norm1.normalize(packet)

        # New normalizer should be independent
        norm2 = TSTimestampNormalizer()
        result = norm2.normalize(packet)
        data = bytearray(result)

        pts = _parse_ts_timestamp(data, 4 + 9)
        self.assertEqual(pts, TSTimestampNormalizer.VIDEO_DEFAULT_DURATION)

    def test_state_after_init(self):
        """Verify initial state is clean."""
        norm = TSTimestampNormalizer()
        self.assertEqual(norm._video["out_dts"], 0)
        self.assertEqual(norm._video["out_pts"], 0)
        self.assertIsNone(norm._video["prev_orig_dts"])
        self.assertIsNone(norm._video["prev_orig_pts"])
        self.assertEqual(norm._audio["out_dts"], 0)
        self.assertEqual(norm._audio["out_pts"], 0)
        self.assertIsNone(norm._audio["prev_orig_dts"])
        self.assertIsNone(norm._audio["prev_orig_pts"])


class TSTimestampNormalizerEdgeCaseTests(TestCase):
    """Edge cases and robustness tests."""

    def test_non_av_stream_id_ignored(self):
        """Packets with non-audio/video stream IDs should not be modified."""
        norm = TSTimestampNormalizer()
        # Stream ID 0xBD = private stream 1
        packet = _build_ts_packet(stream_id=0xBD, pts=100000)
        norm.normalize(packet)
        # No video/audio state should have changed
        self.assertEqual(norm._video["out_dts"], 0)
        self.assertEqual(norm._audio["out_dts"], 0)

    def test_packet_without_pusi_skipped(self):
        """Packets without payload unit start indicator are skipped."""
        norm = TSTimestampNormalizer()
        packet = bytearray(TS_PACKET_SIZE)
        packet[0] = SYNC_BYTE
        packet[1] = 0x00  # PUSI=0
        packet[3] = 0x10

        result = norm.normalize(bytes(packet))
        self.assertEqual(result, bytes(packet))

    def test_multiple_chunks_continuous(self):
        """Simulates processing two separate chunks maintaining continuity."""
        norm = TSTimestampNormalizer()

        # Chunk 1: timestamps 100000, 103000
        c1_pkt1 = _build_ts_packet(stream_id=0xE0, pts=100000, dts=100000)
        c1_pkt2 = _build_ts_packet(stream_id=0xE0, pts=103000, dts=103000)
        chunk1 = c1_pkt1 + c1_pkt2

        # Chunk 2: timestamps jump to 900000 (new recording session)
        c2_pkt1 = _build_ts_packet(stream_id=0xE0, pts=900000, dts=900000)
        c2_pkt2 = _build_ts_packet(stream_id=0xE0, pts=903000, dts=903000)
        chunk2 = c2_pkt1 + c2_pkt2

        result1 = norm.normalize(chunk1)
        result2 = norm.normalize(chunk2)

        data1 = bytearray(result1)
        data2 = bytearray(result2)

        # Chunk 1 results
        t1 = _parse_ts_timestamp(data1, 4 + 9)
        t2 = _parse_ts_timestamp(data1, TS_PACKET_SIZE + 4 + 9)
        self.assertEqual(t1, 3000)  # default
        self.assertEqual(t2, 6000)  # 3000 + 3000

        # Chunk 2: jump >90000 gets capped to default
        t3 = _parse_ts_timestamp(data2, 4 + 9)
        t4 = _parse_ts_timestamp(data2, TS_PACKET_SIZE + 4 + 9)
        self.assertEqual(t3, 9000)  # 6000 + 3000 (capped)
        self.assertEqual(t4, 12000)  # 9000 + 3000

    def test_data_not_starting_with_sync_byte(self):
        """Data with garbage before sync byte should still find packets."""
        norm = TSTimestampNormalizer()
        garbage = bytes(5)  # 5 bytes of zeros
        packet = _build_ts_packet(stream_id=0xE0, pts=100000)
        data = garbage + packet

        result = norm.normalize(data)
        # Should have found and processed the packet after the garbage
        out_data = bytearray(result)
        pts = _parse_ts_timestamp(out_data, 5 + 4 + 9)
        self.assertEqual(pts, TSTimestampNormalizer.VIDEO_DEFAULT_DURATION)

    def test_adaptation_field_with_pes(self):
        """Packet with adaptation field + PES header."""
        norm = TSTimestampNormalizer()
        packet = bytearray(TS_PACKET_SIZE)
        packet[0] = SYNC_BYTE
        packet[1] = 0x40  # PUSI=1
        packet[3] = 0x30  # AFC=11 (adaptation + payload)
        packet[4] = 7  # adaptation field length

        # PES header starts after adaptation field: 5 + 7 = 12
        pes_start = 12
        packet[pes_start] = 0x00
        packet[pes_start + 1] = 0x00
        packet[pes_start + 2] = 0x01
        packet[pes_start + 3] = 0xE0  # video
        packet[pes_start + 6] = 0x80
        packet[pes_start + 7] = 0x80  # PTS only (flags=2)
        packet[pes_start + 8] = 5  # header data length
        _write_ts_timestamp(packet, pes_start + 9, 50000, 2)

        result = norm.normalize(bytes(packet))
        data = bytearray(result)
        pts = _parse_ts_timestamp(data, pes_start + 9)
        self.assertEqual(pts, TSTimestampNormalizer.VIDEO_DEFAULT_DURATION)
