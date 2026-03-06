import logging
import multiprocessing
import time
from multiprocessing import Queue
from threading import Event, Thread

import ffmpeg
import requests
from django.conf import settings

log = logging.getLogger(__name__)

TS_PACKET_SIZE = 188
SYNC_BYTE = 0x47


def _parse_ts_timestamp(data, offset):
    """Parse a 33-bit MPEG-TS PES timestamp from 5 bytes."""
    b0, b1, b2, b3, b4 = data[offset], data[offset + 1], data[offset + 2], data[offset + 3], data[offset + 4]
    ts = ((b0 >> 1) & 0x07) << 30
    ts |= b1 << 22
    ts |= ((b2 >> 1) & 0x7F) << 15
    ts |= b3 << 7
    ts |= (b4 >> 1) & 0x7F
    return ts


def _write_ts_timestamp(data, offset, value, marker_nibble):
    """Write a 33-bit timestamp to 5 bytes in MPEG-TS PES format."""
    value = value & 0x1FFFFFFFF  # 33-bit mask
    data[offset] = (marker_nibble << 4) | (((value >> 30) & 0x07) << 1) | 0x01
    data[offset + 1] = (value >> 22) & 0xFF
    data[offset + 2] = (((value >> 15) & 0x7F) << 1) | 0x01
    data[offset + 3] = (value >> 7) & 0xFF
    data[offset + 4] = ((value & 0x7F) << 1) | 0x01


class TSTimestampNormalizer:
    """Rewrites DTS/PTS in MPEG-TS data to produce continuous timestamps.

    Uses actual DTS deltas from the original stream to preserve natural timing
    within chunks, while ensuring continuity across chunk boundaries.
    """

    # Default durations at 90kHz timebase (used only for first PES or fallback)
    VIDEO_DEFAULT_DURATION = 3000  # 30fps
    AUDIO_DEFAULT_DURATION = 1920  # AAC 48kHz 1024 samples
    # Max reasonable delta: 1 second at 90kHz (caps large jumps at chunk boundaries)
    MAX_DELTA = 90000

    def __init__(self):
        self.video_out_dts = 0
        self.video_out_pts = 0
        self.video_prev_orig_dts = None
        self.video_prev_orig_pts = None
        self.audio_out_dts = 0
        self.audio_out_pts = 0
        self.audio_prev_orig_dts = None
        self.audio_prev_orig_pts = None

    def normalize(self, chunk_data):
        """Rewrite all DTS/PTS in MPEG-TS chunk data using original deltas.

        Returns the modified data with continuous timestamps.
        """
        data = bytearray(chunk_data)
        pos = 0

        while pos + TS_PACKET_SIZE <= len(data):
            if data[pos] != SYNC_BYTE:
                pos += 1
                continue

            pusi = (data[pos + 1] >> 6) & 1
            if not pusi:
                pos += TS_PACKET_SIZE
                continue

            afc = (data[pos + 3] >> 4) & 0x03
            if afc in (2, 3):
                af_length = data[pos + 4]
                payload_start = pos + 5 + af_length
            else:
                payload_start = pos + 4

            if payload_start + 9 > pos + TS_PACKET_SIZE:
                pos += TS_PACKET_SIZE
                continue

            if data[payload_start] != 0 or data[payload_start + 1] != 0 or data[payload_start + 2] != 1:
                pos += TS_PACKET_SIZE
                continue

            stream_id = data[payload_start + 3]
            is_video = 0xE0 <= stream_id <= 0xEF
            is_audio = 0xC0 <= stream_id <= 0xDF

            if not (is_video or is_audio):
                pos += TS_PACKET_SIZE
                continue

            pts_dts_flags = (data[payload_start + 7] >> 6) & 0x03

            if is_video:
                self._rewrite_video(data, payload_start, pts_dts_flags, pos + TS_PACKET_SIZE)
            else:
                self._rewrite_audio(data, payload_start, pts_dts_flags, pos + TS_PACKET_SIZE)

            pos += TS_PACKET_SIZE

        return bytes(data)

    def _compute_delta(self, orig_ts, prev_orig_ts, default_dur):
        """Compute a safe delta from original timestamps."""
        if prev_orig_ts is None:
            return default_dur
        delta = orig_ts - prev_orig_ts
        if delta <= 0 or delta > self.MAX_DELTA:
            return default_dur
        return delta

    def _rewrite_video(self, data, payload_start, pts_dts_flags, packet_end):
        orig_dts = None
        orig_pts = None

        if pts_dts_flags >= 2:
            pts_pos = payload_start + 9
            if pts_pos + 5 <= packet_end:
                orig_pts = _parse_ts_timestamp(data, pts_pos)

        if pts_dts_flags == 3:
            dts_pos = payload_start + 14
            if dts_pos + 5 <= packet_end:
                orig_dts = _parse_ts_timestamp(data, dts_pos)

        # Compute deltas from original timestamps
        if orig_dts is not None:
            dts_delta = self._compute_delta(orig_dts, self.video_prev_orig_dts, self.VIDEO_DEFAULT_DURATION)
            self.video_prev_orig_dts = orig_dts
        else:
            dts_delta = self.VIDEO_DEFAULT_DURATION

        if orig_pts is not None:
            pts_delta = self._compute_delta(orig_pts, self.video_prev_orig_pts, self.VIDEO_DEFAULT_DURATION)
            self.video_prev_orig_pts = orig_pts
        else:
            pts_delta = self.VIDEO_DEFAULT_DURATION

        # Advance output timestamps by the computed delta
        self.video_out_dts += dts_delta
        self.video_out_pts += pts_delta

        # Write new timestamps
        if pts_dts_flags >= 2:
            pts_pos = payload_start + 9
            if pts_pos + 5 <= packet_end:
                marker = 3 if pts_dts_flags == 3 else 2
                _write_ts_timestamp(data, pts_pos, self.video_out_pts, marker)

        if pts_dts_flags == 3:
            dts_pos = payload_start + 14
            if dts_pos + 5 <= packet_end:
                _write_ts_timestamp(data, dts_pos, self.video_out_dts, 1)

    def _rewrite_audio(self, data, payload_start, pts_dts_flags, packet_end):
        orig_dts = None
        orig_pts = None

        if pts_dts_flags >= 2:
            pts_pos = payload_start + 9
            if pts_pos + 5 <= packet_end:
                orig_pts = _parse_ts_timestamp(data, pts_pos)

        if pts_dts_flags == 3:
            dts_pos = payload_start + 14
            if dts_pos + 5 <= packet_end:
                orig_dts = _parse_ts_timestamp(data, dts_pos)

        if orig_dts is not None:
            dts_delta = self._compute_delta(orig_dts, self.audio_prev_orig_dts, self.AUDIO_DEFAULT_DURATION)
            self.audio_prev_orig_dts = orig_dts
        else:
            dts_delta = self.AUDIO_DEFAULT_DURATION

        if orig_pts is not None:
            pts_delta = self._compute_delta(orig_pts, self.audio_prev_orig_pts, self.AUDIO_DEFAULT_DURATION)
            self.audio_prev_orig_pts = orig_pts
        else:
            pts_delta = self.AUDIO_DEFAULT_DURATION

        self.audio_out_dts += dts_delta
        self.audio_out_pts += pts_delta

        if pts_dts_flags >= 2:
            pts_pos = payload_start + 9
            if pts_pos + 5 <= packet_end:
                marker = 3 if pts_dts_flags == 3 else 2
                _write_ts_timestamp(data, pts_pos, self.audio_out_pts, marker)

        if pts_dts_flags == 3:
            dts_pos = payload_start + 14
            if dts_pos + 5 <= packet_end:
                _write_ts_timestamp(data, dts_pos, self.audio_out_dts, 1)


class EndPoint(multiprocessing.Process):
    def __init__(self, alias, service_type, stream_key, stream_identifier, chunk_id):
        super().__init__(name=alias)
        self.alias = alias
        self.service_type = service_type
        self.stream_key = stream_key
        self.buff_size = multiprocessing.Value("L", 0)
        self.chunk_record_id = multiprocessing.Value("i", 0)
        self.reader_thread_terminate = Event()
        # self.stdout_thread = None
        self.stderr_thread = None
        self.last_processed_chunk_id = None
        self.chunk_id = multiprocessing.Value("i", chunk_id)
        self.stream_identifier = stream_identifier
        self.s3 = settings.S3_CLIENT
        self.bucket = settings.AWS_STORAGE_BUCKET_NAME
        self.ts_normalizer = TSTimestampNormalizer() if service_type == "YT_HLS" else None

    def run_ffmpeg(self):
        if self.service_type == "YT_HLS":
            output_url = f"https://a.upload.youtube.com/http_upload_hls?cid={self.stream_key}&copy=0&file=out1248.ts"
            cmd = (
                ffmpeg.input(
                    "pipe:",
                    readrate=1.00,
                    format="mpegts",
                    loglevel="info",
                    fflags="+genpts+discardcorrupt",
                )
                .output(
                    output_url,
                    f="hls",
                    hls_segment_type="mpegts",
                    hls_segment_options="mpegts_flags=+pat_pmt_at_frames+resend_headers",
                    hls_list_size=5,
                    hls_time=2,
                    hls_flags="delete_segments",
                    start_number=0,
                    method="PUT",
                    c="copy",
                    flags="+cgop",
                    muxdelay="0",
                    muxpreload="0",
                    reset_timestamps=1,
                )
                .global_args("-avoid_negative_ts", "make_zero")
            )
            """   threads=2 b='4000k, audio_codec='aac' ,c='libx264',s='1280x720''"""
        elif self.service_type == "FB":
            output_url = f"rtmps://live-api-s.facebook.com:443/rtmp/{self.stream_key}"
            cmd = ffmpeg.input(
                "pipe:",
                readrate=1.00,
                format="mpegts",
                loglevel="info",
                fflags="+genpts+discardcorrupt",
            ).output(output_url, f="flv", c="copy")

        elif self.service_type == "YT_RTMP":
            output_url = f"rtmp://a.rtmp.youtube.com/live2/{self.stream_key}"
            cmd = (
                ffmpeg.input("pipe:", format="mpegts", readrate=1.00, loglevel="info", fflags="+genpts+discardcorrupt")
                .output(
                    output_url,
                    f="flv",
                    vcodec="copy",
                    acodec="aac",
                    ab="160k",
                    ac=2,
                    ar="48000",
                )
                .global_args("-vf", "yadif")
                .global_args("-re")
            )

        elif self.service_type == "VIMEO":
            output_url = f"rtmps://rtmp-global.cloud.vimeo.com:443/live/{self.stream_key}"
            cmd = ffmpeg.input(
                "pipe:",
                readrate=1.00,
                format="mpegts",
                loglevel="info",
                fflags="+genpts+discardcorrupt",
            ).output(output_url, f="flv", c="copy")

        elif self.service_type == "TEST_FILE":
            # Test endpoint that writes to a local file instead of streaming.
            # Used for automated E2E testing without requiring real streaming platforms.
            # Output path: configurable via RESTREAMER_TEST_OUTPUT_DIR env var
            import os
            import tempfile

            # Use RESTREAMER_TEST_OUTPUT_DIR if set, otherwise use system temp
            output_dir = os.environ.get("RESTREAMER_TEST_OUTPUT_DIR") or tempfile.gettempdir()
            safe_alias = self.alias.replace(" ", "_").replace("/", "_")
            if self.stream_key and self.stream_key.endswith(".ts"):
                output_path = os.path.join(output_dir, self.stream_key)
            else:
                output_path = os.path.join(output_dir, f"restreamer_test_{safe_alias}.ts")

            log.info(f"TEST_FILE endpoint writing to: {output_path}")
            cmd = ffmpeg.input(
                "pipe:",
                format="mpegts",
                loglevel="info",
            ).output(output_path, f="mpegts", c="copy")

        else:
            log.error(f"Unsupported service type: {self.service_type}")
            raise ValueError

        log.info(f"Starting new instance of end point ffmpeg for {self.alias}")
        log.debug(" ".join(cmd.compile()))

        self.reader_thread_terminate.set()
        # if self.stdout_thread:
        #     self.stdout_thread.join()
        if self.stderr_thread:
            self.stderr_thread.join()
        self.reader_thread_terminate.clear()

        process = cmd.run_async(pipe_stdin=True, pipe_stdout=False, pipe_stderr=True)
        time.sleep(5)

        # self.stdout_thread = Thread(target=self.reader_thread, args=(process.stdout, 'stdout'), daemon=True)
        # self.stdout_thread.start()
        self.stderr_thread = Thread(target=self.reader_thread, args=(process.stderr, "stderr"), daemon=True)
        self.stderr_thread.start()

        return process

    def reader_thread(self, pipe, pipe_name):
        try:
            with open(f"{self.alias.replace(' ', '_')}_{pipe_name}.log", "ab") as logfile:
                line = bytearray()
                while True:
                    if self.reader_thread_terminate.is_set():
                        log.debug("Terminating reader_thread.")
                        break
                    char = pipe.read(1)
                    line.extend(char)
                    if char in b"\r\n":
                        log_entry = f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {line.decode('utf-8', errors='ignore')}"
                        logfile.write(log_entry.encode("utf-8"))
                        logfile.flush()
                        line.clear()
                    elif char == b"":
                        log.warning("Terminating reader_thread function!")
                        break
        except KeyboardInterrupt:
            # pipe.close()
            pass
        except Exception as e:
            log.exception(e)

    def process_chunk(self, ffmpeg_process, response):

        if not ffmpeg_process.poll():
            try:
                if response:
                    chunk_data = response["Body"].read()
                    if self.ts_normalizer:
                        chunk_data = self.ts_normalizer.normalize(chunk_data)
                    ffmpeg_process.stdin.write(chunk_data)
                    ffmpeg_process.stdin.flush()
                    self.buff_size.value += len(chunk_data)
                else:
                    log.warning("Chunk file not exists, skipping!")

            except BrokenPipeError:
                log.warning("Write to ffmpeg stdin unsuccessful")
            except Exception as e:
                log.error(f"Error {e}")

    def retreive_next_chunk_id(self):
        """Retrieves the next available chunk ID from the streaming API and updates the current chunk ID."""

        # Validate inputs first
        if not hasattr(self.chunk_id, "value") or not isinstance(self.chunk_id.value, int):
            log.error("Invalid current chunk ID state")
            return None

        if not isinstance(self.stream_identifier, str) or not self.stream_identifier:
            log.error("Invalid stream identifier")
            return None

        try:
            api_url = "https://restreamer.newlevel.media/api/get-next-chunk/"
            params = {
                "current_local_id": str(self.chunk_id.value),
                "stream_identifier": self.stream_identifier,
            }

            response = requests.get(
                api_url,
                params=params,
                timeout=(3.0),
                verify=True,
            )
            response.raise_for_status()

            if "application/json" not in response.headers.get("Content-Type", ""):
                log.error("Invalid content type in response")
                return None

            try:
                data = response.json()
            except ValueError:
                log.error("Invalid JSON response")
                return None

            if not isinstance(data, dict):
                log.error("Response is not a JSON object")
                return None

            next_chunk_id = data.get("next_chunk_id")

            if next_chunk_id is None:
                log.warning("No next_chunk_id in response")
                return None

            if not isinstance(next_chunk_id, int) or next_chunk_id <= self.chunk_id.value:
                log.error(f"Invalid chunk ID received: {next_chunk_id}")
                return None

            self.chunk_id.value = next_chunk_id

            return next_chunk_id

        except requests.exceptions.SSLError:
            log.error("SSL certificate verification failed")
            return None
        except requests.exceptions.Timeout:
            log.warning("API request timed out")
            return None
        except requests.exceptions.TooManyRedirects:
            log.error("Too many redirects")
            return None
        except requests.exceptions.RequestException as e:
            log.error(f"Network error: {type(e).__name__}")  # Don't log full exception
            return None
        except Exception:
            log.error("Unexpected error in chunk fetch", exc_info=True)  # Structured logging
            return None

    def run(self):
        from django.db import connection

        connection.close()

        ffmpeg_process = self.run_ffmpeg()

        try:
            while True:
                time.sleep(0.1)

                ret = ffmpeg_process.poll()
                if ret is not None:
                    log.warning(f"Ffmpeg process has exited with code:{ret}!!!")
                    time.sleep(3)
                    if self.ts_normalizer:
                        self.ts_normalizer = TSTimestampNormalizer()
                    ffmpeg_process = self.run_ffmpeg()
                    continue

                if not self.retreive_next_chunk_id():
                    log.warning("The buffer is empty !!! Waiting for new data.")
                    time.sleep(20)
                    continue

                try:
                    object_key = f"{self.stream_identifier}/{self.chunk_id.value}_{self.stream_identifier}.bin"
                    response = self.s3.get_object(Bucket=self.bucket, Key=object_key)
                    self.process_chunk(ffmpeg_process, response)
                except self.s3.exceptions.NoSuchKey:
                    log.warning(
                        f"NoSuchKey: The requested object does not exist."
                        f"Bucket: {self.bucket}, Key: {object_key}, "
                        f"Stream Identifier: {self.stream_identifier}, Chunk ID: {self.chunk_id.value}."
                    )
                    if self.retreive_next_chunk_id():
                        time.sleep(2)
                        continue

                    log.warning("The buffer is empty !!! Waiting for new data.")
                    time.sleep(20)
                except Exception as e:
                    log.error(
                        f"An error occurred while accessing S3. "
                        f"Bucket: {self.bucket}, Key: {object_key}, "
                        f"Stream Identifier: {self.stream_identifier}, Chunk ID: {self.chunk_id.value}. "
                        f"Error: {str(e)}"
                    )
                    log.exception(e)
                    time.sleep(5)

        except KeyboardInterrupt:
            log.info("Ctrl-C detected, terminating!")
            log.info("Cleaning up EndPoint process...")
            if ffmpeg_process and not ffmpeg_process.poll():
                ffmpeg_process.terminate()
                ffmpeg_process.stdin.close()
                ffmpeg_process.terminate()
                ffmpeg_process.wait()
            log.info("Terminated")

        except Exception as e:
            log.exception(e)


""" class ManagerEndPoint:
    def __init__(self):
        self.endpoint_list = []

    def add_endpoint(self, endpoint_alias, service_type, stream_key, streaming_event):
        for n in self.endpoint_list:
            if n.alias == endpoint_alias:
                return

        from django.db import connection

        connection.close()
        end_point = EndPoint(endpoint_alias, service_type, stream_key, streaming_event)
        end_point.start()
        from django.db import connection

        connection.close()

        self.endpoint_list.append(end_point)

    def manage_endpoints(self):
        streaming_events = StreamingEvent.objects.filter(
            delivering_activated=True, short_description="Main Stream"
        )
        for streaming_event in streaming_events:
            end_point_cfgs = streaming_event.end_points.filter(enabled=True)
            for end_point_cfg in end_point_cfgs:
                self.add_endpoint(
                    end_point_cfg.alias,
                    end_point_cfg.service_type,
                    end_point_cfg.stream_key,
                    streaming_event,
                )


restr_manager = ManagerEndPoint() """


def endpoints_info(endpoints):

    try:
        while True:
            buff_string = ""
            for n in endpoints:
                buff_string += f"{n.alias}: {n.buff_size.value / 1024 / 1024:.2f}MB (id:{n.chunk_id})|"
            log.debug(buff_string)

            time.sleep(10)
            log.debug("Endpoint on")
    except KeyboardInterrupt:
        log.info("Ctrl-C detected, terminating!")

    except Exception as e:
        log.exception(e)


class ManagerEndPointControl:
    def __init__(self):
        self.endpoint_processes = {}
        self.check_interval = 5
        self.stop_event = Event()
        self.signals = Queue()

    def add_signal(self, signal):
        self.signals.put(signal)

    def start_endpoint(self, alias, service_type, stream_key, stream_id, chunk_id):
        if alias in self.endpoint_processes:
            log.info(f"Endpoint {alias} is alredy running")
            return

        endpoint_process = EndPoint(alias, service_type, stream_key, stream_id, chunk_id)
        endpoint_process.start()
        self.endpoint_processes[alias] = endpoint_process
        log.info(f"Started endpoint {alias}")

    def stop_endpoint(self, alias):
        if alias in self.endpoint_processes:
            endpoint_process = self.endpoint_processes[alias]
            endpoint_process.terminate()
            endpoint_process.join()
            del self.endpoint_processes[alias]

        else:
            log.info(f"Endpoing {alias} is not running")

    def stop_all_endpoints(self):
        for alias in list(self.endpoint_processes.keys()):
            self.stop_endpoint(alias)
        log.info("All endpoints stopped.")

    def monitor_endpoints(self):
        while not self.stop_event.is_set():
            try:
                while not self.signals.empty():
                    signal = self.signals.get()
                    alias = signal["alias"]
                    action = signal["action"]  # 'start', 'stop', or 'stop_all'
                    service_type = signal.get("service_type")
                    stream_key = signal.get("stream_key")
                    stream_id = signal.get("stream_id")
                    chunk_id = signal.get("chunk_id")

                    if action == "start":
                        self.start_endpoint(alias, service_type, stream_key, stream_id, chunk_id)
                    elif action == "stop":
                        self.stop_endpoint(alias)
                    elif action == "stop_all":
                        self.stop_all_endpoints()

                time.sleep(self.check_interval)
            except Exception as e:
                print(f"An error occurred: {e}")

    def log_endpoints_info(self):
        try:
            while not self.stop_event.is_set():
                buff_string = ""
                for alias, process in self.endpoint_processes.items():
                    buff_string += (
                        f"{alias}: {process.buff_size.value / 1024 / 1024:.2f}MB (id:{process.chunk_id.value})|"
                    )
                log.debug(buff_string)
                time.sleep(10)
                log.debug("Endpoint is running")
        except KeyboardInterrupt:
            log.info("Ctrl-C detected, terminating!")
        except Exception as e:
            log.exception(e)

    def stop(self):
        self.stop_event.set()


endpoing_manger = ManagerEndPointControl()
