import logging
import json
import multiprocessing
import time
from threading import Event, Thread
from django.http import JsonResponse, HttpResponseBadRequest
from django.views.decorators.csrf import csrf_exempt
import boto3
import ffmpeg
from boto3 import exceptions
from botocore.exceptions import BotoCoreError
from django.conf import settings
from restreamer.models import ChunkRecord, EndPointCfg, StreamingEvent
import queue

data_queue = queue.Queue()

log = logging.getLogger(__name__)


class EndPoint(multiprocessing.Process):
    def __init__(self, alias, service_type, stream_key):
        super().__init__(name=alias)
        self.alias = alias
        self.service_type = service_type
        self.stream_key = stream_key
        self.buff_size = multiprocessing.Value("L", 0)
        self.chunk_record_id = multiprocessing.Value("i", 0)
        self.stored_position = 0
        self.reader_thread_terminate = Event()
        self.stderr_thread = None
        self.ffmpeg_process = None

    def run_ffmpeg(self):
        if self.service_type == "YT_HLS":
            output_url = f"https://a.upload.youtube.com/http_upload_hls?cid={self.stream_key}&copy=0&file=out1248.ts"
            cmd = ffmpeg.input(
                "pipe:",
                readrate=1.00,
                format="mpegts",
                loglevel="info",
            ).output(
                output_url,
                f="hls",
                hls_segment_type="mpegts",
                hls_segment_options="mpegts_flags=+pat_pmt_at_frames+resend_headers",
                hls_list_size=5,
                hls_time=2,
                hls_flags="delete_segments",
                start_number=0,
                hls_playlist_type="event",
                method="PUT",
                c="copy",
                flags="+cgop",
            )
        elif self.service_type == "FB":
            output_url = f"rtmps://live-api-s.facebook.com:443/rtmp/{self.stream_key}"
            cmd = ffmpeg.input(
                "pipe:",
                readrate=1.00,
                format="mpegts",
                loglevel="info",
            ).output(output_url, f="flv", c="copy")

        elif self.service_type == "YT_RTMP":
            output_url = f"rtmp://a.rtmp.youtube.com/live2/{self.stream_key}"
            cmd = (
                ffmpeg.input("pipe:", format="mpegts", readrate=1.00, loglevel="info")
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
            ).output(output_url, f="flv", c="copy")
        else:
            log.error(f"Unsupported service type: {self.service_type}")
            raise ValueError

        log.info(f"Starting new instance of end point ffmpeg for {self.alias}")
        log.debug(" ".join(cmd.compile()))

        self.reader_thread_terminate.set()
        if self.stderr_thread:
            self.stderr_thread.join()
        self.reader_thread_terminate.clear()

        self.ffmpeg_process = cmd.run_async(pipe_stdin=True, pipe_stdout=False, pipe_stderr=True)
        time.sleep(5)

        self.stderr_thread = Thread(
            target=self.reader_thread, args=(self.ffmpeg_process.stderr, "stderr"), daemon=True
        )
        self.stderr_thread.start()

    def reader_thread(self, pipe, pipe_name):
        try:
            with open(
                    f'{self.alias.replace(" ", "_")}_{pipe_name}.log', "ab"
            ) as logfile:
                line = bytearray()
                while True:
                    if self.reader_thread_terminate.is_set():
                        log.debug("Terminating reader_thread.")
                        break
                    char = pipe.read(1)
                    line.extend(char)
                    if char in b"\r\n":
                        log_entry = f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {line.decode('utf-8', errors='ignore')}"
                        logfile.write(log_entry.encode('utf-8'))
                        logfile.flush()
                        line.clear()
                    elif char == b"":
                        log.warning("Terminating reader_thread function!")
                        break
        except KeyboardInterrupt:
            pass
        except Exception as e:
            log.exception(e)

    def run(self):
        try:
            log.info(f"Starting end point: {self.alias}")
            self.run_ffmpeg()
            while True:
                time.sleep(0.1)
                ret = self.ffmpeg_process.poll()
                if ret is not None:
                    log.warning(f"Ffmpeg process has exited with code: {ret}!!!")
                    time.sleep(3)
                    self.run_ffmpeg()

        except KeyboardInterrupt:
            log.info("Ctrl-C detected, terminating!")
            log.info("Cleaning up EndPoint process...")
            if self.ffmpeg_process and not self.ffmpeg_process.poll():
                self.ffmpeg_process.terminate()
                self.ffmpeg_process.stdin.close()
                self.ffmpeg_process.terminate()
                self.ffmpeg_process.wait()
            log.info("Terminated")

        except Exception as e:
            log.exception(e)

endpoints = {}

@csrf_exempt
def start_endpoint(request):
    if request.method == 'POST':
        data = json.loads(request.body)
        alias = data['alias']
        service_type = data['service_type']
        stream_key = data['stream_key']
        
        if alias in endpoints:
            return JsonResponse({"error": "Endpoint already exists"}, status=400)
        
        endpoint = EndPoint(alias, service_type, stream_key)
        endpoint.start()
        endpoints[alias] = endpoint
        
        return JsonResponse({"status": "started"}, status=200)
    
    return HttpResponseBadRequest("Invalid request method")

@csrf_exempt
def send_chunk(request):
    if request.method == 'POST':
        alias = request.GET.get('alias')
        if alias not in endpoints:
            return JsonResponse({"error": "Endpoint not found"}, status=404)
        
        chunk_data = request.body
        endpoint = endpoints[alias]
        
        if endpoint.ffmpeg_process:
            try:
                endpoint.ffmpeg_process.stdin.write(chunk_data)
                endpoint.ffmpeg_process.stdin.flush()
                return JsonResponse({"status": "chunk sent"}, status=200)
            except BrokenPipeError:
                return JsonResponse({"error": "Broken pipe"}, status=500)
        
        return JsonResponse({"error": "FFmpeg process not running"}, status=500)
    
    return HttpResponseBadRequest("Invalid request method")
