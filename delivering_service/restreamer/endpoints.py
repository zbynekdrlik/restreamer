import json
import logging
import threading
import multiprocessing
import queue
import time
from queue import Empty, PriorityQueue
from threading import Event, Thread

import boto3
import ffmpeg
import requests
from boto3 import exceptions
from botocore import errorfactory
from botocore.exceptions import BotoCoreError
from django.conf import settings
from django.http import HttpResponseBadRequest, JsonResponse
from django.views.decorators.csrf import csrf_exempt

from .shared import data_queue

log = logging.getLogger(__name__)



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
            """   threads=2 b='4000k, audio_codec='aac' ,c='libx264',s='1280x720''"""
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
        # if self.stdout_thread:
        #     self.stdout_thread.join()
        if self.stderr_thread:
            self.stderr_thread.join()
        self.reader_thread_terminate.clear()

        process = cmd.run_async(pipe_stdin=True, pipe_stdout=False, pipe_stderr=True)
        time.sleep(5)

        # self.stdout_thread = Thread(target=self.reader_thread, args=(process.stdout, 'stdout'), daemon=True)
        # self.stdout_thread.start()
        self.stderr_thread = Thread(
            target=self.reader_thread, args=(process.stderr, "stderr"), daemon=True
        )
        self.stderr_thread.start()

        return process

        """     def get_last_chunk_position(self):
        try:
            ChunkRecord.objects.get(
                streaming_event=self.streaming_event,
                local_id=self.end_point_cfg.position_last,
            )
        except ChunkRecord.DoesNotExist:
            self.end_point_cfg.position_last = 0

            # Try to locate first chunk in db
            first_chunk = (
                ChunkRecord.objects.filter(streaming_event=self.streaming_event)
                .order_by("local_id")
                .first()
            )
            if first_chunk:
                self.end_point_cfg.position_last = first_chunk.local_id

            self.end_point_cfg.position_last -= 1
            self.end_point_cfg.save()
        return self.end_point_cfg.position_last

    def get_next_chunk_position(self):
        self.stored_position = self.get_last_chunk_position() + 1
        return self.stored_position """
    
    """ last_position = self.get_last_chunk_position()
    self.current_position = last_position + 1
    next_chunk = None
    while True:
        try:
            chunk_record = ChunkRecord.objects.get(
                streaming_event=self.streaming_event,
                local_id=self.current_position
            )
            return self.current_position
        except ChunkRecord.DoesNotExist:
            if next_chunk is None:
                next_chunk = ChunkRecord.objects.select_related('streaming_event').filter(local_id__gt=self.current_position).order_by('local_id').first()
                if next_chunk is None:
                    return None
                self.current_position = next_chunk.local_id
            else:
                    self.current_position += 1 """ 

   
    def reader_thread(self, pipe, pipe_name):
        try:
            with open(
                f'{self.alias.replace(" ","_")}_{pipe_name}.log', "ab"
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
            # pipe.close()
            pass
        except Exception as e:
            log.exception(e)
            
            
    def process_chunk(self, ffmpeg_process):
        s3 = settings.S3_CLIENT
        bucket = settings.AWS_STORAGE_BUCKET_NAME
        if not ffmpeg_process.poll():
            try:
                object_key = f"{self.chunk_id}_{self.stream_identifier}.bin"
                response = s3.get_object(Bucket=bucket, Key=object_key)
                if response:
                    chunk_data = response['Body'].read()
                    ffmpeg_process.stdin.write(chunk_data)
                    ffmpeg_process.stdin.flush()
                    self.buff_size.value += len(chunk_data)
                else:
                    log.warning("Chunk file not exists, skipping!")
            except s3.exceptions.NoSuchKey as e:
                log.warning(f"The buffer is empty!! Waiting for new data.")
                time.sleep(2)
            except boto3.exceptions.S3UploadFailedError as e:
                log.error(f"Error uploading chunk to S3: {e}")
            except BotoCoreError as e:
                log.error(f"Error: {e}")
            except BrokenPipeError:
                log.warning("Write to ffmpeg stdin unsuccessful")
            


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
                    ffmpeg_process = self.run_ffmpeg()
                    continue

                if self.chunk_id is None:
                    continue
                
                self.process_chunk(ffmpeg_process)
                self.chunk_id += 1
                
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
            buff_string = ''
            for n in endpoints:
                buff_string += f'{n.alias}: {n.buff_size.value / 1024 / 1024:.2f}MB (id:{n.chunk_id})|'
            log.debug(buff_string)
        
            time.sleep(10)
            log.debug("Endpoint on")
    except KeyboardInterrupt:
        log.info('Ctrl-C detected, terminating!')
        
    except Exception as e:
        log.exception(e)
        
        
        
class ManagerEndPointControl:
    def __init__(self):
        self.endpoint_processes = {}
        self.check_interval = 10
        self.stop_event = Event()
        self.signals = []

    def add_signal(self, signal):
        self.signals.append(signal)
        
    def start_endpoint(self, alias, service_type, stream_key, stream_id, chunk_id):
        if alias in self.endpoint_processes:
            log.info(f'Endpoint {alias} is alredy running')
            return
        
        endpoint_process = EndPoint(alias, service_type, stream_key, stream_id, chunk_id)
        endpoint_process.start()
        self.endpoint_processes[alias] = endpoint_process
        log.info(f'Started endpoint {alias}')
        
        
    def stop_endpoint(self, alias):
        if alias in self.endpoint_processes:
            endpoint_process = self.endpoint_processes[alias]
            endpoint_process.terminate()
            endpoint_process.join()
            del self.endpoint_processes[alias]
            print(f"Stopped endpoint {alias}")

        else:
            log.info(f'Endpoing {alias} is not running')
            
    def stop_all_endpoints(self):
        for alias in list(self.endpoint_processes.keys()):
            self.stop_endpoint(alias)
        log.info("All endpoints stopped.")
        
    def wait_for_signal(self, alias):
        print(f"{alias} is now waiting for a signal to restart.")
        while alias not in self.endpoint_processes:
            time.sleep(self.check_interval)
        print(f"{alias} received signal to restart.")
        
    def monitor_endpoints(self):
        while not self.stop_event.is_set():
            try:
                while self.signals:
                    signal = self.signals.pop(0)
                    alias = signal['alias']
                    action = signal['action']  # 'start', 'stop', or 'stop_all'
                    service_type = signal.get('service_type')
                    stream_key = signal.get('stream_key')
                    stream_id = signal.get('stream_id')
                    chunk_id = signal.get('chunk_id')

                    if action == 'start':
                        self.start_endpoint(alias, service_type, stream_key, stream_id, chunk_id)
                    elif action == 'stop':
                        self.stop_endpoint(alias)
                        threading.Thread(target=self.wait_for_signal, args=(alias,), daemon=True).start()
                    elif action == 'stop_all':
                        self.stop_all_endpoints()
                        for alias in list(self.endpoint_processes.keys()):
                            threading.Thread(target=self.wait_for_signal, args=(alias,), daemon=True).start()

                time.sleep(self.check_interval)
            except Exception as e:
                print(f"An error occurred: {e}")
                
    def log_endpoints_info(self):
        try:
            while not self.stop_event.is_set():
                buff_string = ''
                for alias, process in self.endpoint_processes.items():
                    buff_string += f'{alias}: {process.buff_size.value / 1024 / 1024:.2f}MB (id:{process.chunk_id.value})|'
                log.debug(buff_string)
                time.sleep(10)
                log.debug("Endpoint is running")
        except KeyboardInterrupt:
            log.info('Ctrl-C detected, terminating!')
        except Exception as e:
            log.exception(e)

    def stop(self):
        self.stop_event.set()         
        
               
endpoing_manger = ManagerEndPointControl()