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
import queue 
from queue import PriorityQueue, Empty


from .shared import data_queue

log = logging.getLogger(__name__)



class EndPoint(multiprocessing.Process):
    def __init__(self, alias, service_type, stream_key):
        super().__init__(name=alias)
        self.alias = alias
        self.service_type = service_type
        self.stream_key = stream_key
        self.buff_size = multiprocessing.Value("L", 0)
        self.chunk_record_id = multiprocessing.Value("i", 0)
        self.reader_thread_terminate = Event()
        # self.stdout_thread = None
        self.stderr_thread = None
        self.last_processed_chunk_id = -1  # Indicates no chunks have been processed
        self.chunk_queue = PriorityQueue()

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

    def run(self):
        # Creating separate connection to db
        # log.debug('closing connection - wait 10s')
        # time.sleep(10)

        # log.debug('closing connection - move forward')

        ffmpeg_process = None

        try:
            log.info(f"Starting end point: {self.alias}")
            ffmpeg_process = self.run_ffmpeg()
            while True:
                time.sleep(0.1)

                # Verify if ffmpeg instance is still running
                ret = ffmpeg_process.poll()
                if ret is not None:
                    log.warning(f"Ffmpeg process has exited with code:{ret}!!!")
                    time.sleep(3)
                    ffmpeg_process = self.run_ffmpeg()
                    continue
                try:
                    while not data_queue.empty():
                        chunk_id, stream_identifier = data_queue.get_nowait()
                        self.chunk_queue.put((chunk_id, stream_identifier))
                        log.info(f"Adding chunk to queue: {chunk_id} | stream id --------- > {stream_identifier}")
                except Empty:
                    pass

                if not self.chunk_queue.empty():
                    next_chunk_id, next_stream_identifier = self.chunk_queue.queue[0]  # Peek the next chunk
                    log.info(f"Getting next chunk -----> | {next_chunk_id}")
                    if next_chunk_id == self.last_processed_chunk_id + 1:
                        self.chunk_queue.get()  # Remove the chunk from the queue
                        self.last_processed_chunk_id = next_chunk_id
                        log.info(f"Processing chunk_id ------> {next_chunk_id} | stream id --------- > {next_stream_identifier}")

                        s3 = settings.S3_CLIENT
                        bucket = settings.AWS_STORAGE_BUCKET_NAME

                        if not ffmpeg_process.poll():
                            try:
                                object_key = f"{next_chunk_id}_{next_stream_identifier}.bin"
                                log.info(f"Objcet key processed --------> | {object_key}")
                                response = s3.get_object(Bucket=bucket, Key=object_key)
                                if response:
                                    chunk_data = response['Body'].read()
                                    ffmpeg_process.stdin.write(chunk_data)
                                    ffmpeg_process.stdin.flush()
                                    self.buff_size.value += len(chunk_data)
                                else:
                                    log.warning("Chunk file not exists, skipping!")

                            except boto3.exceptions.S3UploadFailedError as e:
                                log.error(f"Error uploading chunk to S3: {e}")
                            except BotoCoreError as e:
                                log.error(f"Error: {e}")
                            except BrokenPipeError:
                                log.warning("Write to ffmpeg stdin unsuccessful")
                    else:
                        log.info("The buffer is empty waiting for new chunks....")
                        time.sleep(1)
                        
                        
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
                buff_string += f'{n.alias}: {n.buff_size.value / 1024 / 1024:.2f}MB (id:{n.chunk_record_id.value})|'
            log.debug(buff_string)
        
            time.sleep(10)
            log.debug("Endpoint on")
    except KeyboardInterrupt:
        log.info('Ctrl-C detected, terminating!')
        
    except Exception as e:
        log.exception(e)