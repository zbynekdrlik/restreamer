import hashlib
import logging
import threading
import time
from collections import deque
from threading import Event, Thread

import ffmpeg
import redis
from django.core.files.base import ContentFile

from restreamer.models import ChunkRecord, StreamingEvent

log = logging.getLogger(__name__)
redis_client = redis.StrictRedis(host="localhost", port=6379, db=0)


class InPoint(threading.Thread):
    def __init__(self, name, port, streaming_event):
        super().__init__()
        self.name = name
        self.port = port
        self.streaming_event = streaming_event
        self.buff_size = 0
        self.chunk_record_id = 0
        self.control_queue = deque()
        self.reader_thread_terminate = Event()
        self.stderr_thread = None

    def run_ffmpeg(self):
        #    input_url = (
        #         f"srt://0.0.0.0:{self.port}?pkt_size=1316&mode=listener&transtype=live&latency=3000000&linger=10"
        #         f"&ffs=128000&rcvbuf=100058624"
        #     )

        input_url = f"rtmp://0.0.0.0:{self.port}"

        cmd = ffmpeg.input(
            input_url,
            loglevel="debug",
            readrate=1.00,
            listen=1,
        ).output("pipe:", f="mpegts", c="copy")
        log.info("Starting new instance of ffmpeg...")
        log.debug(" ".join(cmd.compile()))

        self.reader_thread_terminate.set()
        if self.stderr_thread:
            self.stderr_thread.join()
        self.reader_thread_terminate.clear()
        process = cmd.run_async(pipe_stdin=False, pipe_stdout=True, pipe_stderr=True)

        self.stderr_thread = Thread(
            target=self.reader_thread,
            args=(process.stderr, "stderr"),
            daemon=True,
        )
        self.stderr_thread.start()

        return process

    def run(self):
        buff_temp_size = 0
        chunks_received = deque()
        ffmpeg_process = None

        try:
            ffmpeg_process = self.run_ffmpeg()
            last_time = time.time()

            while True:
                time.sleep(0.01)
                ret = ffmpeg_process.poll()
                if ret is not None:
                    log.warning(f"\nFfmpeg process has exited with code:{ret}!!!\n")
                    time.sleep(3)
                    ffmpeg_process = self.run_ffmpeg()

                chunk = ffmpeg_process.stdout.read(1024 * 100)
                if not chunk:
                    redis_client.rpush("inpoint_icon_status", "inpoint_active")
                    continue

                chunks_received.append(chunk)
                buff_temp_size += len(chunk)

                if buff_temp_size and time.time() - last_time >= 1:
                    big_chunk = bytearray()
                    for n in chunks_received:
                        big_chunk.extend(n)
                    chunk_record = ChunkRecord(data_size=buff_temp_size, streaming_event=self.streaming_event)
                    md5_hash = hashlib.md5()
                    md5_hash.update(big_chunk)
                    chunk_record.md5 = md5_hash.hexdigest()

                    chunk_record.save()

                    content = ContentFile(big_chunk)
                    while True:
                        try:
                            chunk_record.chunk_file.save(f"{chunk_record.id}.bin", content)
                        except OSError as e:
                            log.exception(e)
                            log.warning("Please fix it immediately!!!")
                            chunk_record.backup_path = "backup_chunks"
                            continue
                        break

                    self.chunk_record_id = chunk_record.id
                    self.streaming_event = StreamingEvent.objects.get(id=self.streaming_event.id)
                    self.streaming_event.refresh_from_db()
                    if not self.streaming_event.receiving_activated:
                        ffmpeg_process.terminate()
                        ffmpeg_process.wait()
                        return

                    self.streaming_event.received_bytes += buff_temp_size
                    self.streaming_event.save()

                    self.buff_size += buff_temp_size
                    buff_temp_size = 0
                    chunks_received.clear()
                    last_time = time.time()

        except KeyboardInterrupt:
            log.info("Ctrl-C detected, terminating!")
            log.info("Cleaning up InPoint process...")
            if ffmpeg_process:
                ffmpeg_process.terminate()
                ffmpeg_process.wait()
            log.info("Done")

        except Exception as e:
            log.exception(e)

    def reader_thread(self, pipe, pipe_name):
        try:
            with open(
                f"{self.streaming_event.short_description.replace(' ', '_')}_{pipe_name}.log",
                "a",
                encoding="utf-8",
            ) as logfile:
                while True:
                    if self.reader_thread_terminate.is_set():
                        logging.debug("Terminating reader_thread.")
                        break
                    data = pipe.read(4096)  # Adjust the buffer size as needed
                    if not data:
                        logging.warning("Terminating reader_thread function!")
                        break

                    try:
                        decoded_data = data.decode("utf-8")
                    except UnicodeDecodeError:
                        decoded_data = repr(data)

                    # Log the decoded data
                    logging.info(decoded_data)

                    # Optionally, write the decoded data to the log file
                    logfile.write(decoded_data)
                    logfile.flush()

        except KeyboardInterrupt:
            # Handle KeyboardInterrupt if needed
            pass
        except Exception as e:
            logging.exception(e)
