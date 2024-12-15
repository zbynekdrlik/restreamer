import importlib
import logging
import threading
import time
import redis
import requests
from restreamer.models import ChunkRecord


importlib.invalidate_caches()


log = logging.getLogger(__name__)

redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)


class ChunkSender:
    def __init__(self, streaming_event):
        self.stored_position = 0
        self.streaming_event = streaming_event
        self.streaming_event_identifier = streaming_event.identifier
        self.api_url = f"https://restreamer.newlevel.media/chunk-upload/"
        self.check_chunk_url = f"https://restreamer.newlevel.media/api/check-chunk/"

    def get_last_chunk_position(self):
        try:
            ChunkRecord.objects.get(id=self.stored_position, in_process=False, send=False)

        except ChunkRecord.DoesNotExist:
            self.stored_position = 0
            first_chunk = ChunkRecord.objects.filter(streaming_event=self.streaming_event, in_process=False,
                                                     send=False).first()
            if first_chunk:
                self.stored_position = first_chunk.id

        return self.stored_position

    def get_next_chunk_position(self):
        self.stored_position = self.get_last_chunk_position() + 1
        return self.stored_position

    def sending_chunks(self):
        while True:
            time.sleep(0.1)
            self.streaming_event.refresh_from_db()
            if not self.streaming_event.delivering_activated:
                log.info(f'Shutting down')
                return
            redis_client.rpush('endpoint_icon_status', 'endpoint_active')
            try:
                next_chunk_position = self.get_last_chunk_position()
                chunk_record = ChunkRecord.objects.get(id=next_chunk_position)
                if chunk_record.chunk_file.name:
                    chunk_path = chunk_record.chunk_file.path

                    with open(chunk_path, "rb") as f:
                        chunk_data = f.read()

                    chunk_id = {"chunk_id": int(chunk_record.id)}
                    chunk_data = {"chunk_data": chunk_data}
                    log.info(
                        f"Chunks in buffer: {ChunkRecord.objects.all().count()}"
                    )
                    # chunk_record.delete()
                    while True:
                        active_thread_count = threading.active_count()
                        if active_thread_count < 4:
                            break
                        time.sleep(0.2)
                    log.info(f"Active threads: {active_thread_count}")
                    chunk = ChunkRecord.objects.get(id=chunk_id["chunk_id"])
                    t1 = threading.Thread(
                        target=self.chunk_send_thread,
                        args=(chunk_id, chunk_data, chunk),
                    )
                    chunk.in_process = True
                    chunk.save()
                    t1.start()

            except KeyboardInterrupt:
                log.info('Ctrl-C detected, terminating!')
                log.info('Koniec simulácie odosielania chunkov.')
                time.sleep(1)
                raise
            except ChunkRecord.DoesNotExist:
                log.info(
                    f"The buffer is empty, waiting for the next chunk to be sent to -- {self.api_url}"
                )
                time.sleep(1)
                continue
            except:
                log.exception(f"Error while sending chunk")
                continue

    def chunk_send_thread(self, chunk_id, chunk_data, chunk):
        chunk_identifier = {"chunk_identifier": self.streaming_event_identifier}
        identifier = f"{chunk_id}_{chunk_identifier}.bin"
        
        while True:
            try:
                upload_to_s3(chunk_data, identifier)
                log.info(f"S3 upload for chunk {chunk_id} succeeded!")
                break
            except Exception as e:
                log.warning(f"S3 upload failed for chunk {chunk_id}: {e}")
                time.sleep(3) 
            
        retries = 0
        while True:
            if retries == 1:
                if self.check_chunk_server(chunk_id):
                    log.info(f"Chunk{chunk_id} sent successfully ------------------ Chunk arrived ---------!")
                    chunk.send = True
                    chunk.save()
                    return
            retries += 1
            self.streaming_event.refresh_from_db()
            if not self.streaming_event.delivering_activated:
                log.info(f'Shutting down chunk send thread: {chunk_id}')
                return
            try:
                data_payload = {**chunk_id, **chunk_identifier}
                response = requests.post(
                    self.api_url, data=data_payload, timeout=5
                )
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                log.warning(f"Lost internet connection and {e}")
                log.info(f"Checking if reaceived {chunk_id} ...")
                if self.check_chunk_server(chunk_id):
                    log.info(f"Chunk{chunk_id} sent successfully ------------------ Chunk arrived ---------!")
                    chunk.send = True
                    chunk.save()
                    return
                time.sleep(3)
                continue

            if response.status_code == 200:
                log.info(f"Chunk{chunk_id} sent successfully!")
                # chunk_record.delete()
                chunk.send = True
                chunk.delete()
                break
            else:
                log.error(
                    f"Failed to send chunk{chunk_id}. Status code: {response.status_code}"
                )
                log.info(f"Retrying Chunk{chunk_id} ...")
                time.sleep(1)

    def check_chunk_server(self, chunk_id):
        chunk = ChunkRecord.objects.get(id=chunk_id['chunk_id'])
        check_sum = chunk.md5
        id = {'md5': check_sum}

        try:
            response = requests.post(self.check_chunk_url, data=id, timeout=1)
            response.raise_for_status()
            chunk_exists = response.json()['chunk_exists']
            return chunk_exists
        
        except requests.exceptions.RequestException as e:
            print(f"Error checking chunk on server: {e}")
            return False
    
    def upload_to_s3(self, chunk_data, filename):
        try:
            bucket_name = os.environ.get('AWS_STORAGE_BUCKET_NAME')
            client = settings.S3_CLIENT

            client.put_object(Body=chunk_data,
                            Bucket= bucket_name,
                            Key=filename,)
        except Exception as e:
            log.exception(e)
        
