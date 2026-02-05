import importlib
import logging
import time
import threading
import requests
from restreamer.models import ChunkRecord

from .models import StreamingEvent
from restreamer.views.delivering import DeliveringManger
importlib.invalidate_caches()


log = logging.getLogger(__name__)


class ChunkSender:
    def __init__(self, streaming_event, user_id=None):
        server_addres = DeliveringManger(user_id, streaming_event.id).get_url()
        self.stored_position = 0
        self.streaming_event = streaming_event
        self.api_url = f"http://{server_addres}/api/receive_data/"

    def get_last_chunk_position(self):
        try:
            ChunkRecord.objects.get(local_id=self.stored_position, in_process=False, send=False)
           
        except ChunkRecord.DoesNotExist:
            self.stored_position = 0
            first_chunk = ChunkRecord.objects.filter(streaming_event=self.streaming_event, in_process=False,send=False).first()
            if first_chunk:
                self.stored_position = first_chunk.local_id
        return self.stored_position

    def get_next_chunk_position(self):
        self.stored_position = self.get_last_chunk_position() + 1
        return self.stored_position

    def sending_chunks(self):
        while True:
            time.sleep(1)
            self.streaming_event.refresh_from_db()
            if not self.streaming_event.delivering_activated:
                log.info(f'Shutting down')
                return
            try:
                next_chunk_position = self.get_last_chunk_position()
                log.info(f"next chunk position {next_chunk_position}")
                chunk_record = ChunkRecord.objects.get(local_id=next_chunk_position)
                chunk_id = {"chunk_id": int(chunk_record.local_id)}
                
                log.info(f"Chunks in buffer: {ChunkRecord.objects.all().count()}" )   
                
                while True:
                    active_thread_count = threading.active_count()
                    if active_thread_count < 7:
                        break
                    time.sleep(0.2) 
                    
                log.info(f"Active threads: {active_thread_count}")   
                chunk = ChunkRecord.objects.get(local_id=chunk_id["chunk_id"])
                t1 = threading.Thread(
                    target=self.chunk_send_thread,
                    args=(chunk_id, chunk)
                )
                chunk.in_process = True
                chunk.save()
                t1.start()
                        
                        
            except KeyboardInterrupt:
                log.info('Ctrl-C detected, terminating!' )
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
            
            
    def chunk_send_thread(self, chunk_id, chunk):
        user_proof = {"stream_id": self.streaming_event.identifier}
        while True:
            self.streaming_event.refresh_from_db()
            if not self.streaming_event.delivering_activated:
                log.info(f'Shutting down chunk send thread: {chunk_id}')
                return      
            try:
                data_payload = {"chunk_id": str(chunk_id)}
                params = {**data_payload, **user_proof}
                response = requests.post(
                    self.api_url, params=params, timeout=5
                )
            except requests.RequestException as e:
                log.warning(f"Lost internet connection and {e}")
                log.info(f"Retrying Chunk{chunk_id} ...")
                time.sleep(3)
                continue
                
            if response.status_code == 200:
                log.info(f"Chunk{chunk_id} sent successfully!")
                #chunk_record.delete()
                chunk.send = True
                chunk.save()
                break
            else:
                log.error(
                    f"Failed to send chunk{chunk_id}. Status code: {response.status_code}"
                )
                log.info(f"Retrying Chunk{chunk_id} ...")
                time.sleep(1)