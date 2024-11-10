import time
import logging
import requests
from celery import shared_task
from restreamer.data_sending import ChunkSender
from concurrent.futures import ThreadPoolExecutor
from django.core.management.base import BaseCommand
from restreamer.models import ChunkRecord, StreamingEvent
from restreamer.views.delivering import DeliveringManger
from restreamer.video_data import VideoDataManager
from django.conf import settings
from restreamer.video_data import VideoDataManager


log = logging.getLogger(__name__)

# celery -A nl_restreamer worker -l INFO --pool=threads -Q init_stream_queue

@shared_task(queue='streaming_queue', acks_late=True)
def start_delivering(streaming_event, user_id):
    while True:
        streaming_event_id = StreamingEvent.objects.get(id=streaming_event)
        ChunkRecord.objects.filter(streaming_event=streaming_event, in_process=True).update(in_process=False)  # Chunks are redy to be send again.
        if not streaming_event_id.delivering_activated:
            log.info("Waiting until it is active")
            log.debug("Press start buttom")
            time.sleep(5)
            continue

        while True:
            try:
                while True:                   
                    first_chunk = ChunkRecord.objects.filter(streaming_event=streaming_event_id).first()
                    
                    if not streaming_event_id.delivering_activated:
                        log.info("Shutting down")
                        break
                    if first_chunk is not None:
                        start_endpoint = ChunkSender(streaming_event_id, user_id)                
                        start_endpoint.sending_chunks()
                                                    
                    else:
                        log.info('No available chunks for this streaming event.')
                        log.info("Waiting for new chunks")
                        break
                if not streaming_event_id.delivering_activated:
                    log.info("Shutting down")
                    break
                
                time.sleep(5)
                continue
            
            except KeyboardInterrupt:
                log.info('Ctrl-C detected, terminating!')
                log.info('Koniec simulácie odosielania chunkov.')
                time.sleep(1)
                raise
            
            
@shared_task(queue='init_stream_queue', acks_late=True)
def init_stream(user_id, streaming_event_id, **kwargs):
    chunk_id = kwargs.get("chunk_id")
    print("We are there 57 ------------------------------------------------------")
    try:
        streaming_event = StreamingEvent.objects.get(id=streaming_event_id)
        DeliveringManger(user_id, streaming_event).send_init_data(chunk_id, kwargs.get("endpoint_id"))
    except Exception as e:
        print(f'An error occurred: {e}')
        
        
@shared_task(queue='init_stream_queue', acks_late=True)
def end_stream(user_id, streaming_event, alias=None):
    try:
        manager = DeliveringManger(user_id, streaming_event)
        manager.end_delivery(alias)
    except Exception as e:
        print(f'An error occurred: {e}')


@shared_task(queue='init_stream_queue', acks_late=True)
def enable_stream(streaming_event):
    video_manger = VideoDataManager(streaming_event=streaming_event)
    buffer = streaming_event.buffer
    while True:
        if not video_manger.mange_buffer(buffer):
            continue
        
        return True
    
@shared_task(queue='services', acks_late=True)
def is_buffer_ready_action(streaming_event_id):
    streaming_event = StreamingEvent.objects.get(id=streaming_event_id)
    data_manager = VideoDataManager(streaming_event.id)

    while True:
        if data_manager.is_buffer_filled(streaming_event.buffer):
            url = f"{settings.BASE_URL}/control/deliverig_action/{streaming_event_id}/"
            log.info("URL: %s", url)

            headers = {
                "Authorization": f"Bearer {settings.CRON_SECRET_TOKEN}",
                "Content-Type": "application/json"
            }

            payload = {
                "user_id": streaming_event.user.id,  # Assuming StreamingEvent has a user field
                "streaming_event_id": streaming_event_id
            }

            try:
                response = requests.post(url, headers=headers, json=payload)
                response.raise_for_status()
                log.info("Stream initialized successfully.")
            except requests.exceptions.RequestException as e:
                log.exception(f"Failed to initialize the stream: {e}")

            return True
        
        time.sleep(5)


