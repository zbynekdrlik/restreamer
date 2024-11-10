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

