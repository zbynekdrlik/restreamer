import time
import logging
import requests
from celery import shared_task
from restreamer.data_sending import ChunkSender
from concurrent.futures import ThreadPoolExecutor
from django.core.management.base import BaseCommand
from restreamer.models import ChunkRecord, StreamingEvent
from restreamer.views.delivering import DeliveringManger
from restreamer.views.instances import InstanceManager
from django_celery_beat.models import PeriodicTask, IntervalSchedule
from restreamer.video_data import VideoDataManager
from django.conf import settings
from restreamer.video_data import VideoDataManager


log = logging.getLogger(__name__)

# celery -A nl_restreamer worker -l INFO --pool=threads -Q init_stream_queue


@shared_task(queue='init_stream_queue')
def init_stream(user_id, streaming_event_id, **kwargs):
    
    video_manger = VideoDataManager(streaming_event=streaming_event)
    init_chunk = video_manger.get_init_chunk_id()
    
    try:
        streaming_event = StreamingEvent.objects.get(id=streaming_event_id)
        DeliveringManger(user_id, streaming_event_id).send_init_data(init_chunk, kwargs.get("endpoint_id"))
    except Exception as e:
        log.exception(f'An error occurred: {e}')
        
        
@shared_task(queue='init_stream_queue', acks_late=True)
def end_stream(user_id, streaming_event, alias=None):
    try:
        manager = DeliveringManger(user_id, streaming_event)
        manager.end_delivery(alias)
    except Exception as e:
        log.exception(f'An error occurred: {e}')


# i dont now what is this 
@shared_task(queue='init_stream_queue', acks_late=True)
def enable_stream(streaming_event):
    video_manger = VideoDataManager(streaming_event=streaming_event)
    buffer = streaming_event.buffer
    while True:
        if not video_manger.mange_buffer(buffer):
            continue
        
        return True

