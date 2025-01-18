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
    chunk_id = kwargs.get("chunk_id")
    try:
        streaming_event = StreamingEvent.objects.get(id=streaming_event_id)
        DeliveringManger(user_id, streaming_event).send_init_data(chunk_id, kwargs.get("endpoint_id"))
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

# start control stream that have only 10s in buffer
@shared_task(queue='init_stream_queue', acks_late=True)
def init_fast_stream(streaming_event_id):
    log.info('init_fast_stream function called')
    streaming_event = StreamingEvent.objects.get(id=streaming_event_id)
    fast_stream = streaming_event.end_points.filter(is_fast=True).first()
    user = streaming_event.user.id
    if not fast_stream:
        return
 
    delivery_manager = DeliveringManger(user_id=user, streamign_event=streaming_event)
    instance_manager = InstanceManager(user)
    
    while True:
        is_active = instance_manager.check_status() == 'running'
        log.info(f'is active ----------> {is_active}')
        if is_active:
            chunks = ChunkRecord.objects.filter(streaming_event=streaming_event).order_by('-id')  # Order by descending ID
            fast_chunk = chunks[4] if chunks.count() >= 5 else None
            time.sleep(5)
            if fast_chunk:
                delivery_manager.send_init_data(fast_chunk.id, fast_stream.id)
                streaming_event.end_points.add(fast_stream)
                log.info(f"Fast stream {fast_stream.alias} initialized successfuly !!!")
                return
        time.sleep(3)
    