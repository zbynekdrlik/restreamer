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
from services.discord_service import send_discord_bot_message
from services.youtube import get_control_stream_url_if_live


log = logging.getLogger(__name__)

credentials_path = settings.GOOGLE_CREDENTIALS_JSON_PATH
token_file = settings.GOOGLE_TOKEN_JSON_PATH
bot_token = settings.DISCORD_BOT_TOKEN
channel_id = settings.DISCORD_CHANNEL_ID
# celery -A nl_restreamer worker -l INFO --pool=threads -Q init_stream_queue


@shared_task(queue='init_stream_queue')
def init_stream(user_id, streaming_event_id, **kwargs):
    chunk_id = kwargs.get("chunk_id")
    try:
        streaming_event = StreamingEvent.objects.get(id=streaming_event_id)
        DeliveringManger(user_id, streaming_event_id).send_init_data(chunk_id, kwargs.get("endpoint_id"))
    except Exception as e:
        log.exception(f'An error occurred: {e}')
        
        
@shared_task(queue='init_stream_queue')
def end_stream(user_id, streaming_event, alias=None):
    try:
        manager = DeliveringManger(user_id, streaming_event.id)
        manager.end_delivery(alias)
    except Exception as e:
        log.exception(f'An error occurred: {e}')


# i dont now what is this 
@shared_task(queue='init_stream_queue')
def enable_stream(streaming_event):
    video_manger = VideoDataManager(streaming_event=streaming_event)
    buffer = streaming_event.buffer
    while True:
        if not video_manger.mange_buffer(buffer):
            continue
        
        return True

# start control stream that have only 10s in buffer
@shared_task(queue='init_stream_queue')
def init_fast_stream(streaming_event_id):
    
    streaming_event = StreamingEvent.objects.get(id=streaming_event_id)
    fast_stream = streaming_event.end_points.filter(is_fast=True).first()
    user = streaming_event.user.id
    if not fast_stream:
        return

    delivery_manager = DeliveringManger(user, streaming_event_id)
    
    while True:
        is_ready = delivery_manager.is_server_ready()
        if is_ready:
            # Fetch only the fifth most recent chunk
            chunks = (
                ChunkRecord.objects.filter(streaming_event=streaming_event)
                .order_by('-local_id')[4:5]
            )

            # Extract the chunk or None if not found
            fast_chunk = chunks.first() if chunks.exists() else None

            if fast_chunk:
                time.sleep(3)
                delivery_manager.send_init_data(fast_chunk.local_id, fast_stream.id)
                streaming_event.end_points.add(fast_stream)
                log.info(f"Fast stream {fast_stream.alias} initialized successfully !!!")
                check_yt_live.delay()
                return True
        
        time.sleep(3)


@shared_task(queue='init_stream_queue')
def check_yt_live():
    """
    Poll YouTube for the "Control Stream" to go live.
    Once live, send the link to Discord. 
    """

    youtube_url = None
    max_attempts = 5
    for attempt in range(max_attempts):
        youtube_url = get_control_stream_url_if_live(token_file, credentials_path)
        if youtube_url:
            log.info(f"Control Stream is LIVE at: {youtube_url}")
            send_discord_bot_message(
                bot_token,
                channel_id,
                f"The stream is now LIVE at {youtube_url}"
            )
            break
        else:
            log.info(f"Attempt {attempt + 1}/{max_attempts}: stream not live yet.")
            time.sleep(15)

    if not youtube_url:
        log.warning("Control Stream did not go live within expected time.")
    return