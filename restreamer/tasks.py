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
from django.contrib.auth import get_user_model
from services.youtube.client import get_active_live_broadcasts


log = logging.getLogger(__name__)


bot_token = settings.DISCORD_BOT_TOKEN
channel_id = settings.DISCORD_CHANNEL_ID
# celery -A nl_restreamer worker -l INFO --pool=threads -Q init_stream_queue


@shared_task(queue='init_stream_queue')
def init_stream(user_id, streaming_event_id, **kwargs):
    chunk_id = kwargs.get('chunk_id', None)
    try:
        streaming_event = StreamingEvent.objects.get(id=streaming_event_id)
        video_manger = VideoDataManager(streaming_event.id)
        init_chunk = video_manger.get_init_chunk_id() if not chunk_id else chunk_id
        DeliveringManger(user_id, streaming_event_id).send_init_data(init_chunk, kwargs.get("endpoint_id"))
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
    video_manger = VideoDataManager(streaming_event.id)
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
    
    user_id = streaming_event.user.id
    
    if not fast_stream:
        return

    delivery_manager = DeliveringManger(user_id, streaming_event_id)
    
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
                check_yt_live.delay(user_id)
                return True
        
        time.sleep(3)


@shared_task(queue='init_stream_queue')
def check_yt_live(user_id):
    """
    Poll up to 5 times, checking if the 'Control Stream' broadcast is live.
    If found, send the link to Discord.
    """
    
    
    max_attempts = 5
    delay_seconds = 15

    User = get_user_model()
    try:
        user = User.objects.get(id=user_id)
    except User.DoesNotExist:
        log.error(f"User {user_id} not found.")
        return None

    
    for attempt in range(max_attempts):
        items = get_active_live_broadcasts(user)
        log.info(f"Attempt {attempt+1}/{max_attempts}: Found {len(items)} active broadcast(s).")
        found_live = False
        for item in items:
            title = item['snippet']['title']
            life_cycle_status = item['status']['lifeCycleStatus']
            log.info(f"Broadcast '{title}' status={life_cycle_status}")

            if title == "Control Stream" and life_cycle_status == 'live':
                broadcast_id = item['id']
                youtube_url = f"https://www.youtube.com/watch?v={broadcast_id}"
                log.info(f"Control Stream is LIVE at: {youtube_url}")

                # Post to Discord
                bot_token = settings.DISCORD_BOT_TOKEN
                channel_id = settings.DISCORD_CHANNEL_ID
                message = f"The stream is now LIVE at {youtube_url}"
                send_discord_bot_message(bot_token, channel_id, message)

                found_live = True
                break  # Stop checking other broadcasts if we found the one we want

        if found_live:
            return youtube_url

        # Not found or not live yet; wait before next attempt
        if attempt < max_attempts - 1:
            log.info(f"Not live yet. Retrying in {delay_seconds} seconds...")
            time.sleep(delay_seconds)

    log.warning("No 'Control Stream' broadcast became live after 5 attempts.")
    return None