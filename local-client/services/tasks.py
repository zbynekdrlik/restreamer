import logging

import requests
from nl_restreamer.celery import app
from restreamer.models import ClientProfile, StreamingEvent

log = logging.getLogger("services")

# celery -A nl_restreamer worker -l INFO --pool=threads
# celery -A nl_restreamer beat -l debug


@app.task(name="services.tasks.check_stream_status")
def check_stream_status():
    url = "https://restreamer.newlevel.media/api/get_active_stream/"
    user = ClientProfile.objects.first()
    streaming_event = StreamingEvent.objects.first()
    user_uuid = user.user_id

    response = requests.get(url, params={"user_uuid": user_uuid})
    response_data = response.json()
    log.info(f"Response Data ---------->{response_data}")
    if response.status_code == 200 and response_data.get("identifier") and response_data.get("short_description"):
        streaming_event_name = response_data["short_description"]
        streaming_event_id = response_data["identifier"]
        # ptoreubujem tu poriesit to aby sa zrusil ten ktroy je tam vytvoreny len kvoli scriptu skriptu
        if not streaming_event:
            streaming_event = StreamingEvent(
                identifier=streaming_event_id,
                short_description=streaming_event_name,
                receiving_activated=True,
                delivering_activated=True,
            )

            log.info(f"Streaming event saved: {streaming_event_id} - {streaming_event_name}")
            streaming_event.save()
            return {"status": "success", "message": "Streaming event saved"}

        elif streaming_event.identifier != streaming_event_id:
            streaming_event.delete()
            log.info("Streaming Event Deleted !!!")

        elif not streaming_event.delivering_activated:
            streaming_event.delivering_activated = True
            streaming_event.save()
            log.info("Data sending enabled again !!")

        else:
            log.info("Streaming Event already created")

    elif response.status_code == 403:
        if streaming_event.delivering_activated:
            streaming_event.delivering_activated = False
            streaming_event.save()
        log.warning("Streaming Event is not activated")
        return {"status": "warning", "message": "Streaming Event is not activated"}

    elif response.status_code == 404:
        if response_data.get("warning") == "No streaming event found":
            log.warning("No streaming event found for user")
            if streaming_event:
                streaming_event.delete()
                log.info("Streaming Event deleted")
        else:
            log.error(f"Error: {response_data.get('error')}")
            return {"status": "error 404", "message": response_data.get("error")}
    else:
        log.error(f"Unexpected error: {response_data.get('error')}")
        return {"status": "error unexpected", "message": response_data.get("error")}


""" @app.task(name='services.tasks.get_buffer_duration')
def get_buffer_duration():
    url = "https://restreamer.newlevel.media/api/get_buffer_health/"
    streaming_event = StreamingEvent.objects.first()
    se_id = streaming_event.identifier

    chunk_record = ChunkRecord(streaming_event=streaming_event)
    duration = chunk_record.buffer_duration()
    log.info(f"duration -------> {duration}")

    data = {
        "streaming_event_id": se_id,
        "buffer_duration": duration
    }

    try:
        response = requests.post(url, json=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.json()
    except requests.exceptions.RequestException as e:
        return {"status": "error", "message": str(e)} """
