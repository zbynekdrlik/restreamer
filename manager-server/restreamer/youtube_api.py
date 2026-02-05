import os
import pickle

import googleapiclient.discovery
import googleapiclient.errors
from google.auth.transport.requests import Request

# Define the path to your client_secret.json file
CLIENT_SECRETS_FILE = "/root/kristian/manager-server/restreamer-manager/api/kristian_test.json"
CREDENTIALS_PICKLE_FILE = "/root/kristian/manager-server/restreamer-manager/api/credentials.pickle"

# This OAuth 2.0 access scope allows an application to manage the authenticated user's YouTube channel.
SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]


def get_credentials():
    credentials = None
    if os.path.exists(CREDENTIALS_PICKLE_FILE):
        with open(CREDENTIALS_PICKLE_FILE, "rb") as token:
            credentials = pickle.load(token)

    # Check if the credentials are valid or need to be refreshed
    if credentials and credentials.expired and credentials.refresh_token:
        credentials.refresh(Request())
    elif not credentials or not credentials.valid:
        raise Exception("The existing credentials are invalid or expired. Please reauthenticate.")

    return credentials


def create_youtube_client(credentials):
    return googleapiclient.discovery.build("youtube", "v3", credentials=credentials)


def create_broadcast(youtube, title, description, scheduled_time, status):
    broadcast_body = {
        "snippet": {"title": title, "description": description, "scheduledStartTime": scheduled_time},
        "status": {
            "privacyStatus": status  # Can be "public", "private" or "unlisted"
        },
    }

    broadcast_response = youtube.liveBroadcasts().insert(part="snippet,status", body=broadcast_body).execute()

    return broadcast_response["id"]


def create_stream(youtube):
    stream_body = {
        "snippet": {"title": "Test Stream", "description": "This is a test stream"},
        "cdn": {
            "format": "1080p",  # Stream resolution
            "ingestionType": "rtmp",  # RTMP ingestion type
            "resolution": "1080p",  # Add resolution attribute
            "frameRate": "30fps",  # Add frame rate attribute
        },
    }

    stream_response = youtube.liveStreams().insert(part="snippet,cdn", body=stream_body).execute()

    return stream_response["id"]


def bind_broadcast_to_stream(youtube, broadcast_id, stream_id):
    youtube.liveBroadcasts().bind(part="id,contentDetails", id=broadcast_id, streamId=stream_id).execute()


def transition_broadcast_to_live(youtube, broadcast_id):
    youtube.liveBroadcasts().transition(
        broadcastStatus="live", id=broadcast_id, part="id,snippet,contentDetails,status"
    ).execute()


def check_stream_health(youtube, stream_id):
    stream_status = youtube.liveStreams().list(part="id,cdn,status", id=stream_id).execute()

    if "items" in stream_status and len(stream_status["items"]) > 0:
        stream = stream_status["items"][0]
        health_status = stream["status"]["healthStatus"]
        print("Stream Health Status:", health_status["status"])
        if "lastUpdateTime" in health_status:
            print("Last Update Time:", health_status["lastUpdateTime"])
        if "configurationIssues" in health_status:
            print("Configuration Issues:", health_status["configurationIssues"])
    else:
        print("No stream found with the given ID")


def list_scheduled_broadcasts(youtube):
    request = youtube.liveBroadcasts().list(
        part="id,snippet,contentDetails,status", broadcastStatus="upcoming", broadcastType="all"
    )
    response = request.execute()

    broadcasts = response.get("items", [])

    if not broadcasts:
        print("No scheduled broadcasts found.")
        return

    broadcast_dict = {}

    for broadcast in broadcasts:
        broadcast_id = broadcast.get("id")
        broadcast_title = broadcast.get("title")
        scheduled_start_time = broadcast["snippet"].get("scheduledStartTime", "Not available")
        status = broadcast["status"].get("lifeCycleStatus", "Unknown")
        description = broadcast["snippet"].get("description", "No description")

        broadcast_dict[broadcast_id] = {
            "title": broadcast_title,
            "scheduled_start_time": scheduled_start_time,
            "status": status,
            "description": description,
        }

    return broadcast_dict


# Check the health of the stream
def check_stream_health(youtube, stream_id):
    stream_status = youtube.liveStreams().list(part="id,cdn,status", id=stream_id).execute()

    if "items" in stream_status and len(stream_status["items"]) > 0:
        stream = stream_status["items"][0]
        health_status = stream["status"]["healthStatus"]
        print("Stream Health Status:", health_status["status"])
        if "lastUpdateTime" in health_status:
            print("Last Update Time:", health_status["lastUpdateTime"])
        if "configurationIssues" in health_status:
            print("Configuration Issues:", health_status["configurationIssues"])
    else:
        print("No stream found with the given ID")
