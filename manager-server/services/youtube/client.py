import logging

from django.utils import timezone
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

log = logging.getLogger(__name__)


def build_youtube_client(user):
    """
    Build the youtube client from the user's stored OAuth tokens in DB.
    Returns the youtube API client or None if the user is not connected.
    """
    youtube_oauth = getattr(user, "youtube_oauth", None)
    if not youtube_oauth:
        return None

    creds = Credentials(
        token=youtube_oauth.access_token,
        refresh_token=youtube_oauth.refresh_token,
        token_uri=youtube_oauth.token_uri,
        client_id=youtube_oauth.client_id,
        client_secret=youtube_oauth.client_secret,
        scopes=youtube_oauth.scopes.split(),
    )

    # Refresh if needed
    if creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
        except Exception as e:
            log.error("Failed to refresh user YouTube creds: %s", e)
            return None

        # Save updated tokens
        youtube_oauth.access_token = creds.token
        if creds.expiry:
            youtube_oauth.expiry = creds.expiry if creds.expiry.tzinfo else creds.expiry.replace(tzinfo=timezone.utc)
        youtube_oauth.save()

    return build("youtube", "v3", credentials=creds)


def get_active_live_broadcasts(user):
    youtube = build_youtube_client(user)
    if not youtube:
        return []
    response = (
        youtube.liveBroadcasts()
        .list(part="id,snippet,contentDetails,status", broadcastStatus="active", broadcastType="all")
        .execute()
    )
    return response.get("items", [])
