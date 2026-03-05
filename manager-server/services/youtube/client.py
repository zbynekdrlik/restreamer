import logging

from django.utils import timezone
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

log = logging.getLogger(__name__)


class YouTubeAuthError(Exception):
    """Raised when YouTube OAuth credentials cannot be refreshed."""

    pass


def build_youtube_client(user):
    """
    Build the youtube client from the user's stored OAuth tokens in DB.
    Returns the youtube API client or None if the user has no OAuth connected.
    Raises YouTubeAuthError if credentials exist but cannot be refreshed.
    """
    youtube_oauth = getattr(user, "youtube_oauth", None)
    if not youtube_oauth:
        return None

    # Strip timezone info for google-auth compatibility (uses naive UTC internally)
    expiry = youtube_oauth.expiry
    if expiry and expiry.tzinfo:
        expiry = expiry.replace(tzinfo=None)

    creds = Credentials(
        token=youtube_oauth.access_token,
        refresh_token=youtube_oauth.refresh_token,
        token_uri=youtube_oauth.token_uri,
        client_id=youtube_oauth.client_id,
        client_secret=youtube_oauth.client_secret,
        scopes=youtube_oauth.scopes.split(),
        expiry=expiry,
    )

    # Refresh if needed
    if creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
        except Exception as e:
            log.error("Failed to refresh user YouTube creds: %s", e)
            raise YouTubeAuthError(
                f"YouTube OAuth refresh failed: {e}. "
                "The refresh token may have expired (7-day limit in Testing mode). "
                "Re-authorize YouTube in Django admin or publish the Google Cloud "
                "project to Production to get permanent refresh tokens."
            ) from e

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
