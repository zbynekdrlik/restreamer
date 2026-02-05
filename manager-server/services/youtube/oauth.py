# services/youtube/oauth.py
import logging

from django.conf import settings
from google_auth_oauthlib.flow import Flow

logger = logging.getLogger(__name__)


SCOPES = ["https://www.googleapis.com/auth/youtube"]


def build_flow(state=None):
    """
    Create a Google OAuth Flow object for the YouTube scope.
    """
    redirect_uri = settings.YOUTUBE_REDIRECT_URI  # e.g. https://yourdomain.com/youtube/callback/
    flow = Flow.from_client_secrets_file(
        settings.GOOGLE_CLIENT_SECRETS_FILE, scopes=SCOPES, state=state, redirect_uri=redirect_uri
    )
    return flow


# You could add more helpers for run_console(), etc. if needed.
