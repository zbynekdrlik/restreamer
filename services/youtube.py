import os
import logging
import time

# For user-based OAuth
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

logger = logging.getLogger(__name__)


SCOPES = ['https://www.googleapis.com/auth/youtube']


def load_credentials(token_file: str, credentials_json_path: str):
    creds = None

    # 1. If we have token.json, try to load it
    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)

    # 2. If no creds or invalid creds, attempt refresh or re-run flow
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            # Attempt to refresh
            try:
                creds.refresh(Request())
            except Exception as e:
                # If refresh fails (token revoked, etc.), re-run the OAuth flow
                print("Refresh token invalid or revoked. Need to re-authenticate.")
                creds = run_local_oauth_flow(credentials_json_path, token_file)
        else:
            # No creds at all, or can't refresh -> run the OAuth flow
            print("No valid credentials found. Need to authenticate.")
            creds = run_local_oauth_flow(credentials_json_path, token_file)

    return creds


def run_local_oauth_flow(credentials_json_path, token_json_path):
    flow = InstalledAppFlow.from_client_secrets_file(
        credentials_json_path,
        SCOPES
    )
    creds = flow.run_local_server(port=8080)
    with open(token_json_path, 'w') as token:
        token.write(creds.to_json())
    print(f"Token saved to {token_json_path}")
    return creds


def get_control_stream_url_if_live(
    token_file: str,
    credentials_json_path: str
) -> str:
    """
    Checks YouTube for a broadcast named "Control Stream" that is actively live.
    Returns the watch URL if found and live, otherwise None.
    """

    # 1. Load or refresh credentials from your utility
    creds = load_credentials(token_file, credentials_json_path)
    if not creds:
        # If you failed to authenticate, just return None or raise an exception
        return None

    # 2. Build YouTube client
    youtube = build('youtube', 'v3', credentials=creds)

    # 3. Query liveBroadcasts
    response = youtube.liveBroadcasts().list(
        part='id,snippet,contentDetails,status',
        broadcastStatus='active',
        broadcastType='all'
    ).execute()

    # 4. Find "Control Stream" that's truly "live"
    for item in response.get('items', []):
        title = item['snippet']['title']
        if title == 'Control Stream':
            life_cycle_status = item['status']['lifeCycleStatus']
            logger.info(f"Found broadcast '{title}' with lifeCycleStatus={life_cycle_status}")
            if life_cycle_status == 'live':
                broadcast_id = item['id']
                return f"https://www.youtube.com/watch?v={broadcast_id}"

    return None