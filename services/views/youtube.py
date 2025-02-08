from django.shortcuts import redirect
from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request
from django.utils import timezone
import datetime
from services.models import YouTubeOAuthCredentials
from django.conf import settings


def youtube_auth_start(request):
    flow = Flow.from_client_secrets_file(
        settings.GOOGLE_CLIENT_SECRETS_FILE,  # This is your client_id/client_secret JSON from Google Cloud
        scopes=['https://www.googleapis.com/auth/youtube'],
        redirect_uri=request.build_absolute_uri('/youtube/auth/callback')
    )
    # Generate the authorization URL
    authorization_url, state = flow.authorization_url(
        access_type='offline',
        prompt='consent',
        include_granted_scopes='true'
    )
    # Store the state in session for security
    request.session['oauth_state'] = state
    return redirect(authorization_url)

def youtube_auth_callback(request):
    state = request.session.pop('oauth_state', '')
    flow = Flow.from_client_secrets_file(
        settings.GOOGLE_CLIENT_SECRETS_FILE,
        scopes=['https://www.googleapis.com/auth/youtube'],
        redirect_uri=request.build_absolute_uri('/youtube/auth/callback')
    )
    flow.fetch_token(authorization_response=request.build_absolute_uri(), state=state)
    creds = flow.credentials  # This is a google.oauth2.credentials.Credentials object

    # Save creds to DB
    user = request.user  # The currently logged-in user
    # Create or update user’s YT credentials
    youtube_oauth, created = YouTubeOAuthCredentials.objects.get_or_create(user=user)
    youtube_oauth.access_token = creds.token
    youtube_oauth.refresh_token = creds.refresh_token
    youtube_oauth.token_uri = creds.token_uri
    youtube_oauth.client_id = creds.client_id
    youtube_oauth.client_secret = creds.client_secret
    youtube_oauth.scopes = ' '.join(creds.scopes) if creds.scopes else ''
    
    # Convert creds.expiry (Python datetime) to something storable
    if creds.expiry:
        youtube_oauth.expiry = creds.expiry.replace(tzinfo=timezone.utc) if not creds.expiry.tzinfo else creds.expiry
    youtube_oauth.save()

    # Redirect somewhere
    return redirect('some_success_page')