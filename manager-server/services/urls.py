from django.urls import path
from services.views.youtube import youtube_auth_start

app_name = "services"

urlpatterns = [
    path("youtube/auth/start", youtube_auth_start, name="youtube_auth_start"),
]
