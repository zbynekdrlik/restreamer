"""E2E Testing API URLs."""

from django.urls import path
from restreamer.views.e2e_api import (
    E2EActivateDelivering,
    E2EActivateReceiving,
    E2EChunkVerification,
    E2EDeactivate,
    E2EDeliveringStatus,
    E2EYouTubeStreamStatus,
)

app_name = "e2e"

urlpatterns = [
    path("activate-receiving/", E2EActivateReceiving.as_view(), name="activate_receiving"),
    path("activate-delivering/", E2EActivateDelivering.as_view(), name="activate_delivering"),
    path("delivering-status/", E2EDeliveringStatus.as_view(), name="delivering_status"),
    path("chunk-verification/", E2EChunkVerification.as_view(), name="chunk_verification"),
    path("youtube-status/", E2EYouTubeStreamStatus.as_view(), name="youtube_status"),
    path("deactivate/", E2EDeactivate.as_view(), name="deactivate"),
]
