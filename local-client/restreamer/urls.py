from django.urls import path

from .views import StreamingEventCreateView

urlpatterns = [path("create-streaming_event/", StreamingEventCreateView.as_view(), name="create_streaming_event")]
