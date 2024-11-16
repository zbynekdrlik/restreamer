from django.conf import settings
from django.conf.urls.static import static
from django.urls import path
from restreamer.views.restreamer import StreamingEventView
from restreamer.views.user_control import (CreateStreamView, DeleteChunkData,
                                           DownloadPageView,
                                           DownloadRestreamer, SetupStream,
                                           StartEndStream,
                                           StreamingEventDetailView, RemoveStreamingEvent,
                                           RemoveEndpoint, AddEndpoint, StreamSchedulerView, user_history)
from restreamer.views.youtube import GoLiveYt, YtLivePage
from restreamer.views.stream_management import IsDeliveringActive

app_name = 'control'

urlpatterns = [
    path('home/', StreamingEventView.as_view(), name='home'),
    path('create-stream/', CreateStreamView.as_view(), name='create_stream'),
    path('streaming_event-detail/<int:id>/', StreamingEventDetailView.as_view(), name='streaming_event_detail'),
    path('restreamer-download', DownloadRestreamer.as_view(), name='download_zip'),
    path('downloads/', DownloadPageView.as_view(), name='downloads'),
    path('setup_stream/<int:id>/', SetupStream.as_view(), name='setup_stream'),
    path('start_stream/<int:id>/', StartEndStream.as_view(), name='start_stream'),
    path('go_live/', YtLivePage.as_view(), name='go_live'),
    path('delete_data/', DeleteChunkData.as_view(), name='delete_chunk_data'),
    path('remove-streaming-event/<int:id>/', RemoveStreamingEvent.as_view(), name='remove_streaming_event'),
    path('remove-endpoint/<int:streaming_event_id>/<int:endpoint_id>/', RemoveEndpoint.as_view(), name='remove_endpoint'),
    path('add-endpoint/<int:streaming_event_id>/', AddEndpoint.as_view(), name='add_endpoint'),
    path('stream-scheduler/', StreamSchedulerView.as_view(), name='stream-scheduler'),
    path('user/<int:user_id>/history/', user_history, name='user_history'),

    path('<int:streaming_event_id>/streaming_event_active/', IsDeliveringActive.as_view(), name='streaming_event_active'),
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
