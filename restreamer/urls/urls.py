from django.urls import path
from restreamer.views.stream_management import ChunkUploadView, PositionLastUploadView, DeleteChunksView, ChunkExistsView
from restreamer.views.communication import GetActiveStream, GetBufferHealth, DeliveringReady



urlpatterns = [
    path("chunk-upload/", ChunkUploadView.as_view(), name="receive-local-chunks"),
    path("api/update_position_last/", PositionLastUploadView.as_view(), name="update_position_last"),
    path("api/delete_all_chunks/", DeleteChunksView.as_view(), name="delete_all_chunks"),
    path('api/check-chunk/', ChunkExistsView.as_view(), name='check_chunk'),
    path('api/get_active_stream/', GetActiveStream.as_view(), name='get_active_stream'),
    path('api/get_buffer_health/', GetBufferHealth.as_view(), name='get_buffer_health'),
    path('check-status/<int:streaming_event_id>/', DeliveringReady.as_view(), name='stream_ready'),
  
]
