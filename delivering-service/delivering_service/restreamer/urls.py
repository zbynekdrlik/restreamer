from django.urls import path

from .views import EndpointProcessStatusView, EndStreamView, ReceiveInitDataView, ReceiveStreamDataView

urlpatterns = [
    path("api/receive_data/", ReceiveStreamDataView.as_view(), name="receive_data"),
    path("api/raceive_init_data/", ReceiveInitDataView.as_view(), name="raceive_init_data"),
    path("api/endpoint-status/", EndpointProcessStatusView.as_view(), name="endpoint_status"),
    path("api/end_stream/", EndStreamView.as_view(), name="end_stream"),
]
