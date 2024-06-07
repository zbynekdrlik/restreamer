from django.urls import path
from .views import ReceiveDataView
from restreamer.endpoints import send_chunk, start_endpoint

urlpatterns = [
    path('api/receive_data/', ReceiveDataView.as_view(), name='receive_data'),
    path('api/send_chunk/', send_chunk , name='send_chunk'),
    path('api/start_endpoint/', start_endpoint , name='start_endpoint'),
]
