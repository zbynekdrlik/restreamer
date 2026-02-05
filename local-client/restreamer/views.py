from rest_framework import generics

from .models import StreamingEvent
from .serializers import StreamingEventSerializer


class StreamingEventCreateView(generics.CreateAPIView):
    model = StreamingEvent
    serializer_class = StreamingEventSerializer
