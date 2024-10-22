from .models import StreamingEvent
from rest_framework import generics
from .serializers import StreamingEventSerializer


class StreamingEventCreateView(generics.CreateAPIView):
    model = StreamingEvent
    serializer_class = StreamingEventSerializer



