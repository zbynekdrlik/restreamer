from rest_framework import serializers

from .models import StreamingEvent


class StreamingEventSerializer(serializers.ModelSerializer):
    class Meta:
        model = StreamingEvent
        fields = ["short_description", "identifier", "server_ip"]
