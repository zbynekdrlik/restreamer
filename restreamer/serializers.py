from rest_framework import serializers
from .models import ChunkRecord, StreamingEvent


class ChunkSerializer(serializers.Serializer):
    chunk_data = serializers.FileField()
    chunk_id = serializers.IntegerField()
    chunk_identifier = serializers.CharField()


class PositionLastSerializer(serializers.Serializer):
    position_last = serializers.IntegerField()


class ChunkRecordSerializer(serializers.ModelSerializer):
    class Meta:
        model = ChunkRecord
        fields = ['md5']


class StreamingEventSerializer(serializers.ModelSerializer):

    class Meta:
        model = StreamingEvent
        fields = ['identifier', 'short_description']

class BufferHealthSerializer(serializers.Serializer):
    streaming_event_id = serializers.CharField(max_length=255)
    buffer_duration = serializers.CharField()