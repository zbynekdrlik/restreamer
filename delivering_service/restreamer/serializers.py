from rest_framework import serializers


class StreamInfoSerializer(serializers.Serializer):
    streaming_event = serializers.CharField(max_length=250)
    streaming_event_id = serializers.CharField(max_length=250)
    endpoints = serializers.CharField(max_length=250)
    streaming_key = serializers.CharField(max_length=250, write_only=True)

class StreamDataSerializer(serializers.Serializer):
    chunk_data = serializers.FileField()
    chunk_id = serializers.IntegerField()
    
    
    