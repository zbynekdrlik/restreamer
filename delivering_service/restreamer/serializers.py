from rest_framework import serializers

class StreamDataSerializer(serializers.Serializer):
    chunk_data = serializers.FileField()
    chunk_id = serializers.IntegerField()
    
    
class StreamInfoSerializer(serializers.Serializer):
    alias = serializers.CharField(max_length=50)
    service_type = serializers.CharField(max_length=50)
    stream_key = serializers.CharField(max_length=450)
    

class EndpointsListSerializer(serializers.Serializer):
    endpoints = StreamInfoSerializer(many=True)
    chunk_id = serializers.IntegerField()
    stream_id = serializers.CharField()
    