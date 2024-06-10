from rest_framework import serializers


class StreamInfoSerializer(serializers.Serializer):
    service_type = serializers.CharField(max_length=50)
    endpoint_key = serializers.CharField(max_length=450)
    

class StreamDataSerializer(serializers.Serializer):
    chunk_data = serializers.FileField()
    chunk_id = serializers.IntegerField()
    
    
class EndpointsListSerializer(serializers.Serializer):
    endpoints = StreamInfoSerializer(many=True)