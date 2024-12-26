import logging

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.shortcuts import (get_list_or_404, get_object_or_404, redirect,
                              render)
from django.utils.decorators import method_decorator
from django.views import View
from rest_framework import status
from rest_framework.response import Response
from rest_framework.exceptions import NotFound
from rest_framework.views import APIView

from accounts.models import RestreamerUser
from restreamer.models import StreamingEvent, ChunkRecord
from restreamer.serializers import (BufferHealthSerializer,
                                    StreamingEventSerializer)
from restreamer.video_data import VideoDataManager
from restreamer.views.instances import InstanceManager
from services.utils import get_client_ip

log = logging.getLogger(__name__)


class GetActiveStream(APIView):
    def get(self, request):
        user_id = request.GET.get('user_uuid')
        ip_address = get_client_ip(request)
        
        log.info(f'user ip addres is {ip_address}')                                                                                                                       

        if not user_id:
            return Response({'error': 'user id parameter is required'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            user = RestreamerUser.objects.get(api_key=user_id)
            streaming_event = StreamingEvent.objects.filter(user=user).first()
            if streaming_event.receiving_activated:
                serializer = StreamingEventSerializer(streaming_event)
                return Response(serializer.data, status=status.HTTP_200_OK)
            elif not streaming_event.receiving_activated:
                return Response({'warning': 'Streaming Event is not activated'}, status=status.HTTP_403_FORBIDDEN)

            if not streaming_event.exist():
                return Response({"warning": "No streaming event found"}, status=status.HTTP_404_NOT_FOUND)

        except RestreamerUser.DoesNotExist:
            return Response({"error": "User not found"}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


""" class GetBufferHealth(APIView):
    def post(self, request):
        serializer = BufferHealthSerializer(data=request.data)
        if serializer.is_valid():
            streaming_event_id = serializer.validated_data['streaming_event_id']
            buffer_duration = serializer.validated_data['buffer_duration']

            channel_layer = get_channel_layer()
            async_to_sync(channel_layer.group_send)(
                "buffer_health",
                {
                    "type": "buffer_health_update",
                    "message": {
                        "streaming_event_id": streaming_event_id,
                        "buffer_duration": buffer_duration,
                    },
                },
            )

            return Response({'status': 'success', 'message': 'Buffer health data received'}, status=status.HTTP_200_OK)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST) """
    
    
@method_decorator(login_required, name='dispatch')
class DeliveringReady(View):

    def get(self, request, streaming_event_id):
        # Check server status
        manager = InstanceManager(request.user.id)
        status = manager.check_status()

        # Check buffer status
        streaming_event = get_object_or_404(StreamingEvent, id=streaming_event_id)
        live = streaming_event.delivering_activated
        video_manager = VideoDataManager(streaming_event=streaming_event.id)
        buffer_time = streaming_event.buffer
        buffer_filled = video_manager.is_buffer_filled(buffer_time)

        return JsonResponse({
            'status': status,
            'buffer_filled': buffer_filled,
            'live': live
        })
        
class GetNextChunkView(APIView):
    """
    API View to retrieve the next available chunk ID greater than the given one.
    """
    
    def get(self, request, *args, **kwargs):
        current_local_id = request.query_params.get("current_local_id")
        stream_identifier = request.query_params.get("stream_identifier")

        if not current_local_id or not stream_identifier:
            return JsonResponse(
                {"error": "Missing required parameters: current_chunk_id or stream_identifier"},
                status=400,
            )

        try:
            current_local_id = int(current_local_id)
        except ValueError:
            return JsonResponse({"error": "Invalid chunk ID format"}, status=400)

        # Fetch the next chunk greater than the current chunk ID
        next_chunk = (
            ChunkRecord.objects.filter(stream_identifier=stream_identifier, local_id__gt=current_local_id)
            .order_by("local_id")
            .first()
        )

        if not next_chunk:
            raise NotFound(
                detail="No chunk found greater than the current chunk ID for the given stream identifier."
            )

        return Response({"next_chunk_id": next_chunk.local_id}, status=200)