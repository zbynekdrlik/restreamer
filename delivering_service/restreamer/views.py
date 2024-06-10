from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from restreamer.serializers import StreamInfoSerializer, EndpointsListSerializer
from restreamer.endpoints import EndPoint
import logging
import queue
import json

log = logging.getLogger(__name__)

data_queue = queue.Queue()

class ReceiveStreamDataView(APIView):
    def post(self, request, *args, **kwargs):
        try:
            chunk_id = request.GET.get('chunk_id')
            chunk_identifer = request.GET.get('stream_id')
            log.info(f"chunk_id------>{chunk_id}")
            log.info(f"request------>{request.GET}")
            log.info(f"chunk_identifer------>{chunk_identifer}")
            data_queue.put(chunk_id)
            log.info(f"data_queue --- > {data_queue}")
            try:
                while not data_queue.empty():
                    queued_data = data_queue.get()
                    log.info(f"Processing queued data: {queued_data}")
            except Exception as e:
                log.exception("Error processing data from queue: ", e)
            return Response({'status':'success'}, status=status.HTTP_200_OK)
        
        except Exception as e:
            log.exception(e)
            return Response({'status':"error", 'messege': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
        
class ReceiveInitDataView(APIView):
    def post(self, request, *args, **kwargs):
        log.info("ReceiveInitDataView POST method called")
        
        try:
            data = json.loads(request.body)
            endpoints = data.get('endpoints', [])
            if not isinstance(endpoints, list):
                raise ValueError("Invalid format for 'endpoints'")
            
            for endpoint in endpoints:
                alias = endpoint.get('alias')
                service_type = endpoint.get('service_type')
                stream_key = endpoint.get('stream_key')
                
                if not alias or not service_type or not stream_key:
                    raise ValueError("Missing required endpoint fields")
                
                log.info(f'alias ------> {alias}')
                log.info(f'service_type ------> {service_type}')
                log.info(f'stream_key ------> {stream_key}')
            
            return Response({"message": "Data received successfully"}, status=status.HTTP_200_OK)
        
        except json.JSONDecodeError:
            log.error("Invalid JSON format")
            return Response({"error": "Invalid JSON format"}, status=status.HTTP_400_BAD_REQUEST)
        except ValueError as e:
            log.error(f"Error processing data: {e}")
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            log.error(f"Unexpected error: {e}")
            return Response({"error": "Unexpected error occurred"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
