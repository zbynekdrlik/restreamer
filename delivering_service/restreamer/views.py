from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from restreamer.serializers import StreamInfoSerializer, EndpointsListSerializer
from restreamer.endpoints import EndPoint
import logging
import queue

log = logging.getLogger(__name__)

data_queue = queue.Queue()

class ReceiveStreamDataView(APIView):
    def post(self, request, *args, **kwargs):
        try:
            chunk_id = request.GET.get('chunk_id')
            stream_identifier = request.GET.get('stream_id')
            log.info(f"chunk_id ------> {chunk_id}")
            log.info(f"request.GET ------> {request.GET}")
            log.info(f"chunk_identifier ------> {stream_identifier}")
           
            if chunk_id and stream_identifier:
                data_queue.put((chunk_id, stream_identifier))
                log.info(f"data_queue --- > {data_queue}")
                try:
                    while not data_queue.empty():
                        queued_data = data_queue.get()
                        log.info(f"Processing queued data: {queued_data}")
                except Exception as e:
                    log.exception("Error processing data from queue: ", exc_info=e)
                return Response({'status': 'success'}, status=status.HTTP_200_OK)
            else:
                log.error("Missing chunk_id or chunk_identifier")
                return Response({'status': 'error', 'message': 'Missing chunk_id or stream_id'}, status=status.HTTP_400_BAD_REQUEST)
        
        except Exception as e:
            log.exception("An error occurred", exc_info=e)
            return Response({'status': 'error', 'message': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
        
class ReceiveInitDataView(APIView):
    def post(self, request, *args, **kwargs):
        serializer = EndpointsListSerializer(data=request.data)
        if serializer.is_valid():
            endpoints = serializer.validated_data['endpoints']
            
            for endpoint in endpoints:
                alias = endpoint['alias']
                service_type = endpoint['service_type']
                stream_key = endpoint['stream_key']
                log.info(f'alias ------> {alias}')
                log.info(f'service_type ------> {service_type}')
                log.info(f'stream_key ------> {stream_key}')
                
                try:
                    endpoint_process = EndPoint(alias, service_type, stream_key)
                    endpoint_process.start()
                except Exception as e:
                    print(f'An error occurred: {e}')
            
            return Response({"message": "Data received successfully endpoint started"}, status=status.HTTP_200_OK)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
