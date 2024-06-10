from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from restreamer.serializers import StreamInfoSerializer
from restreamer.endpoints import EndPoint
import logging
import queue

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
        serializer = StreamInfoSerializer(data=request.data)
        if serializer.is_valid():
            service_type = serializer.validated_data['service_type']
            endpoint_key = serializer.validated_data['endpoint_key']
            
            log.info(f'service type ------>{service_type}')
            log.info(f'endpoint_key ------>{endpoint_key}')
            
            return Response({"message": "Data received successfully"}, status=status.HTTP_200_OK)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    
    
