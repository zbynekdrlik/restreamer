from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from restreamer.endpoints import EndPoint, endpoints
import logging
import queue

log = logging.getLogger(__name__)

data_queue = queue.Queue()

class ReceiveDataView(APIView):
    def post(self, request, *args, **kwargs):
        try:
            data = request.body
            log.info(f"Data------------------>{data}")
            data_queue.put(data)
            return Response({'status':'success'}, status=status.HTTP_200_OK)
        
        except Exception as e:
            log.exception(e)
            return Response({'status':"error", 'messege': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
