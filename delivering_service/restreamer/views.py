from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from restreamer.serializers import StreamInfoSerializer, EndpointsListSerializer
from restreamer.endpoints import EndPoint
import logging
import queue
from ast import literal_eval

from restreamer.endpoints import endpoints_info

from .shared import data_queue

log = logging.getLogger(__name__)


class ReceiveStreamDataView(APIView):
    def post(self, request, *args, **kwargs):
        try:
            chunk_id_raw = request.GET.get('chunk_id')
            stream_identifier = request.GET.get('stream_id')

            if chunk_id_raw:
                chunk_id_dict = literal_eval(chunk_id_raw)
                chunk_id = chunk_id_dict.get('chunk_id')
            else:
                chunk_id = None

            if chunk_id and stream_identifier:
                data_queue.put((chunk_id, stream_identifier))
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
            print("validated data ----------->", serializer.validated_data)
            endpoints = serializer.validated_data['endpoints']
            chunk_id = serializer.validated_data['chunk_id']
            stream_id = serializer.validated_data['steram_id']
            
            endpoint_list = []
            
            for endpoint in endpoints:
                alias = endpoint['alias']
                service_type = endpoint['service_type']
                stream_key = endpoint['stream_key']
            
                log.info(f'alias ------> {alias}')
                log.info(f'service_type ------> {service_type}')
                log.info(f'stream_key ------> {stream_key}')
                
                try:
                    endpoint_process = EndPoint(alias, service_type, stream_key, stream_id, chunk_id)
                    endpoint_process.start()
                    endpoint_list.append(endpoint_process)
                except Exception as e:
                    print(f'An error occurred: {e}')
            try:
                endpoints_info(endpoint_list)
            except KeyboardInterrupt:
                log.info('Ctrl-C detected, terminating!')
        
            return Response({"message": "Data received successfully endpoint started"}, status=status.HTTP_200_OK)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
