import logging
import threading
from ast import literal_eval

from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from restreamer.endpoints import endpoing_manger
from restreamer.serializers import EndpointsListSerializer

from .shared import data_queue

log = logging.getLogger(__name__)


def start_central_manager():
    if not hasattr(start_central_manager, "started"):
        start_central_manager.started = True
        central_manager_thread = threading.Thread(target=endpoing_manger.monitor_endpoints, daemon=True)
        central_manager_thread.start()
        logging_thread = threading.Thread(target=endpoing_manger.log_endpoints_info, daemon=True)
        logging_thread.start()
        log.info("Central Manager and Logging threads started.")


class ReceiveStreamDataView(APIView):
    def post(self, request, *args, **kwargs):
        try:
            chunk_id_raw = request.GET.get("chunk_id")
            stream_identifier = request.GET.get("stream_id")

            if chunk_id_raw:
                chunk_id_dict = literal_eval(chunk_id_raw)
                chunk_id = chunk_id_dict.get("chunk_id")
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
                return Response({"status": "success"}, status=status.HTTP_200_OK)
            else:
                log.error("Missing chunk_id or chunk_identifier")
                return Response(
                    {"status": "error", "message": "Missing chunk_id or stream_id"}, status=status.HTTP_400_BAD_REQUEST
                )

        except Exception as e:
            log.exception("An error occurred", exc_info=e)
            return Response({"status": "error", "message": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class ReceiveInitDataView(APIView):
    def get(self, request, *args, **kwargs):
        # Logic to handle GET requests (used for readiness check)
        return Response(
            {"status": "ready", "message": "Django is ready to serve responses."}, status=status.HTTP_200_OK
        )

    def post(self, request, *args, **kwargs):
        serializer = EndpointsListSerializer(data=request.data)
        if serializer.is_valid():
            endpoints = serializer.validated_data["endpoints"]
            chunk_id = serializer.validated_data["chunk_id"]
            stream_id = serializer.validated_data["stream_id"]

            start_central_manager()

            for endpoint in endpoints:
                alias = endpoint["alias"]
                service_type = endpoint["service_type"]
                stream_key = endpoint["stream_key"]

                signal = {
                    "alias": alias,
                    "action": "start",
                    "service_type": service_type,
                    "stream_key": stream_key,
                    "stream_id": stream_id,
                    "chunk_id": chunk_id,
                }

                endpoing_manger.add_signal(signal)

            return Response({"message": "Data received successfully endpoint started"}, status=status.HTTP_200_OK)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class EndpointProcessStatusView(APIView):
    """Returns status of running ffmpeg endpoint processes."""

    def get(self, request, *args, **kwargs):
        endpoints = []
        for alias, process in endpoing_manger.endpoint_processes.items():
            endpoints.append(
                {
                    "alias": alias,
                    "alive": process.is_alive(),
                    "pid": process.pid,
                    "buff_size_mb": round(process.buff_size.value / 1024 / 1024, 2),
                    "current_chunk_id": process.chunk_id.value,
                }
            )

        return Response(
            {
                "status": "ok",
                "endpoint_count": len(endpoints),
                "endpoints": endpoints,
            },
            status=status.HTTP_200_OK,
        )


class EndStreamView(APIView):
    def post(self, request, *args, **kwargs):
        alias = request.data.get("alias")
        action = "stop_all" if alias is None else "stop"

        signal = {"alias": alias if alias else "all", "action": action}
        endpoing_manger.add_signal(signal)

        message = (
            "Signal sent to stop all endpoints" if action == "stop_all" else f"Signal sent to stop endpoint {alias}"
        )
        return Response({"message": message}, status=status.HTTP_200_OK)
