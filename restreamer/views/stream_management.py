import hashlib
import logging
import os

from django.conf import settings
from django.db.utils import IntegrityError
from rest_framework import status
from rest_framework.generics import GenericAPIView
from rest_framework.response import Response
from rest_framework.views import APIView
from ..models import ChunkRecord, StreamingEvent
from ..serializers import ChunkSerializer, PositionLastSerializer, ChunkRecordSerializer
from django.utils.decorators import method_decorator
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from restreamer.video_data import VideoDataManager

log = logging.getLogger(__name__)


class ChunkUploadView(APIView):
    def post(self, request):
        serializer = ChunkSerializer(data=request.data)


        if serializer.is_valid():
            try:
                chunk_id = serializer.validated_data.get("chunk_id")
                chunk_data_ = request.FILES.get("chunk_data")

                if not chunk_data_:
                    return Response({"error": "chunk_data is missing"}, status=status.HTTP_400_BAD_REQUEST)

                chunk_data = chunk_data_.read()
                chunk_identifier = serializer.validated_data.get("chunk_identifier")

                try:
                    streaming_event = StreamingEvent.objects.get(identifier=chunk_identifier)
                except StreamingEvent.DoesNotExist:
                    log.info("Streaming Event not valid !!")
                    return Response({"message": "Streaming Event not valid"}, status=status.HTTP_400_BAD_REQUEST)


                chunk_record = ChunkRecord()
                chunk_record.data_size = len(chunk_data)
                chunk_record.streaming_event = streaming_event
                md5_hash = hashlib.md5()
                chunk_record.local_id = chunk_id
                md5_hash.update(chunk_data)
                chunk_record.md5 = md5_hash.hexdigest()
                chunk_record.identifier = chunk_identifier
                chunk_record.save()

                while True:
                    try:
                        if chunk_data:
                            self.save_chunk_s3(chunk_record, chunk_data, chunk_identifier)
                        else:
                            log.info("Only Main streaming evnet is set")
                    except OSError as e:
                        log.exception(e)
                        log.warning("Please fix it immediately!!!")
                        # chunk_record_1.backup_path = "backup_chunks"
                        # chunk_record_2.backup_path = "backup_chunks"
                        continue
                    break

            except IntegrityError as e:
                log.info("IntegrityError occurred while saving chunk_record_1: %s" % str(e))
                pass

            except Exception as e:
                log.exception("Error", e)
                return Response(
                    {"message": "Error occurred while processing the chunk."},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR,
                )
            return Response(
                {"message": "Chunk successfully received and saved."},
                status=status.HTTP_200_OK,
            )
        else:
            log.error(serializer.errors)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    def upload_to_s3(self, chunk_data, filename):
        try:
            bucket_name = os.environ.get('AWS_STORAGE_BUCKET_NAME')
            client = settings.S3_CLIENT

            client.put_object(Body=chunk_data,
                              Bucket= bucket_name,
                              Key=filename,)
        except Exception as e:
            log.exception(e)

    def save_chunk_s3(self, chunk_record, chunk_data, indentifier):
        try:
            # Upload the chunk to Object Storage using the utility function
            self.upload_to_s3(chunk_data, f"{chunk_record.local_id}_{indentifier}.bin")

        except Exception as e:
            log.info("Error", e)
        log.info("Chunk successfully stored to bucket")


class PositionLastUploadView(APIView):
    def post(self, request):
        serializer = PositionLastSerializer(data=request.data)
        if serializer.is_valid():
            try:
                position_last = serializer.validated_data.get("position_last")
                streaming_event = StreamingEvent.objects.get(id=1)
                endpoints = streaming_event.end_points.all()
                for endpoint in endpoints:
                    endpoint.position_last = position_last
                    endpoint.save()
            except Exception as e:
                log.info(f"Error {e}")
            return Response({"success": "Position last updated successfully"})

        else:
            log.error(serializer.errors)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class DeleteChunksView(APIView):
    def post(self, request):
        if request.data.get("signal") == "delete_all_chunks_signal":
            all_chunks = ChunkRecord.objects.all()
            all_chunks.delete()

            return Response({"status": "success"}, status=status.HTTP_200_OK)
        else:
            return Response(
                {"status": "error", "message": "Invalid signal"},
                status=status.HTTP_400_BAD_REQUEST,
            )


class ChunkExistsView(APIView):
    def post(self, request):
        md5_from_client = request.data.get('md5')

        if not md5_from_client:
            return Response("Missing 'md5' in request data", status=status.HTTP_400_BAD_REQUEST)

        try:
            chunk_exists = ChunkRecord.objects.filter(md5=md5_from_client).exists()
            if chunk_exists:
                return Response({'chunk_exists': True}, status=status.HTTP_200_OK)
            else:
                log.info('Chunk Record dosnt exsist')
                return Response({'chunk_exists': False}, status=status.HTTP_200_OK)

        except ChunkRecord.DoesNotExist:
            return Response({'chunk_exists': False }, status=status.HTTP_200_OK)



def check_buffer_status(request, streaming_event_id):
    streaming_event = StreamingEvent.objects.get(id=streaming_event_id)
    video_manager = VideoDataManager(streaming_event=streaming_event.id)
    buffer_time = streaming_event.buffer
    buffer_filled = video_manager.is_buffer_filled(buffer_time)
    
    return JsonResponse({'buffer_filled': buffer_filled})