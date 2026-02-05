import logging
import socket
import uuid
from uuid import uuid4

from accounts.models import RestreamerUser
from django.db import models
from django.utils import timezone
from simple_history.models import HistoricalRecords

log = logging.getLogger(__name__)


def chunk_directory_path(instance, filename):
    # file will be uploaded to MEDIA_ROOT/user_<id>/<filename>
    if hasattr(instance, "backup_path"):
        chunk_media_path = f"{instance.backup_path}/stream_event_{instance.streaming_event.id}/{filename}"
    else:
        chunk_media_path = f"stream_chunks/stream_event_{instance.streaming_event.id}/{filename}"
    return chunk_media_path


class EndPointCfg(models.Model):
    user = models.ForeignKey(
        RestreamerUser, on_delete=models.CASCADE, blank=True, null=True, related_name="users_endpoint"
    )
    alias = models.CharField(max_length=50)
    SERVICE_TYPE_CHOICES = (
        ("YT_HLS", "YouTube HLS"),
        ("FB", "Facebook"),
        ("YT_RTMP", "YouTube RTMP"),
        ("VIMEO", "Vimeo RTMP"),
        ("INSTAGRAM", "Instagram RTMP"),
    )
    service_type = models.CharField(max_length=20, choices=SERVICE_TYPE_CHOICES, default="YT")
    stream_key = models.CharField(max_length=250, default="", blank=True)
    enabled = models.BooleanField(default=False)
    position_last = models.IntegerField(default=0, verbose_name="Last id of processed chunk record")
    delivered_bytes = models.PositiveBigIntegerField(default=0, verbose_name="Delivered bytes")
    is_fast = models.BooleanField(default=False)
    history = HistoricalRecords()

    class Meta:
        ordering = ["alias"]

    def __str__(self):
        return self.alias


class StreamingEvent(models.Model):
    BUFFER_CHOICES = [
        (1, "1 minute"),
        (2, "2 minutes"),
        (5, "5 minutes"),
        (10, "10 minutes"),
    ]

    user = models.ForeignKey(RestreamerUser, on_delete=models.CASCADE, related_name="users_stream")
    identifier = models.CharField(max_length=255, unique=True, default=uuid4)
    short_description = models.CharField(max_length=20)
    date_of_event = models.DateTimeField()
    received_bytes = models.PositiveBigIntegerField(default=0, verbose_name="Received bytes")
    receiving_activated = models.BooleanField(default=False)
    delivering_activated = models.BooleanField(default=False)
    end_points = models.ManyToManyField(EndPointCfg)
    buffer = models.IntegerField(choices=BUFFER_CHOICES, default=1)
    history = HistoricalRecords()

    def __str__(self):
        return self.short_description

    def stream_info(self):
        endpoints = self.end_points.all()

        stream_data = [
            {"stream_key": endpoint.stream_key, "service_type": endpoint.service_type, "name": endpoint.alias}
            for endpoint in endpoints
        ]

        return stream_data

    def remove_endpoint(self, endpoint_id):
        try:
            endpont = self.end_points.get(id=endpoint_id)
            self.end_points.remove(endpont)
            return True
        except Exception as e:
            log.exception(f"An error occurred: {e}")

    def add_endpoint(self, endpoint_id, position_last=None):
        try:
            # Retrieve the endpoint object from the database
            endpoint = EndPointCfg.objects.get(id=endpoint_id)
            if position_last:
                endpoint.position_last = position_last
                endpoint.save()
            self.end_points.add(endpoint)
            return True
        except EndPointCfg.DoesNotExist:
            log.info(f"Endpoint with id {endpoint_id} does not exist.")
            return False
        except Exception as e:
            log.info(f"An error occurred: {e}")
            return False

    # Get ip address of instance where django server is running.
    @classmethod
    def get_server_ip(cls):
        # Retrieve the current server's IP address
        return socket.gethostbyname(socket.gethostname())


class ChunkRecord(models.Model):
    streaming_event = models.ForeignKey(StreamingEvent, on_delete=models.CASCADE, related_name="chunks")
    chunk_file = models.FileField(upload_to=chunk_directory_path)
    # data = models.BinaryField(default=b'', verbose_name='Chunk of data from input stream')
    data_size = models.IntegerField()
    created_at = models.DateTimeField(default=timezone.now)
    md5 = models.CharField(max_length=50, default="", blank=True)
    local_id = models.IntegerField(default=0, db_index=True)
    in_process = models.BooleanField(default=True)
    send = models.BooleanField(default=False)
    identifier = models.CharField(max_length=50, db_index=True)
    uuid_identifier = models.UUIDField(default=uuid.uuid4, editable=False)
    history = HistoricalRecords()

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["local_id", "identifier", "streaming_event"], name="unique stream")
        ]
        indexes = [
            models.Index(fields=["streaming_event", "created_at", "md5"]),
        ]
