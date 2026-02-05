import logging
import os

from django.db import models
from django.db.models.signals import pre_delete
from django.dispatch import receiver

log = logging.getLogger(__name__)


# hello server
def chunk_directory_path(instance, filename):
    # file will be uploaded to MEDIA_ROOT/user_<id>/<filename>
    if hasattr(instance, "backup_path"):
        chunk_media_path = f"{instance.backup_path}/streaming_event_{instance.streaming_event.id}/{filename}"
    else:
        chunk_media_path = f"stream_chunks/streaming_event_{instance.streaming_event.id}/{filename}"
    return chunk_media_path


class StreamingEvent(models.Model):
    identifier = models.CharField(max_length=255, unique=True, default="", null=True, blank=True)
    short_description = models.CharField(max_length=20, null=True, blank=True)
    date_of_event = models.DateTimeField(auto_now_add=True)
    server_ip = models.CharField(max_length=250, default="", blank=True, null=True)
    received_bytes = models.PositiveBigIntegerField(default=0, verbose_name="Received bytes")
    receiving_activated = models.BooleanField(default=False)
    delivering_activated = models.BooleanField(default=False)

    def __str__(self):
        return self.short_description

    @classmethod
    def create(self, data):
        pass


class ChunkRecord(models.Model):
    streaming_event = models.ForeignKey(StreamingEvent, on_delete=models.CASCADE, related_name="chunks")
    chunk_file = models.FileField(upload_to=chunk_directory_path)
    # data = models.BinaryField(default=b'', verbose_name='Chunk of data from input stream')
    data_size = models.IntegerField()
    created_at = models.DateTimeField(auto_now_add=True)
    md5 = models.CharField(max_length=50, default="", blank=True)
    in_process = models.BooleanField(default=False)
    send = models.BooleanField(default=False)

    class Meta:
        indexes = [
            models.Index(fields=["streaming_event", "created_at", "md5"]),
        ]

    def buffer_duration(self):
        all_chunks = ChunkRecord.objects.filter(streaming_event=self.streaming_event, send=False)
        if all_chunks.exists():
            time_delta = all_chunks.last().created_at - all_chunks.first().created_at
            formated_time = str(time_delta)
        else:
            return "00:00s"
        return formated_time


class ClientProfile(models.Model):
    user_id = models.CharField(max_length=255, unique=True, default="", null=True, blank=True)


@receiver(pre_delete, sender=ChunkRecord)
def delete_file(sender, instance, **kwargs):
    # delete the file associated with the instance
    if instance.chunk_file:
        try:
            os.remove(instance.chunk_file.path)
        except FileNotFoundError:
            log.warning(f"File {instance.chunk_file.path} missing. Never-mind. Skipping.")
