import logging
import os

from admin_object_actions.admin import ModelAdminObjectActionsMixin
from django.conf import settings
from django.contrib import admin
from django.db.models import Sum
from django.utils import timezone
from django.utils.html import format_html
from restreamer.models import ChunkRecord, EndPointCfg, StreamingEvent

log = logging.getLogger(__name__)


class StreamingEventAdmin(ModelAdminObjectActionsMixin, admin.ModelAdmin):
    list_display = (
        "id",
        "short_description",
        "date_of_event",
        "received_speed",
        "receiving_activated",
        "delivering_activated",
        "display_object_actions_list",
    )

    filter_horizontal = ("end_points",)
    ordering = ["id"]

    object_actions = [
        {
            "slug": "switch receiving",
            "verbose_name": "Start receiving",
            "verbose_name_past": "switched",
            "form_method9": "GET",
            "function": "switch_receiving",
            "permission": "change",
        },
        {
            "slug": "switch_delivering",
            "verbose_name": "Start delivering",
            "verbose_name_past": "switched",
            "form_method": "GET",
            "function": "switch_delivering",
            "permission": "change",
        },
    ]

    """     def __init__(self, model, admin_site):
        super().__init__(model, admin_site)
        log.info('init')

        events = StreamingEvent.objects.all()

        for event in events:
            if event.receiving_activated:
                self.object_actions[0]['verbose_name'] = 'Stop receiving'

            else:
                self.object_actions[0]['verbose_name'] = 'Start receiving'

            if event.delivering_activated:
                self.object_actions[1]['verbose_name'] = 'Stop delivering'

            else:
                self.object_actions[1]['verbose_name'] = 'Start delivering' """

    def received_speed(self, instance):
        received_mb_str = f"{instance.received_bytes / 1024 / 1024:.2f}MB"
        try:
            # Calculate the datetime for 10 seconds ago
            ten_seconds_ago = timezone.now() - timezone.timedelta(seconds=10)

            # Retrieve the records from the last 10 seconds
            last_chunks = ChunkRecord.objects.filter(
                streaming_event=instance, created_at__gte=ten_seconds_ago
            ).order_by("local_id")
            speed_str = "O.00Kb/s"
            if last_chunks:
                chunks_size = last_chunks.aggregate(Sum("data_size"))["data_size__sum"]
                time_delta = last_chunks.last().created_at - last_chunks.first().created_at
                speed_kbs = chunks_size / time_delta.total_seconds()
                speed_str = f"{speed_kbs * 8 / 1024:.2f}Kb/s"
        except Exception as e:
            log.exception(e)

        formatted_str = format_html(f"{received_mb_str}<br>{speed_str}")
        return formatted_str

    received_speed.short_description = "Received/Speed"

    def has_change_permission(self, request, obj=True):
        # Vráť True, ak používateľ má povolenie meniť objekty, inak False

        return True

    def switch_delivering(self, instance, form):
        log.info("Switch delivering button pressed")
        if instance.delivering_activated:
            instance.delivering_activated = False  # Prepínanie hodnoty active
            instance.save()

            self.object_actions[1]["verbose_name"] = "Start delivering"
            log.info(instance.delivering_activated)

        else:
            instance.delivering_activated = True
            instance.save()

            self.object_actions[1]["verbose_name"] = "Stop delivering"
            # instance.delivering_activated = True
            # instance.save()
            # pridajte ďalšiu logiku na spustenie doručovania, napríklad volanie príslušnej funkcie alebo metódy
            log.info(instance.delivering_activated)

        # from django.db import connection
        # connection.close()
        # try:
        #     restr_manager.manage_endpoints()
        # except Exception as e:
        #     log.exception(e)
        # from django.db import connection
        # connection.close()

    def switch_receiving(self, instance, form):
        # Vykonajte požadovanú akciu
        log.info("You pressed it correctly")
        log.info(self.object_actions)

        if instance.receiving_activated:
            instance.receiving_activated = False  # Ak je pôvodná hodnota True, nastavíme na False
            instance.save()

            self.object_actions[0]["verbose_name"] = "Start receiving"
        else:
            instance.receiving_activated = True  # Ak je pôvodná hodnota False, nastavíme na True
            instance.save()

            self.object_actions[0]["verbose_name"] = "Stop receiving"


@admin.register(EndPointCfg)
class EndPointCfgAdmin(admin.ModelAdmin):
    list_display = (
        "alias",
        "service_type",
        "stream_key",
        "enabled",
        "delivered_mb",
    )

    def delivered_mb(self, instance):
        return f"{instance.delivered_bytes / 1024 / 1024:.2f}MB"

    delivered_mb.short_description = "Delivered MB"


"""
    def position_vs_last(self, instance):
        log.debug(instance)
        last_chunk = ChunkRecord.objects.last()

        if last_chunk:
            last_chunk_created_at_str = f'{last_chunk.created_at:%H:%M:%S %e.%b}'
        else:
            last_chunk_created_at_str = 'NA'

        try:
            position_chunk = ChunkRecord.objects.get(local_id=instance.position_last)
        except ChunkRecord.DoesNotExist:
            position_chunk = None

        if position_chunk:
            position_created_at_str = f"{position_chunk.created_at:%H:%M:%S %e.%b}"
        else:
            position_created_at_str = 'NA'

        formatted_str = format_html(
            f'{instance.position_last} ({position_created_at_str})<br>'
            f'{last_chunk.local_id if last_chunk else "NA"} ({last_chunk_created_at_str})'
        )
        log.debug(formatted_str)
        return formatted_str

    position_vs_last.short_description = 'Position / Last' """


class ChunkRecordAdmin(admin.ModelAdmin):
    list_display = ("local_id", "created_at", "data_size", "streaming_event", "md5", "identifier", "uuid_identifier")
    ordering = ["local_id"]

    # this is called when chunks are deleted in admin
    def delete_queryset(self, request, queryset):
        s3_client = settings.S3_CLIENT
        linode_bucket_name = os.environ.get("AWS_STORAGE_BUCKET_NAME")

        try:
            s3_keys = [f"{chunk_record.local_id}_{chunk_record.uuid_identifier}.bin" for chunk_record in queryset]
            super().delete_queryset(request, queryset)

            # deleting in smaller groups ---> s3 request limit

            while s3_keys:
                objects_to_delete = [{"Key": key} for key in s3_keys[:990]]
                s3_client.delete_objects(Bucket=linode_bucket_name, Delete={"Objects": objects_to_delete})
                del s3_keys[:990]

            log.info("Chunks deleted successfully")
        except Exception as e:
            log.debug("There was an error deleting")
            log.exception(e)


admin.site.register(StreamingEvent, StreamingEventAdmin)
admin.site.register(ChunkRecord, ChunkRecordAdmin)
