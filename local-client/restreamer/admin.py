import logging

from admin_object_actions.admin import ModelAdminObjectActionsMixin
from django.contrib import admin
from django.db.models import Sum
from django.utils import timezone
from django.utils.html import format_html
from .models import (ChunkRecord, StreamingEvent, ClientProfile)

log = logging.getLogger(__name__)


class StreamingEventAdmin(ModelAdminObjectActionsMixin, admin.ModelAdmin):

    list_display = (
        'id',
        'short_description',
        'date_of_event',
        'received_speed',
        'receiving_activated',
        'delivering_activated',
        'display_object_actions_list',
        'buffer_duration',
    )
    #readonly_fields = ('total_duration',)
    

    object_actions = [
        {
            'slug': 'switch receiving',
            'verbose_name': 'Start receiving',
            'verbose_name_past': 'switched',
            'form_method9': 'GET',
            'function': 'switch_receiving',
            'permission': 'change',
        },
        {
            'slug': 'switch_delivering',
            'verbose_name': 'Start delivering',
            'verbose_name_past': 'switched',
            'form_method': 'GET',
            'function': 'switch_delivering',
            'permission': 'change',
        },
    ]

    def __init__(self, model, admin_site):
        super().__init__(model, admin_site)
        log.info('init')
        return


#       events = StreamingEvent.objects.all()
#
#        for event in events:
#            if event.receiving_activated:
#                self.object_actions[0]['verbose_name'] = 'Stop receiving'
#
# else:
#      self.object_actions[0]['verbose_name'] = 'Start receiving'
#
#   if event.delivering_activated:
#        self.object_actions[1]['verbose_name'] = 'Stop delivering'
#
#  else:
#        self.object_actions[1]['verbose_name'] = 'Start delivering'

    def received_speed(self, instance):
        received_mb_str = f'{instance.received_bytes / 1024 / 1024:.2f}MB'
        try:
            # Calculate the datetime for 10 seconds ago
            ten_seconds_ago = timezone.now() - timezone.timedelta(seconds=10)

            # Retrieve the records from the last 10 seconds
            last_chunks = ChunkRecord.objects.filter(
                streaming_event=instance,
                created_at__gte=ten_seconds_ago).order_by('id')
            speed_str = 'O.00Kb/s'
            if last_chunks:
                chunks_size = last_chunks.aggregate(
                    Sum('data_size'))['data_size__sum']
                time_delta = (last_chunks.last().created_at -
                              last_chunks.first().created_at)
                speed_kbs = chunks_size / time_delta.total_seconds()
                speed_str = f'{speed_kbs * 8 / 1024:.2f}Kb/s'
        except Exception as e:
            log.exception(e)

        formatted_str = format_html(f'{received_mb_str}<br>{speed_str}')
        return formatted_str

    received_speed.short_description = 'Received/Speed'

    def has_change_permission(self, request, obj=True):
        # Vráť True, ak používateľ má povolenie meniť objekty, inak False

        return True

    def switch_delivering(self, instance, form):
        log.info('Switch delivering button pressed')
        if instance.delivering_activated:
            instance.delivering_activated = False  # Prepínanie hodnoty active
            instance.save()

            self.object_actions[1]['verbose_name'] = 'Start delivering'
            log.info(instance.delivering_activated)

        else:
            instance.delivering_activated = True
            instance.save()

            self.object_actions[1]['verbose_name'] = 'Stop delivering'
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
        log.info('You pressed it correctly')
        log.info(self.object_actions)

        if instance.receiving_activated:
            instance.receiving_activated = False
            instance.save()

            self.object_actions[0]['verbose_name'] = 'Start receiving'
        else:
            instance.receiving_activated = True
            instance.save()

            self.object_actions[0]['verbose_name'] = 'Stop receiving'

    def buffer_duration(self, instance):
        all_chunks = ChunkRecord.objects.filter(streaming_event=instance, send=False)
        if all_chunks.exists():
            time_delta = (all_chunks.last().created_at -
                          all_chunks.first().created_at)
            formated_time = str(time_delta)
        else:
            return "00:00s"
        return formated_time

    buffer_duration.short_description = 'Buffer duration'
    
    
class ChunkRecordAdmin(admin.ModelAdmin):
    list_display = ('id', 'created_at', 'data_size', 'streaming_event', 'md5',
                    "in_process", "send")


class ClientProfileAdmin(admin.ModelAdmin):
    list_display = ['user_id',]


admin.site.register(ClientProfile, ClientProfileAdmin)
admin.site.register(StreamingEvent, StreamingEventAdmin)
admin.site.register(ChunkRecord, ChunkRecordAdmin)

