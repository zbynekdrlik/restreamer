from __future__ import absolute_import, unicode_literals
import os
from celery import Celery
from datetime import  timedelta

from django.conf import settings

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'nl_restreamer.settings')

app = Celery("nl_restreamer")

app.config_from_object('django.conf:settings', namespace='CELERY')

# Celery Settings   
broker_connection_retry_on_startup = True
# Debugging: Print the broker URL to ensure it's using Redis
print(f"Celery broker URL: {settings.CELERY_BROKER_URL}")

app.conf.beat_schedule = {
    "stream_ready": {
        "task": "services.tasks.check_stream_status",
        "schedule": timedelta(seconds=5)
    }
}

app.autodiscover_tasks()

# "buffer_health": {
#             "task": "services.tasks.get_buffer_duration",
#             "schedule": timedelta(seconds=7)  # Adjust the interval as needed
#         },

