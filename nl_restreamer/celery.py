from __future__ import absolute_import, unicode_literals
import os
from celery import Celery


#celery -A nl_restreamer worker -l INFO --pool=threads -Q init_stream_queue
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'nl_restreamer.settings')


app = Celery('nl_restreamer')

app.config_from_object('django.conf:settings', namespace='CELERY')

app.autodiscover_tasks()

app.conf.task_routes = {
    'restreamer.tasks.init_stream': {'queue': 'init_stream_queue'},
    'restreamer.tasks.start_delivering': {'queue': 'streaming_queue'},
    'restreamer.tasks.is_buffer_ready_action': {'queue': 'services'},
}
@app.task(bind=True)
def debug_task(self):
    print(f'Request: {self.request!r}')