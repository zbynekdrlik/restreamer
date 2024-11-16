import logging
from datetime import datetime, timedelta

from apscheduler.schedulers.background import BackgroundScheduler
from pytz import timezone

from restreamer.tasks import init_stream
from restreamer.views.instances import InstanceManager

log = logging.getLogger(__name__)

scheduler_timezone = timezone('Europe/Bratislava')
scheduler = BackgroundScheduler()
scheduler.start()


def schedule_init_stream(user_id, streaming_event_id, start_time, chunk_id, endpoint_id, repeat, interval='weeks'):
    # Convert start_time to datetime format if it is a string
    if isinstance(start_time, str):
        start_time = datetime.strptime(start_time, '%Y-%m-%dT%H:%M')
    start_time = scheduler_timezone.localize(start_time)
    days_of_week = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']
    day_of_week = days_of_week[start_time.weekday()]

    if repeat:
        if interval == 'weeks':
            job = scheduler.add_job(init_stream, 'cron', day_of_week=day_of_week, hour=start_time.hour, minute=start_time.minute, 
                              args=[user_id, streaming_event_id], kwargs={"chunk_id": chunk_id, 'endpoint_id': endpoint_id})
            next_run_time = job.next_run_time
            log.info(f"----------- Stream scheduled to repeat every week on {day_of_week} at {start_time.hour}:{start_time.minute}. Next run at {next_run_time} --------------")
    scheduler.add_job(init_stream, 'date', run_date=start_time, args=[user_id, streaming_event_id], kwargs={"chunk_id": chunk_id, 'endpoint_id': endpoint_id})
    
    jobs = scheduler.get_jobs()
    for job in jobs:
        log.info(f"Job id: {job.id}, Next run time: {job.next_run_time}")

def delete_instance_schedule(user_id):
    run_time = datetime.now() + timedelta(minutes=30)
    scheduler.add_job(delete_instance, run_date=run_time, args=[user_id])
    

def delete_instance(user_id):
    instance_manager = InstanceManager(user_id)
    instance_manager.delete_instance()