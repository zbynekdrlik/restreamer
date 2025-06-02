import logging
import os
import zipfile

from accounts.models import RestreamerUser
from django.conf import settings
from django.contrib import messages
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.contrib.auth.mixins import LoginRequiredMixin
from django.http import FileResponse
from django.shortcuts import (get_object_or_404, redirect,
                              render)
from django.utils.decorators import method_decorator
from django.views import View
from django.views.generic import TemplateView
from restreamer.tasks import init_stream, end_stream, init_fast_stream
from restreamer.scheduler import schedule_init_stream

from ..models import EndPointCfg, StreamingEvent, ChunkRecord
from .instances import InstanceManager as IM
from restreamer.video_data import VideoDataManager

from accounts.models import RestreamerUser

from restreamer.scheduler import delete_instance_schedule, schedule_init_stream, cancel_delete_instance_jobs
from restreamer.tasks import end_stream, init_stream, init_fast_stream
from restreamer.video_data import VideoDataManager

from ..models import ChunkRecord, EndPointCfg, StreamingEvent
from .instances import InstanceManager as IM

from restreamer.utils import delete_s3_chunks
from django.http import JsonResponse



log = logging.getLogger(__name__)


class DownloadPageView(LoginRequiredMixin, TemplateView,):
    template_name = 'restreamer/download.html'


@method_decorator(login_required, name='dispatch')
class DownloadRestreamer(View):
    
   def get(self, request, *args, **kwargs):
        original_zip_path = os.path.join(settings.STATIC_ROOT, 'files', 'restreamer.zip')
        temp_dir = os.path.join(settings.MEDIA_ROOT, 'temp')
        os.makedirs(temp_dir, exist_ok=True)

        modified_zip_path = os.path.join(temp_dir, 'restreamer_with_user_id.zip')

        with zipfile.ZipFile(modified_zip_path, 'w') as zf:
            with zipfile.ZipFile(original_zip_path, 'r') as original_zip:
                for file in original_zip.filelist:
                    file_contents = original_zip.read(file.filename)
                    zf.writestr(file.filename, file_contents)
            user_key = request.user.api_key
            text = f"user_api_key: {user_key}"
            zf.writestr('config.txt', text.encode('utf-8'))

        response = FileResponse(open(modified_zip_path, 'rb'), content_type='application/zip')
        response['Content-Disposition'] = 'attachment; filename=restreamer.zip'
        return response


@method_decorator(login_required, name='dispatch')
class SetupStream(View):
    def post(self, request, *args, **kwargs):

        streaming_event = StreamingEvent.objects.get(id=self.kwargs['id'])
        if streaming_event.receiving_activated:
            streaming_event.receiving_activated=False
            streaming_event.save()

            return redirect('control:home')

        if not streaming_event.receiving_activated:
            streaming_event.receiving_activated=True
            
            try:
                IM(user_id=request.user.id).create_instance()
            except Exception as e:
                messages.error(request, f'There was a problem creating instance {e}')
           
            init_fast_stream.delay(streaming_event.id)
            streaming_event.save()
            messages.success(request, 'Streaming server successfuly scheduled for creation')
            return redirect('control:home')


@method_decorator(login_required, name='dispatch')
class StartEndStream(View):
   
    def post(self, request, *args, **kwargs):
        
        data = request.POST
        
        streaming_event = StreamingEvent.objects.get(id=self.kwargs['id'])
        video_manager = VideoDataManager(streaming_event.id)
        buffer_time = streaming_event.buffer
        user_id = request.user.id
        
        
        if streaming_event.delivering_activated:
            streaming_event.delivering_activated=False
            streaming_event.save()
            
            end_stream(user_id, streaming_event)
            delete_instance_schedule(user_id, streaming_event.identifier)
            return redirect('control:home')
        
        if not streaming_event.delivering_activated:
            
            cancel_delete_instance_jobs(user_id, streaming_event.identifier)
            
            if video_manager.is_buffer_filled(buffer_time) or data.get('confirm_start') == '1':
                streaming_event.delivering_activated=True
                streaming_event.save()
                user_id = self.request.user.id
                  
                    
                try:
                    init_stream.delay(user_id, streaming_event.id)
                except Exception as e:
                    messages.error(request, f"There was a problem initialize streams {e}")
               
            messages.success(request, 'Streams initialized successfuly')
            return redirect('control:home')



@method_decorator(login_required, name='dispatch')
class DeleteChunkData(View):
    def post(self, request):
        streaming_event_id = request.POST.get("streaming_event_id")
        bucket_name = os.environ.get('AWS_STORAGE_BUCKET_NAME')
        s3 = settings.S3_CLIENT
        
        try:
            
            # Debug: Print starting deletion info
            log.debug(f"Starting deletion process for streaming event: {streaming_event_id}")
            log.debug(f"Bucket: {bucket_name}")
            
            # List all objects with the streaming event prefix
            objects_to_delete = []
            paginator = s3.get_paginator('list_objects_v2')
            
            # Debug: Count objects before deletion
            object_count = 0
            for page in paginator.paginate(Bucket=bucket_name, Prefix=f"{streaming_event_id}/"):
                if 'Contents' in page:
                    object_count += len(page['Contents'])
                    objects_to_delete.extend([{'Key': obj['Key']} for obj in page['Contents']])
            
            log.debug(f"Found {object_count} objects to delete in folder {streaming_event_id}/")
            
            # Delete all found objects in batches
            if objects_to_delete:
                deleted_count = 0
                for i in range(0, len(objects_to_delete), 1000):
                    batch = objects_to_delete[i:i+1000]
                    response = s3.delete_objects(
                        Bucket=bucket_name,
                        Delete={'Objects': batch}
                    )
                    deleted_count += len(batch)
                    
                    # Debug: Log deletion response
                    log.debug(f"Deleted batch {i//1000 + 1}: {len(batch)} objects")
                    if 'Deleted' in response:
                        log.debug(f"Successfully deleted objects: {[obj['Key'] for obj in response['Deleted']]}")
                    if 'Errors' in response:
                        log.error(f"Errors during deletion: {response['Errors']}")
                
                log.debug(f"Total objects deleted: {deleted_count}")
                
                # Verify deletion by listing objects again
                remaining_objects = []
                for page in paginator.paginate(Bucket=bucket_name, Prefix=f"{streaming_event_id}/"):
                    if 'Contents' in page:
                        remaining_objects.extend(page['Contents'])
                
                if remaining_objects:
                    log.error(f"Deletion verification failed! {len(remaining_objects)} objects remain")
                    for obj in remaining_objects:
                        log.error(f"Object still exists: {obj['Key']}")
                else:
                    log.debug("Deletion verification successful - no objects remain in the folder")
            
            else:
                log.debug(f"No objects found to delete in folder {streaming_event_id}/")
            
            # Delete chunks from the database
            db_deleted_count = ChunkRecord.objects.filter(identifier=streaming_event_id).delete()
            log.debug(f"Deleted {db_deleted_count[0]} database records")
            
        except Exception as e:
            log.exception(f"Error deleting data for streaming event {streaming_event_id}")
            messages.error(request, f"Error deleting data: {e}")
            return redirect('control:home')
        
        messages.success(request, f'All chunks for streaming event {streaming_event_id} deleted successfully!')
        log.info(f"Successfully completed deletion for streaming event {streaming_event_id}")
        return redirect('control:home')

        

@method_decorator(login_required, name='dispatch')
class RemoveStreamingEvent(View):
    def post(self, request, *args, **kwargs):
        streaming_event = StreamingEvent.objects.get(id=self.kwargs['id'])
        streaming_event.delete()
        messages.success(request, 'Streaming event deleted successfuly!')
        return redirect('control:home')


@method_decorator(login_required, name='dispatch')
class RemoveEndpoint(View):
    def post(self, request, *args, **kwargs):
        streaming_event = get_object_or_404(StreamingEvent, id=self.kwargs['streaming_event_id'])
        success = streaming_event.remove_endpoint(endpoint_id=self.kwargs['endpoint_id'])
        user_id = request.user.id
        
        alias = EndPointCfg.objects.get(id=self.kwargs['endpoint_id']).alias
        if streaming_event.delivering_activated:
            try:
                end_stream(user_id, streaming_event, alias=alias)
            except Exception as e:
                messages.error(request, f'Error ending stream for {alias}')
                
        if success:
            messages.success(request, f'Endpoint Removed stream for {alias} finished!')
            return redirect('control:streaming_event_detail', id=streaming_event.id)
        else:
            messages.error(request, f'Removing {alias} failed!')
            return redirect('control:streaming_event_detail', id=streaming_event.id)


@method_decorator(login_required, name='dispatch')
class AddEndpoint(View):
    def post(self, request, *args, **kwargs):
        endpoint_ids = request.POST.getlist("endpoint", [])
        streaming_event = get_object_or_404(StreamingEvent, id=self.kwargs['streaming_event_id'])
        user_id = request.user.id
        
        video_manager = VideoDataManager(streaming_event.id)
        time_point = request.POST.get("time_point", None)
        
        if time_point:
        
            hours, minutes, seconds = map(int, time_point.split(':'))
            total_minutes = hours * 60 + minutes + seconds / 60
            
        else:
            total_minutes = None
            
        chunk_id = video_manager.time_to_chunk(total_minutes)
         
        if streaming_event.delivering_activated:
            try:
                init_stream.delay(user_id, streaming_event.id, chunk_id=chunk_id)
            except Exception as e:
               messages.error(request, f'Error initialized stream!')
        
            messages.success(request, f'Endpoint added stream initialized!')
        
        if endpoint_ids:
            for endpoint in endpoint_ids:
                streaming_event.add_endpoint(endpoint_id=endpoint, position_last=chunk_id)
          
        return redirect('control:streaming_event_detail',id=streaming_event.id)
    
    
@method_decorator(login_required, name='dispatch')       
class StreamSchedulerView(View):
    
    def get(self, request):
        template_name = 'restreamer/scheduler.html'
        user = request.user
        endpoionts = EndPointCfg.objects.filter(user=user).all()
        streaming_events = StreamingEvent.objects.filter(user=user)
        
        context = {
            'streaming_events': streaming_events,
            'endpoints': endpoionts,  
        }
        
        return render(request, template_name, context)
    
    def post(self, request):
        
        data = request.POST
        
        if data.get("start_time") and \
           data.get("chunk_time") and \
           len(data.get("end_points", 0)) >= 1 and \
           data.get('streaming_event'):
           streaming_event = StreamingEvent.objects.get(id=data['streaming_event'])
           video_manager = VideoDataManager(streaming_event.id)
            
           start_time = data['start_time']
           chunk_time = data['chunk_time']
           endpoint =  data.get('end_points')
           
           endpoint = EndPointCfg.objects.get(id=endpoint)
           e_id = endpoint.id
           if data.get('repeat'):
                repeat = data['repeat'] == "on"
           chunk_id = video_manager.stream_time_to_chunk(chunk_time)
           schedule_init_stream(request.user.id, streaming_event.id, start_time, chunk_id, e_id, repeat)
           
           return redirect('control:stream-scheduler')


@method_decorator(login_required, name='dispatch') 
def user_history(request, user_id):
    user = RestreamerUser.objects.get(id=user_id)
    streaming_events = StreamingEvent.objects.filter(user=user)
    endpoints = EndPointCfg.objects.filter(user=user)
    
    streaming_event_history = []
    for event in streaming_events:
        for record in event.history.all():
            prev_record = record.prev_record
            changes = {}
            if prev_record:
                diff = record.diff_against(prev_record)
                changes = {field: (getattr(prev_record, field), getattr(record, field)) for field in diff.changed_fields}
            streaming_event_history.append({
                'record': record,
                'changes': changes
            })

    endpoints_history = []
    for endpoint in endpoints:
        for record in endpoint.history.all():
            prev_record = record.prev_record
            changes = {}
            if prev_record:
                diff = record.diff_against(prev_record)
                changes = {field: (getattr(prev_record, field), getattr(record, field)) for field in diff.changed_fields}
            endpoints_history.append({
                'record': record,
                'changes': changes
            })

    users_history = []
    for record in user.history.all():
        prev_record = record.prev_record
        changes = {}
        if prev_record:
            diff = record.diff_against(prev_record)
            changes = {field: (getattr(prev_record, field), getattr(record, field)) for field in diff.changed_fields}
        users_history.append({
            'record': record,
            'changes': changes
        })
    
    context = {
        'user': user,
        'streaming_event_history': streaming_event_history,
        'endpoints_history': endpoints_history,
        'users_history': users_history,
    }
    return render(request, 'restreamer/user_history.html', context)




