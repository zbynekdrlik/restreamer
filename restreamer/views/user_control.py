import logging
import os
import zipfile

from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.contrib.auth.mixins import LoginRequiredMixin
from django.http import FileResponse
from django.shortcuts import redirect, render , get_list_or_404, get_object_or_404
from django.utils.decorators import method_decorator
from django.views import View
from django.views.generic import TemplateView
from restreamer.data_sending import ChunkSender
from restreamer.tasks import init_stream, end_stream
from restreamer.scheduler import schedule_init_stream

from ..forms import EndPointForm, StreamingEventForm
from ..models import EndPointCfg, StreamingEvent, ChunkRecord
from .delivering import DeliveringManger
from .instances import InstanceManager as IM
from restreamer.video_data import VideoDataManager

from accounts.models import RestreamerUser

from django.http import JsonResponse


log = logging.getLogger(__name__)


class DownloadPageView(LoginRequiredMixin, TemplateView,):
    template_name = 'restreamer/download.html'


# Receiving data from user when create new streaming event
class CreateStreamView(LoginRequiredMixin,TemplateView):
    template_name = 'restreamer/setup_stream.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['form'] = StreamingEventForm(user=self.request.user)
        context['endpoint_form'] = EndPointForm()
        return context

    def post(self, request, *args, **kwargs):
        try:
            streaming_event_form = StreamingEventForm(request.POST)
            endpoint_form = EndPointForm(request.POST)

            if streaming_event_form.is_valid():
                try:
                    streaming_event = streaming_event_form.save(commit=False)
                    streaming_event.user = request.user
                    streaming_event.save()

                    endpoints = request.POST.getlist('end_points', [])
                    endpoints = EndPointCfg.objects.filter(pk__in=endpoints)
                    streaming_event.end_points.add(*endpoints)

                    return redirect('home')

                except Exception as e:
                    log.exception(f'Error saving form {e}')

            elif endpoint_form.is_valid():
                endpoint = endpoint_form.save(commit=False)
                endpoint.user = request.user
                endpoint.save()
                return redirect('create_stream')

            else:
                log.error(f'Invalid form {streaming_event_form.errors}')

        except Exception as e:
            log.exception(f'Error: {e}')
            streaming_event_form = None
            endpoint_form = None

        context = self.get_context_data(streaming_event_form=streaming_event_form, endpoint_form=endpoint_form)
        return self.render_to_response(context)


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
class StreamingEventDetailView(View):
    def get(self, request, *args, **kwargs):
        template_name = 'restreamer/streaming_event.html'
        
        streaming_event = StreamingEvent.objects.get(id=self.kwargs['id'])
        
        selected_endpoints = streaming_event.end_points.all()
        endpoint_form = EndPointForm()
        all_endpoints = EndPointCfg.objects.filter(user=request.user)
        video_manager = VideoDataManager(streaming_event=streaming_event.id)
        
        video_length = video_manager.get_stream_length()
        
        buffer_display = streaming_event.get_buffer_display()
        
        context = {
            'endpoints': selected_endpoints,
            'streaming_event':streaming_event,
            'endpoint_form': endpoint_form,
            'available_endpoints': all_endpoints,
            'video_length': video_length,
            'buffer': buffer_display,
            }
        
        return render(request, template_name, context)

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
            IM(user_id=request.user.id).create_instance()
            streaming_event.save()
            return redirect('control:home')


@method_decorator(login_required, name='dispatch')
class StartEndStream(View):
   
    def post(self, request, *args, **kwargs):
        
        data = request.POST
        
        streaming_event = StreamingEvent.objects.get(id=self.kwargs['id'])
        video_manager = VideoDataManager(streaming_event=streaming_event.id)
        buffer_time = streaming_event.buffer
        user_id = request.user.id
        
        
        if streaming_event.delivering_activated:
            streaming_event.delivering_activated=False
            streaming_event.save()
            
            end_stream(user_id, streaming_event)
            #delete_instance_schedule(user_id)
            return redirect('control:home')
        
        if not streaming_event.delivering_activated:
            
            if video_manager.is_buffer_filled(buffer_time) or data.get('confirm_start') == '1':
                streaming_event.delivering_activated=True
                streaming_event.save()
                user_id = self.request.user.id
                    
                try:
                    init_stream.delay(user_id, streaming_event.id)
                except Exception as e:
                    print(f'An error occurred: {e}')
               
            #start_delivering.delay(streaming_event.id, user_id)
            return redirect('control:home')



@method_decorator(login_required, name='dispatch')
class DeleteChunkData(View):
    def post(self, request):
        streaming_event_id = request.POST.get("streaming_event_id")
        print("steraming evnet id ----------------------->", streaming_event_id)
        try:
            ChunkRecord.objects.filter(identifier=streaming_event_id).all().delete()
        except Exception as e:
            print(f'An error occurred: {e}')
        
        return redirect('control:home')
        

@method_decorator(login_required, name='dispatch')
class RemoveStreamingEvent(View):
    def post(self, request, *args, **kwargs):
        streaming_event = StreamingEvent.objects.get(id=self.kwargs['id'])
        streaming_event.delete()
        log.info("Streaming Event deleted successfuly!!")
        return redirect('control:home')


@method_decorator(login_required, name='dispatch')
class RemoveEndpoint(View):
    def post(self, request, *args, **kwargs):
        streaming_event = get_object_or_404(StreamingEvent, id=self.kwargs['streaming_event_id'])
        success = streaming_event.remove_endpoint(endpoint_id=self.kwargs['endpoint_id'])
        user_id = request.user.id
        
        alias = EndPointCfg.objects.get(id=self.kwargs['endpoint_id']).alias
        if streaming_event.delivering_activated:
            end_stream(user_id, streaming_event, alias=alias)
        
        if success:
            # Optionally, add a message indicating success
            return redirect('control:streaming_event_detail', id=streaming_event.id)
        else:
            # Optionally, add a message indicating failure ak budu messages v djangu 
            return redirect('control:streaming_event_detail', id=streaming_event.id)


@method_decorator(login_required, name='dispatch')
class AddEndpoint(View):
    def post(self, request, *args, **kwargs):
        endpoint_ids = request.POST.getlist("endpoint", [])
        streaming_event = get_object_or_404(StreamingEvent, id=self.kwargs['streaming_event_id'])
        user_id = request.user.id
        video_manager = VideoDataManager(streaming_event=streaming_event.id)
        time_point = request.POST.get("time_point", None)
        
        if time_point:
        
            hours, minutes, seconds = map(int, time_point.split(':'))
            total_minutes = hours * 60 + minutes + seconds / 60
            
        else:
            total_minutes = None
            
        chunk_id = video_manager.time_to_chunk(total_minutes)
         
        if streaming_event.delivering_activated:
            print("We are here ----------------- 240")
            try:
                init_stream.delay(user_id, streaming_event.id, chunk_id=chunk_id)
            except Exception as e:
                print(f'An error occurred: {e}')
         
        if endpoint_ids:
            
            print("We are here ----------------- 244")
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
           video_manager = VideoDataManager(streaming_event=streaming_event.id)
            
           start_time = data['start_time']
           chunk_time = data['chunk_time']
           endpoint =  data.get('end_points')
           
           endpoint = EndPointCfg.objects.get(id=endpoint)
           e_id = endpoint.id
           print('data repeat', data.get('repeat', None))
           if data.get('repeat'):
                repeat = data['repeat'] == "on"
                print("reapeat post ------>", repeat)
           chunk_id = video_manager.stream_time_to_chunk(chunk_time)
           
           print("Chunk id view --------------------->", chunk_id)
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


