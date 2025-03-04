import logging
from django.shortcuts import (get_object_or_404, redirect,
                              render)
from django.utils.decorators import method_decorator
from django.views import View
from django.contrib.auth.decorators import login_required
from ..models import ChunkRecord, EndPointCfg, StreamingEvent
from ..forms import EndPointForm, StreamingEventForm
from restreamer.video_data import VideoDataManager
from restreamer.views.instances import InstanceManager
from restreamer.views.delivering import DeliveringManger
from django.contrib import messages
from django.contrib.auth.mixins import LoginRequiredMixin
from django.views.generic import TemplateView

from django.http import JsonResponse


log = logging.getLogger(__name__)

@method_decorator(login_required, name='dispatch')
class StreamingEventView(View):
    def get(self, request):
        template_name = "restreamer/home.html"
        user = request.user

        # Get streaming events and handle the case where there are none
        streaming_events = StreamingEvent.objects.filter(user=user).order_by("id")
        video_length = '00:00'

        in_manager = InstanceManager(user.id)

        # Default `streaming_event_id` to `None` if no events exist
        
        streaming_event_id = streaming_events.first().id if streaming_events.exists() else None
        buffer_time = streaming_events.first().buffer
        del_manager = None

        if streaming_event_id:
            del_manager = DeliveringManger(user.id, streaming_event_id)

        instance_status = in_manager.check_status()
        is_preparing = False

        if instance_status != 'Inactive' and del_manager:
            server_ready = del_manager.is_server_ready()
            if not server_ready:
                is_preparing = True

        # Handle a streaming event with chunks or set a default value
        try:
            streaming_event = StreamingEvent.objects.filter(chunks__isnull=False, user=user).first()
            if streaming_event:
                video_manager = VideoDataManager(streaming_event.id)
                video_length = video_manager.get_stream_length()
        except StreamingEvent.DoesNotExist:
            pass

        context = {
            "instance_status": instance_status,
            "streaming_events": streaming_events,
            'video_length': video_length,
            'is_preparing': is_preparing,
            'is_buffering' : video_manager.is_buffer_filled(buffer_time)
        }

        return render(request, template_name, context)


# Receiving data from user when create new streaming event
class CreateStreamView(LoginRequiredMixin, TemplateView):
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

                    messages.success(request, 'Streaming event successfully created!')
                    return redirect('control:home')

                except Exception as e:
                    log.exception(f'Error saving form {e}')
                    messages.error(request, 'There was an error saving the streaming event.')

            elif endpoint_form.is_valid():
                endpoint = endpoint_form.save(commit=False)
                endpoint.user = request.user
                endpoint.save()

                messages.success(request, f'Endpoint {endpoint.alias} successfully created!')
                return redirect('control:streaming_event_create')
                
            else:
                messages.error(request, 'There was an error creating new endpoint.')
                log.error(f'Invalid form {streaming_event_form.errors}')

        except Exception as e:
            log.exception(f'Error: {e}')
            streaming_event_form = None
            endpoint_form = None
            messages.error(request, 'Some error occured')


        context = self.get_context_data(streaming_event_form=streaming_event_form, endpoint_form=endpoint_form)
        return self.render_to_response(context)


@method_decorator(login_required, name='dispatch')
class StreamingEventDetailView(View):
    def get(self, request, *args, **kwargs):
        template_name = 'restreamer/streaming_event.html'
        
        streaming_event = StreamingEvent.objects.get(id=self.kwargs['id'])
        
        selected_endpoints = streaming_event.end_points.all()
        endpoint_form = EndPointForm()
        available_endpoints = EndPointCfg.objects.filter(
            user=request.user
        ).exclude(
            id__in=selected_endpoints.values_list('id', flat=True)
        )
        video_manager = VideoDataManager(streaming_event.id)
        
        video_length = video_manager.get_stream_length()
        
        buffer_display = streaming_event.get_buffer_display()
        
        context = {
            'endpoints': selected_endpoints,
            'streaming_event':streaming_event,
            'endpoint_form': endpoint_form,
            'available_endpoints': available_endpoints,
            'video_length': video_length,
            'buffer': buffer_display,
            }
        
        return render(request, template_name, context)

@method_decorator(login_required, name='dispatch')
class StreamingEventEdit(View):
    def post(self, request, streaming_event_id):
        data = request.POST
        buffer_time = data.get('buffer-time')
        streaming_event = get_object_or_404(StreamingEvent, id=streaming_event_id)
        streaming_event.buffer = buffer_time
        streaming_event.save()
        return redirect('control:home')
    

@method_decorator(login_required, name='dispatch')
class VideoLengthData(View):
    def get(self, request, id):
        
        streaming_event = StreamingEvent.objects.get(id=id)
        video_manager = VideoDataManager(streaming_event.id)
        video_length = video_manager.get_stream_length()

        return JsonResponse({'video_length': video_length})