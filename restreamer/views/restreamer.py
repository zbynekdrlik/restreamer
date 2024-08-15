from django.shortcuts import render
from django.views import View
from django.contrib.auth.decorators import login_required, permission_required
from django.utils.decorators import method_decorator
from restreamer.models import StreamingEvent

@method_decorator(login_required, name='dispatch')
class StreamingEventView(View):
    def get(self, request):
        template_name = "restreamer/home.html"
        user = request.user
        streaming_events = StreamingEvent.objects.filter(user=user).order_by("id")

        print(streaming_events)

        context = {
            "streaming_events": streaming_events,
        }

        return render(request, template_name, context)



