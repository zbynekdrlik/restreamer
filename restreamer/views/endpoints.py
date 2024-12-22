import logging

from django.views import View
from django.shortcuts import (get_object_or_404, redirect,
                              render)
from django.utils.decorators import method_decorator
from django.contrib import messages
from django.http import JsonResponse
from restreamer.models import EndPointCfg
from django.contrib.auth.decorators import login_required


@method_decorator(login_required, name='dispatch')
class EditEndpoint(View):
    
    def get(self, request, endpoint_id):
    
        # Replace `Endpoint` with your actual model name
        endpoint = get_object_or_404(EndPointCfg, id=endpoint_id)
        data = {
            'id': endpoint.id,
            'name': endpoint.name,
            'service_type': endpoint.service_type,
            'stream_key': endpoint.stream_key,
        }
        return JsonResponse(data)
        
    def post(self, request):
        data = request.POST
        
        endpoint_id = data.get('endpint_id', None)
        endpint_name = data.get('endpoint_name', None)
        service_type = data.get('service_type', None)
        stream_key = data.get('stream_key', None)
        enabled =  data.get('enabled', None)
        
        endpoint = EndPointCfg.objects.get(id=endpoint_id, user=request.user)
        
        endpoint.alias = endpint_name
        endpoint.service_type = service_type
        endpoint.stream_key = stream_key
        endpoint.enabled = enabled
        
        endpoint.save()
        
        return redirect('control:endpoint_edit')