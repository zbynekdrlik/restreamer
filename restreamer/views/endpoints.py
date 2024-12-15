import logging

from django.views import View
from django.shortcuts import (get_object_or_404, redirect,
                              render)
from django.utils.decorators import method_decorator
from django.contrib import messages

from restreamer.models import EndPointCfg


class EditEndpoint(View):
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
        
        