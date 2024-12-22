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
    def get(self, request):
        # Fetch `endpoint_id` from query parameters
        endpoint_id = request.GET.get('endpoint_id', None)
        if not endpoint_id:
            return JsonResponse({'error': 'Missing endpoint_id'}, status=400)

        # Fetch the endpoint data
        endpoint = get_object_or_404(EndPointCfg, id=endpoint_id, user=request.user)
        data = {
            'id': endpoint.id,
            'name': endpoint.alias,
            'service_type': endpoint.service_type,
            'stream_key': endpoint.stream_key,
            'enabled': endpoint.enabled
        }
        return JsonResponse(data)

    def post(self, request):
        # Handle form submission
        data = request.POST
        endpoint_id = data.get('endpoint_id', None)
        if not endpoint_id:
            return JsonResponse({'error': 'Missing endpoint_id'}, status=400)

        # Fetch and update the endpoint
        endpoint = get_object_or_404(EndPointCfg, id=endpoint_id, user=request.user)
        endpoint.alias = data.get('endpoint_name')
        endpoint.service_type = data.get('service_type')
        endpoint.stream_key = data.get('stream_key')
        endpoint.enabled = data.get('enabled', 'off') == 'on'  # Handle checkbox
        endpoint.save()

        return JsonResponse({'success': True})