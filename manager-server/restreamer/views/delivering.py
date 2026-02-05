import logging

from typing import Any
import requests
from django.views import View
from . instances import InstanceManager as IM
from requests.exceptions import RequestException
from restreamer.models import StreamingEvent
from accounts.models import RestreamerUser
from restreamer.models import ChunkRecord, EndPointCfg

log = logging.getLogger('__name__')

class DeliveringManger:
    
    def __init__(self, user_id=None, streamign_event_id=None):
        user = RestreamerUser.objects.get(id=user_id)
        self.user_id = user_id
        self.streaming_event = StreamingEvent.objects.get(id=streamign_event_id, user=user)
        self.stream_data = self.streaming_event.stream_info()
        self.session = requests.Session()
        

    def get_url(self):
        server_manger = IM(self.user_id)
        instance_ip = server_manger.get_my_server_ip()
        url = f'{instance_ip}:8000'
        return url
    
    def is_server_ready(self):
        """
        Check if Django is initialized and ready to accept requests.
        """
        url = f"http://{self.get_url()}/api/raceive_init_data/"
        try:
            response = self.session.get(url, timeout=1)  # Timeout ensures it doesn't hang
            log.info(f"response {response.status_code}: {response.text}")
            if response.status_code == 200:
                return True
        except requests.ConnectionError as e:
            log.error(f'There is error connecting {url} !!! {e}')
            return False
        return False
    
    # unused
    def init_delivery(self):
        response = self.session.get(f"{self.get_url}/connect", params={"init_data": self.stream_data})
        
    def send_chunk_data(self, chunk_data):
        """
        Send chunk data to the server.
        
        :param chunk_data: The chunk data to be sent
        """
        try:
            response = self.session.post(f'{self.get_url}/get-chunk_data', data=chunk_data)
            response.raise_for_status()
            print("Chunk data sent")
        except RequestException as e:
            print("Failed to send chunk data:", e)
            raise
    
    # This is actualy initalization of stream so from witch particular chunk and where to stream.
    def send_init_data(self, chunk_id=None, endpoint_id=None):
        if endpoint_id is None:
            endpoints = self.streaming_event.end_points.exclude(is_fast=True).values("alias", "service_type", "stream_key")
        
        else:
            endpoints = EndPointCfg.objects.filter(id=endpoint_id).values("alias", 'service_type', "stream_key")
    
        stream_id = self.streaming_event.identifier
        chunk_id = chunk_id
        if chunk_id is None:
            chunk_record = ChunkRecord.objects.filter(identifier=stream_id).first()
            chunk_id = chunk_record.local_id if chunk_record else None
        
        data = {
            'endpoints': list(endpoints),
            'chunk_id': chunk_id,
            'stream_id': stream_id
        }
    
        url = f"http://{self.get_url()}/api/raceive_init_data/"
        try:
            response = self.session.post(url, json=data)
            response.raise_for_status()  # Raises an HTTPError for bad responses
            log.info("Stream Initialized Successfully")
        except requests.exceptions.RequestException as e:
            print(f"Failed to send data: {e}")
    
    # End streaming for all endpoints or for selected 
    def end_delivery(self, alias=None):
        url = f"http://{self.get_url()}/api/end_stream/"
        
        data = {
            'alias':alias
        }
        
        try:
            response = self.session.post(url, json=data)
            response.raise_for_status()  # Raises an HTTPError for bad responses
        except Exception as e:
            log.info(f"Failed to send data: {e}")
        log.info(f" Sygnal for interuption for {alias if alias else 'All'} {'stream' if alias else 'streams'} sent")
        
        
        
        
    
 