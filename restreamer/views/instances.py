import os
import logging
from datetime import datetime, timedelta
from django.views import View
from linode_api4 import Instance, objects
from django.conf import settings
from django.contrib.auth.decorators import login_required, permission_required
from django.utils.decorators import method_decorator
from restreamer.models import StreamingEvent
from celery import shared_task
from django_celery_beat.models import PeriodicTask, IntervalSchedule

log = logging.getLogger('__name__')

# Cloud-init script
cloud_init_script = """#cloud-config
write_files:
  - path: /root/setup_server.sh
    permissions: '0755'
    content: |
      #!/bin/bash
      cd /root/kristian/delivering_server/delivering-service
      source /root/kristian/delivering_server/delivering-service/venv/bin/activate
      cd /root/kristian/delivering_server/delivering-service/delivering_service
      python manage.py runserver 0.0.0.0:8000 --insecure

runcmd:
  - /root/setup_server.sh
"""

class InstanceManager():
    
    def __init__(self, user_id=None):
        self.linode_client = settings.LINODE_CLIENT
        self.large_instance = settings.INSTANCE_TYPE_4G
        self.larger_instance = settings.INSTANCE_TYPE_8G
        self.cheapest_instance = settings.INSTANCE_TYPE_1G
        self.region = settings.REGION
        self.root_password = settings.ROOT_PASSWORD
        self.user_id = user_id
        self.instance_label = f"delivering-server-{self.user_id}"
        self.image_label = "delivering-server"
    
        
        
    def get_correct_image(self):
        """
        Retrieves the latest Linode image with the specified label.

        Returns:
            str: The ID of the latest Linode image with the specified label.
        
        Raises:
            ValueError: If no image with the specified label is found.
        """
        try:
            images = self.linode_client.images()
            matching_images = [image for image in images if image.label == self.image_label]

            if not matching_images:
                raise ValueError(f"No image found with the label '{self.image_label}'")

            # Sort matching images by creation date, assuming the API provides a `created` attribute as datetime
            matching_images.sort(key=lambda image: image.created, reverse=True)
            # Return the ID of the latest image
            return matching_images[0].id

        except Exception as e:
            logging.error(f"An error occurred while fetching the image: {e}")
            raise

    def create_image(self):
        pass
    
   
    def delete_instance(self):
        se = StreamingEvent.objects.filter(user=self.user_id).last()
        if not se.delivering_activated:
            #if chunk_not_arrived:
            instance = self.get_instance()
            if instance:
                instance.delete()
                return True
        return False
                
    def get_instance(self):
        for linode in self.linode_client.linode.instances():
            log.warning(f"linode label {linode.label}")
            if linode.label == self.instance_label:
                return linode
            log.warning(f"Instance with label {self.instance_label} not found.")
        return None
    
    
    def create_instance(self):
        try:
            se = StreamingEvent.objects.filter(user=self.user_id).last()
            se_count = se.end_points.count() 
            if se_count > 7:
                linode_type = self.larger_instance
            elif se_count == 1 or 'test' in se.short_description.lower():
                linode_type = self.cheapest_instance 
            else:
                linode_type = self.large_instance
            image_id = self.get_correct_image()

            for linode in self.linode_client.linode.instances():
                if linode.label == self.instance_label:
                    return
                
            new_linode = self.linode_client.linode.instance_create(
                ltype=linode_type,
                region=self.region,
                image=image_id,
                label=self.instance_label,
                root_pass=self.root_password,
                user_data=cloud_init_script
            )
            log.info("Instance created successfully: ", new_linode)
            return new_linode
        except Exception as e: 
            log.exception(f'An error occurred: {e}')
            
            
    def get_my_server_ip(self):
        instance = self.get_instance()
        if instance is None:
            log.warning("No instance found. Cannot retrieve IP address.")
            return None  # Or a default value, e.g., '0.0.0.0', depending on your requirements
        return instance.ipv4[0]
            
    def check_status(self):
        instance = self.get_instance()
        log.warning(f"Instance in check status -------------------> {instance}")
        if instance:
            return instance.status
        return "Inactive"
        
# if chunk didnt arried for 30 minutes from switching delivering and reaceiving shut down linode.        
def chunk_not_arrived(user_id):
    se = StreamingEvent.objects.filter(user=user_id).last()
    last_chunk_time = se.chunks.latest('created_at').created_at
    
    can_delete = (datetime.now() - last_chunk_time) > timedelta(minutes=30)
    
    return can_delete
    
         
