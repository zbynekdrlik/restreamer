import os
import logging
from datetime import datetime
from django.views import View
from linode_api4 import Instance, objects
from django.conf import settings
from django.contrib.auth.decorators import login_required, permission_required
from django.utils.decorators import method_decorator
from restreamer.models import StreamingEvent
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
            
            print("Matching image ------------->", matching_images[0].id)
            # Return the ID of the latest image
            return matching_images[0].id

        except Exception as e:
            logging.error(f"An error occurred while fetching the image: {e}")
            raise

    def create_image(self):
        pass
    
    def delete_instance(self):
        print("we are here 78")
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
            if linode.label == self.instance_label:
                return linode
        log.warning(f"Instance with label {self.instance_label} not found.")
        return None 
    
    
    def create_instance(self):
        try:
            se = StreamingEvent.objects.filter(user=self.user_id).last()
            if se.end_points.count() > 7:
                linode_type = self.larger_instance        
            else:
                linode_type = self.large_instance
            image_id = self.get_correct_image()
            print("image_id = self.get_correct_image() -------------------> ", image_id)
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
            print("Instance created successfully: ", new_linode)
            return new_linode
        except Exception as e: 
            print(f'An error occurred: {e}')
            
            
    def get_my_server_ip(self):
        return self.get_instance().ipv4[0]
            
    def check_status(self):
        print(" self.get_instance().status -------------------> ",  self.get_instance().status)
        return self.get_instance().status
        
        
        
        

         
