from django.urls import re_path

from channels.auth import AuthMiddlewareStack
from django.core.asgi import get_asgi_application
from channels.routing import ProtocolTypeRouter, URLRouter
from django.urls import path
from channels.security.websocket import AllowedHostsOriginValidator
from django.urls import re_path
from . import consumers

websocket_urlpatterns =  [],


# re_path(r'ws/buffer_health/$', BufferHealthConsumer.as_asgi())
# from .consumers import BufferHealthConsumer