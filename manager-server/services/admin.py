from django.contrib import admin

from .models import DiscordApp, DiscrodChannel, YouTubeOAuthCredentials

admin.site.register(YouTubeOAuthCredentials)
admin.site.register(DiscordApp)
admin.site.register(DiscrodChannel)
# Register your models here.
