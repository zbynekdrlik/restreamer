from django.db import models
from django.conf import settings
from accounts.models import RestreamerUser
# Create your models here.

class YouTubeOAuthCredentials(models.Model):
    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        related_name='youtube_oauth',
        on_delete=models.CASCADE
    )
    access_token = models.TextField()
    refresh_token = models.TextField(blank=True, null=True)
    token_uri = models.CharField(max_length=255, blank=True, null=True)
    client_id = models.CharField(max_length=255, blank=True, null=True)
    client_secret = models.CharField(max_length=255, blank=True, null=True)
    scopes = models.TextField(blank=True, null=True)
    expiry = models.DateTimeField(blank=True, null=True)

    def __str__(self):
        return f"YouTube OAuth for {self.user.username}"
    
class DiscrodChannel(models.Model):
    name = models.CharField(max_length=250)
    id = models.BigIntegerField(null=True, blank=True)
    
class DiscordApp(models.Model):
    user = models.ForeignKey(RestreamerUser, on_delete=models.CASCADE, blank=True, null=True, related_name='discord_apps')
    bot_token = models.CharField(max_length=255)
    channel = models.ForeignKey(DiscrodChannel, on_delete=models.SET_NULL, related_name="bots")
    
