import uuid

from django.db import models
from django.contrib.auth.models import AbstractUser
from simple_history.models import HistoricalRecords

# Create your models here.

class RestreamerUser(AbstractUser):
    api_key = models.UUIDField(default=uuid.uuid4, editable=False, unique=True)
    email = models.EmailField(unique=True)  # Ensure email is unique
    first_name = models.CharField(max_length=250)
    last_name = models.CharField(max_length=250)
    is_active = models.BooleanField(default=True)
    history = HistoricalRecords()
    
    def get_user_id(self):
        return self.id
    


