from django.contrib import admin
from .models import RestreamerUser
# Register your models here.
from simple_history.admin import SimpleHistoryAdmin
from simple_history.models import HistoricalRecords
from django.template.response import TemplateResponse

@admin.register(RestreamerUser)
class RestreamerUserAdmin(SimpleHistoryAdmin):
        list_display = ['first_name', 'email', 'last_name', 'is_active']
        
        
# Zaregistrujte globální historii pro všechny historické záznamy
