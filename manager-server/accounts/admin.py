from django.contrib import admin

# Register your models here.
from simple_history.admin import SimpleHistoryAdmin

from .models import RestreamerUser


@admin.register(RestreamerUser)
class RestreamerUserAdmin(SimpleHistoryAdmin):
    list_display = ["first_name", "email", "last_name", "is_active"]


# Zaregistrujte globální historii pro všechny historické záznamy
