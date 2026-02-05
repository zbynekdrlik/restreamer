from django.contrib import admin


class MyAdminSite(admin.AdminSite):
    site_header = "NL Restreamer"


admin_site = MyAdminSite(name="myadmin")
