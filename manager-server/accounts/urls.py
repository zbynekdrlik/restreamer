from django.contrib.auth import views as auth_views
from django.urls import path

from .views import CustomLoginView, CustomLogoutView, SignUpView

urlpatterns = [
    path("login/", CustomLoginView.as_view(), name="login"),
    path("register/", SignUpView.as_view(), name="register"),
    path("logout/", CustomLogoutView.as_view(), name="logout"),
    path(
        "change-password/",
        auth_views.PasswordChangeView.as_view(template_name="registration/change_password.html"),
        name="password_change",
    ),
    path(
        "password-reset/",
        auth_views.PasswordResetView.as_view(template_name="registration/password_reset.html"),
        name="password_reset",
    ),
    path(
        "password-reset/done/",
        auth_views.PasswordResetDoneView.as_view(template_name="registration/password_reset_done.html"),
        name="password_reset_done",
    ),
    path(
        "reset/<uidb64>/<token>/",
        auth_views.PasswordResetConfirmView.as_view(template_name="registration/password_reset_confirm.html"),
        name="password_reset_confirm",
    ),
    path(
        "reset/done/",
        auth_views.PasswordResetCompleteView.as_view(template_name="registration/password_reset_complete.html"),
        name="password_reset_complete",
    ),
]
