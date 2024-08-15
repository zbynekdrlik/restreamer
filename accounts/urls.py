from django.urls import path
from django.contrib.auth import views as auth_views
from .views import SignUpView, CustomLogoutView, CustomLoginView

app_name = 'accounts'

urlpatterns = [
    path('login/', CustomLoginView.as_view(), name='login'),
    path('register/', SignUpView.as_view(), name='register'),
    path('logout/', CustomLogoutView.as_view(), name='logout')
]
