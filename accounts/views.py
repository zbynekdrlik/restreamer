from django.shortcuts import render
from django.views import generic
from .forms import RegistrationForm
from django.urls import reverse_lazy
from django.db import IntegrityError
from django.contrib.auth.views import LogoutView, LoginView

class SignUpView(generic.CreateView):
    template_name = 'registration/register.html'
    form_class = RegistrationForm
    success_url = reverse_lazy('control:home')

    def form_valid(self, form):
        try:
            return super().form_valid(form)
        except IntegrityError:
            form.add_error(None, "A user with that username or email already exists.")
            return self.form_invalid(form)

class CustomLogoutView(LogoutView):
    next_page = reverse_lazy('accounts:login')
    
    def get(self, request, *args, **kwargs):
        return self.post(request, *args, **kwargs)
    
class CustomLoginView(LoginView):
    success_url = reverse_lazy('control/home/')
