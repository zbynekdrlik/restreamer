from django.contrib.auth import logout
from django.http import HttpResponseForbidden


class BlockUnknownUserMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # Check if the user is authenticated and belongs to the 'unknown-user' group
        if request.user.is_authenticated and request.user.groups.filter(name="unknown-user").exists():
            # Log out the user
            logout(request)

            # Optionally, redirect them to a specific page (e.g., login page)
            response = HttpResponseForbidden("Ask admin to give you permissions for further actions")
            response["Location"] = "/accounts/login/"
            response.status_code = 302
            return response

        response = self.get_response(request)
        return response
