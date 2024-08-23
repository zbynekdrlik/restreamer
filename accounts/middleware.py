from django.http import HttpResponseForbidden

class BlockUnknownUserMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # If the user belongs to the 'unknown-user' group, restrict access
        if request.user.is_authenticated and request.user.groups.filter(name='unknown-user').exists():
            # Block specific views or actions (e.g., based on request path)
            return HttpResponseForbidden("Ask admin to give you permissions for further actions")
        
        response = self.get_response(request)
        return response