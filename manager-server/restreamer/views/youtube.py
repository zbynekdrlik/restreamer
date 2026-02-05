from django.contrib.auth.decorators import login_required
from django.shortcuts import render
from django.utils.decorators import method_decorator
from django.views import View
from restreamer.youtube_api import create_youtube_client, get_credentials, list_scheduled_broadcasts


@method_decorator(login_required, name="dispatch")
class GoLiveYt(View):
    def get(self, request):

        pass

    pass


@method_decorator(login_required, name="dispatch")
class YtLivePage(View):
    def get(self, request):
        template_name = "restreamer/youtube_page.html"

        try:
            credentials = get_credentials()
            youtube = create_youtube_client(credentials)
            broadcast_dict = list_scheduled_broadcasts(youtube)
            if len(broadcast_dict) == 0:
                broadcast_dict = {}

            print("broadcast dict ---------->", broadcast_dict)
        except Exception as e:
            print(f"An error occurred: {e}")

        print("Print all broadcasts ----->", broadcast_dict)

        context = {"broadcasts": broadcast_dict}
        return render(request, template_name, context)
