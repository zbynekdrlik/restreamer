from django.contrib.admin.apps import AdminConfig
from django.template.response import TemplateResponse
from django.urls import reverse
from django.utils.html import format_html
from django.utils.safestring import mark_safe


class MyAdminConfig(AdminConfig):
    default_site = "nl_restreamer.admin.MyAdminSite"

    def index(self, request, extra_context=None):
        app_list = self.get_app_list(request)

        # Add your button HTML code to the context
        button_html = """
            <div style="padding: 10px;">
                <button id="my-ajax-button" class="button">Perform Action</button>
            </div>
            <script>
                $(function() {
                    $('#my-ajax-button').click(function() {
                        $.ajax({
                            url: '{url}',
                            type: 'POST',
                            success: function(response) {
                                alert(response.message);
                            },
                            error: function(response) {
                                alert('An error occurred while performing the action.');
                            }
                        });
                    });
                });
            </script>
        """
        button_url = reverse("ajax_action")
        button_html = format_html(button_html, url=button_url)
        extra_context = extra_context or {}
        extra_context["button_html"] = mark_safe(button_html)

        context = dict(
            self.each_context(request),
            title=self.index_title,
            app_list=app_list,
            **(extra_context or {}),
        )

        request.current_app = self.name

        return TemplateResponse(request, self.index_template or "admin/index.html", context)
