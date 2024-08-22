from django import forms
from .models import StreamingEvent, EndPointCfg


class StreamingEventForm(forms.ModelForm):

    class Meta:
        model = StreamingEvent
        fields = ['short_description', 'date_of_event', 'end_points', 'buffer']
        widgets = {
            'date_of_event': forms.DateTimeInput(attrs={'type': 'datetime-local'})
        }
    def __init__(self, *args, **kwargs):
        user = kwargs.pop('user', None)
        super(StreamingEventForm, self).__init__(*args, **kwargs)
        if user:
            self.fields['end_points'].queryset = EndPointCfg.objects.filter(user=user)

class EndPointForm(forms.ModelForm):

    class Meta:
        model = EndPointCfg
        fields = ['alias', 'service_type', 'stream_key', 'enabled']
        widgets = {'stream_key': forms.PasswordInput()}
   