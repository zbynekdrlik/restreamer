from datetime import timedelta , datetime
from .models import ChunkRecord, StreamingEvent
import pytz

class VideoDataManager:
    def __init__(self, streaming_event):
        self.streaming_event = StreamingEvent.objects.get(id=streaming_event)
        self.video_data = ChunkRecord.objects.filter(streaming_event=self.streaming_event).order_by('created_at')
        pass
    
    def is_buffer_filled(self, buffer_time):
        if buffer_time:
            buffer_duration = buffer_time * 60
            if self.stream_length() >= buffer_duration:
                print("---------------Buffer is filled delivering allowed --------------------")
                return True
        return False
    
    def stream_length(self):
        if not self.video_data.exists():
            return 0

        total_length = self.video_data.count()
        return total_length
    
    def format_duration(self, seconds):
        seconds = round(seconds)
        return str(timedelta(seconds=seconds))

    def get_stream_length(self):
        total_length = self.stream_length()
        return self.format_duration(total_length)
    
    
    def time_to_chunk(self, minutes):
        stream_legth = self.stream_length()
        
        if minutes:
            target_seconds =  minutes * 60
            if target_seconds <= stream_legth:
                accumulated_seconds = 0
                
                for chunk in self.video_data:
                    accumulated_seconds += 1
                    if accumulated_seconds >= target_seconds:
                        return chunk.local_id
            return False
        return None
         
    def stream_time_to_chunk(self, time):

        local_tz = pytz.timezone('Europe/Bratislava')

        input_time = datetime.strptime(time, '%H:%M').time()
        input_datetime = datetime.combine(datetime.today(), input_time)
        input_datetime = local_tz.localize(input_datetime)

        
        for chunk in self.video_data:
            chunk_datetime = chunk.created_at.astimezone(local_tz)
            chunk_time = chunk_datetime.time()
            
            if chunk_time >= input_datetime.time():
                return chunk.local_id

        return None
            
            