from restreamer.models import ChunkRecord, StreamingEvent


def delete_local_chunks():
    se = StreamingEvent.objects.filter(chunks__isnull=False).first()
    se.chunks.all().delete()

def get_buffer_time():
    all_chunks = ChunkRecord.objects.filter(
        streaming_event=StreamingEvent.objects.filter(chunks__isnull=False).first(),
        send=False
    )
    if all_chunks.exists():
        # Calculate the time delta
        time_delta = all_chunks.last().created_at - all_chunks.first().created_at

        # Extract hours, minutes, and seconds
        total_seconds = int(time_delta.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)

        # Format the time nicely
        formatted_time = f"{hours:02}:{minutes:02}:{seconds:02}"
    else:
        formatted_time = "00:00:00"

    return formatted_time