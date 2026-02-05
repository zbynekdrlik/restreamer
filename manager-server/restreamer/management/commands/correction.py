from restreamer.models import ChunkRecord

def rename_chunk_records(starting_id):
    # Get the last ID currently in the database
    last_id = ChunkRecord.objects.order_by('-id').first().id if ChunkRecord.objects.exists() else 72097

    # Start the renaming process from the specified starting ID
    next_id = starting_id + 1  # Start with the next ID

    # Loop to update the IDs of the pairs from the starting ID
    while next_id <= last_id:
        # Update the IDs of the pairs
        ChunkRecord.objects.filter(id=next_id).update(id=next_id + 1)
        
        # Move to the next pair of records
        next_id += 2

# Call the function to start renaming the chunk records from the specified starting ID
rename_chunk_records(72097)