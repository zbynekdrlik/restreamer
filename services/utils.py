import logging
import os

from django.conf import settings

log = logging.getLogger(__name__)

def get_client_ip(request):
    """
    Retrieve the client IP address from the request.
    """
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        # HTTP_X_FORWARDED_FOR can contain multiple IPs; take the first one
        ip = x_forwarded_for.split(',')[0].strip()
    else:
        ip = request.META.get('REMOTE_ADDR')
    return ip


def delete_s3_chunks(chunk_keys):
    """
    Deletes the specified chunks from the S3 bucket.

    Args:
        chunk_keys (list): List of S3 keys for the chunks to delete.
    """
    log.info("------------------- Delete chunks called ----------------------")
    bucket_name = os.environ.get('AWS_STORAGE_BUCKET_NAME')
    client = settings.S3_CLIENT
    
    try:
        # Prepare objects for batch deletion
        objects_to_delete = [{'Key': key} for key in chunk_keys]

        if objects_to_delete:
            response = client.delete_objects(
                Bucket=bucket_name,
                Delete={
                    'Objects': objects_to_delete,
                    'Quiet': True
                }
            )

            errors = response.get('Errors', [])

            for err in errors:
                log.warning(f"Failed to delete chunk from S3: {err['Key']} - {err['Message']}")

    except Exception as e:
        log.error(f"Error while deleting chunks from S3: {e}")
        
        
def calculate_bucket_size():
    """
    Calculates the total size of all objects in an S3 bucket.

    Args:
        bucket_name (str): Name of the S3 bucket.

    Returns:
        float: Total size in GB.
    """
    client = settings.S3_CLIENT  # Use your configured client if not default
    total_size_bytes = 0
    bucket_name = os.environ.get('AWS_STORAGE_BUCKET_NAME')

    try:
        paginator = client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name):
            if 'Contents' in page:
                total_size_bytes += sum(obj['Size'] for obj in page['Contents'])

    except Exception as e:
        print(f"Error calculating bucket size: {e}")
        return None

    total_size_gb = total_size_bytes / (1024 ** 3)  # Convert bytes to GB
    return total_size_gb