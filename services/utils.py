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
    Deletes the specified chunks from the S3 bucket with enhanced debugging.

    Args:
        chunk_keys (list): List of S3 keys for the chunks to delete.
    """
    log.info("------------------- Delete chunks called ----------------------")
    bucket_name = os.environ.get('AWS_STORAGE_BUCKET_NAME')
    client = settings.S3_CLIENT
    
    # Log the bucket name and the number of keys to be deleted
    log.info(f"Target bucket: {bucket_name}")
    log.info(f"Number of chunks to delete: {len(chunk_keys)}")
    log.debug(f"Chunk keys: {chunk_keys}")

    try:
        # Prepare objects for batch deletion
        objects_to_delete = [{'Key': key} for key in chunk_keys]

        if objects_to_delete:
            log.info(f"Attempting to delete {len(objects_to_delete)} objects from S3...")

            # Perform the delete operation
            response = client.delete_objects(
                Bucket=bucket_name,
                Delete={
                    'Objects': objects_to_delete,
                    'Quiet': False  # Set to False to get detailed feedback
                }
            )

            # Log the full response for debugging
            log.debug(f"Delete response: {response}")

            # Check for errors in the response
            errors = response.get('Errors', [])
            if errors:
                log.warning("Some objects failed to delete:")
                for err in errors:
                    log.warning(f"Failed: {err.get('Key')} - {err.get('Message')}")
            else:
                log.info("All chunks deleted successfully.")

        else:
            log.info("No objects to delete from the bucket.")

    except client.exceptions.NoSuchBucket as e:
        log.error(f"The specified bucket does not exist: {bucket_name} - {e}")
    except client.exceptions.ClientError as e:
        log.error(f"Client error occurred: {e}")
    except Exception as e:
        log.error(f"An unexpected error occurred while deleting chunks from S3: {e}")

    log.info("------------------- Delete chunks completed -------------------")
    
