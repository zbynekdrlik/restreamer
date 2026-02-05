import os
from unittest.mock import MagicMock

os.environ.setdefault("SECRET_KEY", "ci-test-secret-key")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "ci-dummy")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "ci-dummy")
os.environ.setdefault("AWS_STORAGE_BUCKET_NAME", "ci-bucket")
os.environ.setdefault("AWS_S3_REGION_NAME", "us-east-1")
os.environ.setdefault("OBJECT_STORAGE_URL", "https://localhost")
os.environ.setdefault("LINODE_TOKEN", "ci-dummy")
os.environ.setdefault("CELERY_BROKER_URL", "memory://")
os.environ.setdefault("CELERY_RESULT_BACKEND", "cache+memory://")
os.environ.setdefault("MANAGER_SERVER_URL", "http://localhost:8000")

from nl_restreamer.settings import *  # noqa: F403, E402

S3_CLIENT = MagicMock()

CELERY_BROKER_URL = "memory://"
CELERY_RESULT_BACKEND = "cache+memory://"
CELERY_TASK_ALWAYS_EAGER = True

CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels.layers.InMemoryChannelLayer",
    },
}

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    },
}
