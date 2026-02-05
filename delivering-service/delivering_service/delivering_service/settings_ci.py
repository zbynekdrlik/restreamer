import os
from unittest.mock import MagicMock

os.environ.setdefault("SECRET_KEY", "ci-test-secret-key")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "ci-dummy")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "ci-dummy")
os.environ.setdefault("SENTRY_DSN", "")

from delivering_service.settings import *  # noqa: F403, E402

S3_CLIENT = MagicMock()

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    },
}
