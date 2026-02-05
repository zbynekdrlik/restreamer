import os
from unittest.mock import MagicMock

os.environ.setdefault("SECRET_KEY", "ci-test-secret-key")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "ci-dummy")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "ci-dummy")
os.environ.setdefault("LINODE_TOKEN", "ci-dummy")
os.environ.setdefault("DB_NAME", "ci_test")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "postgres")
os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("EMAIL_HOST_USER", "ci@test.com")
os.environ.setdefault("EMAIL_HOST_PASSWORD", "ci-dummy")
os.environ.setdefault("DEFAULT_FROM_EMAIL", "ci@test.com")
os.environ.setdefault("CRON_SECRET_TOKEN", "ci-dummy")
os.environ.setdefault("INSTANCE_TYPE_4G", "g6-standard-2")
os.environ.setdefault("INSTANCE_TYPE_1G", "g6-nanode-1")
os.environ.setdefault("INSTANCE_TYPE_8G", "g6-standard-4")
os.environ.setdefault("INSTANCE_REGION", "eu-central")
os.environ.setdefault("ROOT_SERVER_PASSWORD", "ci-dummy")

from nl_restreamer.settings import *  # noqa: F403, E402

S3_CLIENT = MagicMock()
LINODE_CLIENT = MagicMock()

CELERY_BROKER_URL = "memory://"
CELERY_RESULT_BACKEND = "cache+memory://"
CELERY_TASK_ALWAYS_EAGER = True

CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels.layers.InMemoryChannelLayer",
    },
}

EMAIL_BACKEND = "django.core.mail.backends.locmem.EmailBackend"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.environ["DB_NAME"],
        "USER": os.environ["DB_USER"],
        "PASSWORD": os.environ["DB_PASSWORD"],
        "HOST": os.environ["DB_HOST"],
        "PORT": os.environ["DB_PORT"],
    },
    "client_db": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    },
}
