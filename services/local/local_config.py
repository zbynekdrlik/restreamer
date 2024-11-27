import sys
import os


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'local_client.nl_restreamer.settings')

import django
django.setup()


from services.local.user import set_uuid


def setup():
    # save uuid of user to the db
    set_uuid()


if __name__ == '__main__':
    setup()