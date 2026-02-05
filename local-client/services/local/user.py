import os
from pathlib import Path
from restreamer.models import ClientProfile
from django.conf import settings


def set_uuid():
    file_dir = settings.BASE_DIR.parent.parent.parent
    print("file dir", file_dir)
    conf_file = os.path.join(file_dir, 'config.txt')
    with open(conf_file, 'r') as f:  # Change 'w' to 'r' for read mode
        line = f.readline().strip()
        user_id = line.split(" ")[1]

    ClientProfile.objects.create(user_id=user_id)
    os.remove(conf_file)
