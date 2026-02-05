import logging

from django.core.management.base import BaseCommand

from restreamer.models import ClientProfile

log = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Create a local client profile with a UUID from the manager server"

    def add_arguments(self, parser):
        parser.add_argument(
            "--uuid",
            type=str,
            required=True,
            help="Client UUID assigned by the manager server",
        )

    def handle(self, *args, **options):
        user_id = options["uuid"]
        ClientProfile.objects.create(user_id=user_id)
        self.stdout.write(self.style.SUCCESS(f"Created client profile with UUID: {user_id}"))
