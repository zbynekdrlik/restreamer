import contextlib
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from django.core.management import call_command
from django.test import TestCase

from restreamer.models import StreamingEvent

SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent / "local-client" / "scripts"


class InpointServiceTests(TestCase):
    """Test the inpoint_service management command handles missing/inactive streaming events."""

    @patch("restreamer.management.commands.inpoint_service.redis_client")
    @patch("restreamer.management.commands.inpoint_service.time.sleep", side_effect=KeyboardInterrupt)
    def test_no_streaming_event_does_not_crash(self, mock_sleep, mock_redis):
        """inpoint_service must not crash when no StreamingEvent exists."""
        self.assertFalse(StreamingEvent.objects.exists())
        with contextlib.suppress(KeyboardInterrupt):
            call_command("inpoint_service", stdout=StringIO(), stderr=StringIO())
        mock_redis.rpush.assert_called_with("inpoint_icon_status", "inpoint_waiting")

    @patch("restreamer.management.commands.inpoint_service.redis_client")
    @patch("restreamer.management.commands.inpoint_service.time.sleep", side_effect=KeyboardInterrupt)
    def test_inactive_streaming_event_waits(self, mock_sleep, mock_redis):
        """inpoint_service must wait when streaming event has receiving_activated=False."""
        StreamingEvent.objects.create(
            identifier="test-event",
            short_description="test",
            receiving_activated=False,
        )
        with contextlib.suppress(KeyboardInterrupt):
            call_command("inpoint_service", stdout=StringIO(), stderr=StringIO())
        mock_redis.rpush.assert_called_with("inpoint_icon_status", "inpoint_waiting")


class ScriptPathTests(TestCase):
    """Verify all scripts reference 'local-client' (hyphen) not 'local_client' (underscore).

    The monorepo renamed the directory from local_client to local-client.
    All scripts must use the new name so NSSM services find the correct path.
    """

    def _get_script_files(self):
        """Return all .ps1 and .bat files in the scripts directory."""
        scripts = []
        for ext in ("*.ps1", "*.bat"):
            scripts.extend(SCRIPTS_DIR.glob(ext))
        return scripts

    def test_scripts_directory_exists(self):
        self.assertTrue(SCRIPTS_DIR.is_dir(), f"Scripts directory not found: {SCRIPTS_DIR}")

    def test_no_legacy_local_client_underscore_in_ps1_files(self):
        """PowerShell scripts must not reference the old local_client directory."""
        ps1_files = list(SCRIPTS_DIR.glob("*.ps1"))
        self.assertTrue(len(ps1_files) > 0, "No .ps1 files found")
        for script in ps1_files:
            content = script.read_text()
            self.assertNotIn(
                "local_client",
                content,
                f"{script.name} still references legacy 'local_client' directory",
            )

    def test_no_legacy_local_client_underscore_in_bat_files(self):
        """Batch scripts must not reference the old local_client directory."""
        bat_files = list(SCRIPTS_DIR.glob("*.bat"))
        self.assertTrue(len(bat_files) > 0, "No .bat files found")
        for script in bat_files:
            content = script.read_text()
            self.assertNotIn(
                "local_client",
                content,
                f"{script.name} still references legacy 'local_client' directory",
            )

    def test_ps1_scripts_use_correct_directory(self):
        """PS1 scripts that navigate to the Django app must use local-client."""
        navigation_scripts = [
            "start_inpoint_service.ps1",
            "start_endpoint_service.ps1",
            "start_trayicon_service.ps1",
            "start_update_service.ps1",
        ]
        for name in navigation_scripts:
            script = SCRIPTS_DIR / name
            self.assertTrue(script.exists(), f"{name} not found")
            content = script.read_text()
            self.assertIn(
                "local-client",
                content,
                f"{name} does not reference 'local-client' directory",
            )

    def test_bat_scripts_use_correct_directory(self):
        """Batch scripts that cd into the Django app must use local-client."""
        cd_scripts = [
            "stream_ready_worker.bat",
            "clery_beat.bat",
        ]
        for name in cd_scripts:
            script = SCRIPTS_DIR / name
            self.assertTrue(script.exists(), f"{name} not found")
            content = script.read_text()
            self.assertIn(
                "local-client",
                content,
                f"{name} does not reference 'local-client' directory",
            )

    def test_inpoint_service_bat_invokes_ps1(self):
        """inpoint_service.bat must call start_inpoint_service.ps1."""
        script = SCRIPTS_DIR / "inpoint_service.bat"
        self.assertTrue(script.exists())
        content = script.read_text()
        self.assertIn("start_inpoint_service.ps1", content)

    def test_endpoint_service_bat_invokes_ps1(self):
        """endpoint_service.bat must call start_endpoint_service.ps1."""
        script = SCRIPTS_DIR / "endpoint_service.bat"
        self.assertTrue(script.exists())
        content = script.read_text()
        self.assertIn("start_endpoint_service.ps1", content)

    def test_run_services_registers_all_nssm_services(self):
        """run_services.bat must register all 5 NSSM services."""
        script = SCRIPTS_DIR / "run_services.bat"
        self.assertTrue(script.exists())
        content = script.read_text()
        expected_services = [
            "RedisServer",
            "inpoint_service",
            "endpoint_service",
            "CeleryWorker",
            "CeleryBeat",
        ]
        for svc in expected_services:
            self.assertIn(
                f"nssm install {svc}",
                content,
                f"run_services.bat missing nssm install for {svc}",
            )

    def test_celery_worker_uses_threads_pool(self):
        """Celery worker on Windows must use --pool=threads."""
        script = SCRIPTS_DIR / "stream_ready_worker.bat"
        content = script.read_text()
        self.assertIn("--pool=threads", content)

    def test_celery_scripts_use_python_module_not_shim(self):
        """Celery bat scripts must use 'python.exe -m celery' not bare 'celery' shim.

        The celery.exe shim fails under NSSM on Python 3.13 Windows.
        Using the full python.exe path with -m celery avoids the shim.
        """
        for name in ("stream_ready_worker.bat", "clery_beat.bat"):
            script = SCRIPTS_DIR / name
            content = script.read_text()
            self.assertIn(
                "-m celery",
                content,
                f"{name} must use '-m celery' module invocation instead of bare 'celery' command",
            )
            self.assertIn(
                "python.exe",
                content,
                f"{name} must use explicit python.exe path for NSSM compatibility",
            )

    def test_all_ps1_activate_venv(self):
        """All PS1 service scripts must activate the virtual environment."""
        ps1_service_scripts = [
            "start_inpoint_service.ps1",
            "start_endpoint_service.ps1",
            "start_trayicon_service.ps1",
            "start_update_service.ps1",
        ]
        for name in ps1_service_scripts:
            script = SCRIPTS_DIR / name
            content = script.read_text()
            self.assertIn(
                r".\venv\Scripts\Activate",
                content,
                f"{name} does not activate the virtual environment",
            )

    def test_setup_bat_checks_env_file(self):
        """setup.bat must verify .env exists before proceeding."""
        script = SCRIPTS_DIR / "setup.bat"
        content = script.read_text()
        self.assertIn(".env", content)
        self.assertIn("not exist", content.lower())
