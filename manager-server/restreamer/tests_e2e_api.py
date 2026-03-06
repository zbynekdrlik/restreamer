"""
Tests for the E2E Testing API endpoints.

These tests verify:
- E2EActivateReceiving: sets receiving_activated flag
- E2EActivateDelivering: sets delivering_activated flag, queues init_stream
- E2EChunkVerification: returns correct chunk count from DB
- E2EDeactivate: clears both flags
- Error cases: invalid user, missing event, missing params
"""

from unittest.mock import patch

from accounts.models import RestreamerUser
from django.urls import reverse
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APITestCase
from restreamer.models import ChunkRecord, EndPointCfg, StreamingEvent


class E2EActivateReceivingTests(APITestCase):
    """Tests for POST /api/e2e/activate-receiving/"""

    def setUp(self):
        self.user = RestreamerUser.objects.create_user(
            username="e2e_user",
            email="e2e@example.com",
            password="testpass123",
            first_name="E2E",
            last_name="User",
        )
        self.event = StreamingEvent.objects.create(
            user=self.user,
            identifier="e2e-test-event-001",
            short_description="E2E-Test",
            date_of_event=timezone.now(),
            receiving_activated=False,
        )
        self.url = reverse("e2e:activate_receiving")

    @patch("restreamer.views.e2e_api.InstanceManager")
    def test_activate_receiving_sets_flag_and_creates_instance(self, mock_im_class):
        mock_im = mock_im_class.return_value
        mock_im.create_instance.return_value = None

        data = {"user_uuid": str(self.user.api_key), "event_name": "E2E-Test"}
        response = self.client.post(self.url, data, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["status"], "ok")
        self.assertEqual(response.data["event_id"], self.event.id)
        self.assertTrue(response.data["receiving_activated"])
        self.assertTrue(response.data["instance_created"])
        self.assertIsNone(response.data["instance_error"])

        self.event.refresh_from_db()
        self.assertTrue(self.event.receiving_activated)
        mock_im_class.assert_called_once_with(user_id=self.user.id)
        mock_im.create_instance.assert_called_once()

    @patch("restreamer.views.e2e_api.InstanceManager")
    def test_activate_receiving_handles_instance_creation_failure(self, mock_im_class):
        mock_im = mock_im_class.return_value
        mock_im.create_instance.side_effect = Exception("Linode API error")

        data = {"user_uuid": str(self.user.api_key), "event_name": "E2E-Test"}
        response = self.client.post(self.url, data, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["status"], "ok")
        self.assertTrue(response.data["receiving_activated"])
        self.assertFalse(response.data["instance_created"])
        self.assertEqual(response.data["instance_error"], "Linode API error")

        # Flag should still be set even if instance creation fails
        self.event.refresh_from_db()
        self.assertTrue(self.event.receiving_activated)

    def test_activate_receiving_missing_uuid(self):
        response = self.client.post(self.url, {"event_name": "E2E-Test"}, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_activate_receiving_invalid_uuid(self):
        data = {"user_uuid": "00000000-0000-0000-0000-000000000000", "event_name": "E2E-Test"}
        response = self.client.post(self.url, data, format="json")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_activate_receiving_missing_event(self):
        data = {"user_uuid": str(self.user.api_key), "event_name": "Nonexistent"}
        response = self.client.post(self.url, data, format="json")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)


class E2EActivateDeliveringTests(APITestCase):
    """Tests for POST /api/e2e/activate-delivering/"""

    def setUp(self):
        self.user = RestreamerUser.objects.create_user(
            username="e2e_user",
            email="e2e@example.com",
            password="testpass123",
            first_name="E2E",
            last_name="User",
        )
        self.event = StreamingEvent.objects.create(
            user=self.user,
            identifier="e2e-test-event-001",
            short_description="E2E-Test",
            date_of_event=timezone.now(),
            receiving_activated=True,
        )
        self.endpoint = EndPointCfg.objects.create(
            user=self.user,
            alias="Test Endpoint",
            service_type="YT_HLS",
            stream_key="test-key",
            enabled=True,
        )
        self.event.end_points.add(self.endpoint)
        # Create enough chunks
        for i in range(10):
            ChunkRecord.objects.create(
                streaming_event=self.event,
                data_size=1024,
                local_id=i + 1,
                identifier="e2e-test-event-001",
            )
        self.url = reverse("e2e:activate_delivering")

    @patch("restreamer.views.e2e_api.init_stream")
    def test_activate_delivering_sets_flag_and_queues_task(self, mock_init_stream):
        mock_init_stream.delay.return_value = None

        data = {"user_uuid": str(self.user.api_key), "event_name": "E2E-Test"}
        response = self.client.post(self.url, data, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["status"], "ok")
        self.assertTrue(response.data["delivering_activated"])
        self.assertTrue(response.data["init_stream_queued"])
        self.assertEqual(response.data["chunk_count"], 10)

        self.event.refresh_from_db()
        self.assertTrue(self.event.delivering_activated)
        mock_init_stream.delay.assert_called_once()

    def test_activate_delivering_missing_uuid(self):
        response = self.client.post(self.url, {"event_name": "E2E-Test"}, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_activate_delivering_not_enough_chunks(self):
        # Delete all chunks
        ChunkRecord.objects.all().delete()
        data = {"user_uuid": str(self.user.api_key), "event_name": "E2E-Test"}
        response = self.client.post(self.url, data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("Not enough chunks", response.data["error"])

    def test_activate_delivering_no_enabled_endpoint(self):
        self.endpoint.enabled = False
        self.endpoint.save()
        data = {"user_uuid": str(self.user.api_key), "event_name": "E2E-Test"}
        response = self.client.post(self.url, data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("No enabled endpoint", response.data["error"])


class E2EChunkVerificationTests(APITestCase):
    """Tests for GET /api/e2e/chunk-verification/"""

    def setUp(self):
        self.user = RestreamerUser.objects.create_user(
            username="e2e_user",
            email="e2e@example.com",
            password="testpass123",
            first_name="E2E",
            last_name="User",
        )
        self.event = StreamingEvent.objects.create(
            user=self.user,
            identifier="e2e-test-event-001",
            short_description="E2E-Test",
            date_of_event=timezone.now(),
        )
        for i in range(15):
            ChunkRecord.objects.create(
                streaming_event=self.event,
                data_size=1024,
                local_id=i + 1,
                identifier="e2e-test-event-001",
            )
        self.url = reverse("e2e:chunk_verification")

    def test_chunk_verification_returns_correct_count(self):
        response = self.client.get(
            self.url,
            {"user_uuid": str(self.user.api_key), "event_name": "E2E-Test", "min_chunks": 8},
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["chunk_count"], 15)
        self.assertTrue(response.data["has_enough"])
        self.assertEqual(response.data["min_chunks_required"], 8)

    def test_chunk_verification_not_enough(self):
        response = self.client.get(
            self.url,
            {"user_uuid": str(self.user.api_key), "event_name": "E2E-Test", "min_chunks": 100},
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["chunk_count"], 15)
        self.assertFalse(response.data["has_enough"])

    def test_chunk_verification_missing_uuid(self):
        response = self.client.get(self.url, {"event_name": "E2E-Test"})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_chunk_verification_invalid_user(self):
        response = self.client.get(
            self.url,
            {"user_uuid": "00000000-0000-0000-0000-000000000000", "event_name": "E2E-Test"},
        )
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_chunk_verification_missing_event(self):
        response = self.client.get(
            self.url,
            {"user_uuid": str(self.user.api_key), "event_name": "Nonexistent"},
        )
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)


class E2EDeactivateTests(APITestCase):
    """Tests for POST /api/e2e/deactivate/"""

    def setUp(self):
        self.user = RestreamerUser.objects.create_user(
            username="e2e_user",
            email="e2e@example.com",
            password="testpass123",
            first_name="E2E",
            last_name="User",
        )
        self.event = StreamingEvent.objects.create(
            user=self.user,
            identifier="e2e-test-event-001",
            short_description="E2E-Test",
            date_of_event=timezone.now(),
            receiving_activated=True,
            delivering_activated=True,
        )
        self.url = reverse("e2e:deactivate")

    @patch("restreamer.views.e2e_api.InstanceManager")
    def test_deactivate_clears_both_flags(self, mock_im_class):
        mock_im = mock_im_class.return_value
        mock_im.get_my_server_ip.return_value = None

        data = {"user_uuid": str(self.user.api_key), "event_name": "E2E-Test"}
        response = self.client.post(self.url, data, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["status"], "ok")
        self.assertFalse(response.data["receiving_activated"])
        self.assertFalse(response.data["delivering_activated"])

        self.event.refresh_from_db()
        self.assertFalse(self.event.receiving_activated)
        self.assertFalse(self.event.delivering_activated)

    def test_deactivate_missing_uuid(self):
        response = self.client.post(self.url, {"event_name": "E2E-Test"}, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_deactivate_invalid_user(self):
        data = {"user_uuid": "00000000-0000-0000-0000-000000000000", "event_name": "E2E-Test"}
        response = self.client.post(self.url, data, format="json")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_deactivate_missing_event(self):
        data = {"user_uuid": str(self.user.api_key), "event_name": "Nonexistent"}
        response = self.client.post(self.url, data, format="json")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)


class E2EDeliveringStatusTests(APITestCase):
    """Tests for GET /api/e2e/delivering-status/"""

    def setUp(self):
        self.user = RestreamerUser.objects.create_user(
            username="e2e_user",
            email="e2e@example.com",
            password="testpass123",
            first_name="E2E",
            last_name="User",
        )
        self.event = StreamingEvent.objects.create(
            user=self.user,
            identifier="e2e-test-event-001",
            short_description="E2E-Test",
            date_of_event=timezone.now(),
            delivering_activated=False,
        )
        self.url = reverse("e2e:delivering_status")

    @patch("restreamer.views.e2e_api.InstanceManager")
    def test_delivering_status_not_activated_no_instance(self, mock_im_class):
        mock_im = mock_im_class.return_value
        mock_im.get_my_server_ip.return_value = None

        response = self.client.get(
            self.url,
            {"user_uuid": str(self.user.api_key), "event_name": "E2E-Test"},
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertFalse(response.data["delivering_activated"])
        self.assertIsNone(response.data["server_ip"])
        self.assertFalse(response.data["server_ready"])
        self.assertFalse(response.data["endpoints_alive"])
        # Always checks instance status regardless of delivering_activated
        mock_im_class.assert_called_once_with(self.user.id)

    @patch("restreamer.views.e2e_api.InstanceManager")
    @patch("restreamer.views.e2e_api.http_requests.get")
    def test_delivering_status_with_active_server(self, mock_get, mock_im_class):
        self.event.delivering_activated = True
        self.event.save()

        mock_im = mock_im_class.return_value
        mock_im.get_my_server_ip.return_value = "10.0.0.1"

        # First call: readiness check, second call: endpoint-status
        from unittest.mock import MagicMock

        ready_response = MagicMock()
        ready_response.status_code = 200

        ep_response = MagicMock()
        ep_response.status_code = 200
        ep_response.json.return_value = {
            "endpoint_count": 1,
            "endpoints": [{"alias": "Test EP", "alive": True, "pid": 123, "buff_size_mb": 5.0, "current_chunk_id": 42}],
        }
        mock_get.side_effect = [ready_response, ep_response]

        response = self.client.get(
            self.url,
            {"user_uuid": str(self.user.api_key), "event_name": "E2E-Test"},
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data["delivering_activated"])
        self.assertEqual(response.data["server_ip"], "10.0.0.1")
        self.assertTrue(response.data["server_ready"])
        self.assertTrue(response.data["endpoints_alive"])
        self.assertEqual(len(response.data["endpoint_details"]), 1)
        self.assertEqual(response.data["endpoint_details"][0]["alias"], "Test EP")

    def test_delivering_status_missing_uuid(self):
        response = self.client.get(self.url, {"event_name": "E2E-Test"})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)


class E2EYouTubeStreamStatusTests(APITestCase):
    """Tests for GET /api/e2e/youtube-status/"""

    def setUp(self):
        self.user = RestreamerUser.objects.create_user(
            username="e2e_user",
            email="e2e@example.com",
            password="testpass123",
            first_name="E2E",
            last_name="User",
        )
        self.event = StreamingEvent.objects.create(
            user=self.user,
            identifier="e2e-test-event-001",
            short_description="E2E-Test",
            date_of_event=timezone.now(),
        )
        self.url = reverse("e2e:youtube_status")

    @patch("restreamer.views.e2e_api.build_youtube_client")
    def test_youtube_status_no_oauth(self, mock_build):
        mock_build.return_value = None

        response = self.client.get(
            self.url,
            {"user_uuid": str(self.user.api_key), "event_name": "E2E-Test"},
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertFalse(response.data["stream_receiving"])
        self.assertIn("not connected", response.data["error_detail"])

    @patch("restreamer.views.e2e_api.build_youtube_client")
    def test_youtube_status_auth_error(self, mock_build):
        from services.youtube.client import YouTubeAuthError

        mock_build.side_effect = YouTubeAuthError("OAuth refresh failed: invalid_grant")

        response = self.client.get(
            self.url,
            {"user_uuid": str(self.user.api_key), "event_name": "E2E-Test"},
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertFalse(response.data["stream_receiving"])
        self.assertIn("OAuth refresh failed", response.data["error_detail"])

    @patch("restreamer.views.e2e_api.build_youtube_client")
    def test_youtube_status_no_streams(self, mock_build):
        from unittest.mock import MagicMock

        mock_youtube = MagicMock()
        mock_build.return_value = mock_youtube
        mock_youtube.liveStreams.return_value.list.return_value.execute.return_value = {"items": []}
        mock_youtube.liveBroadcasts.return_value.list.return_value.execute.return_value = {"items": []}

        response = self.client.get(
            self.url,
            {"user_uuid": str(self.user.api_key), "event_name": "E2E-Test"},
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertFalse(response.data["stream_receiving"])
        self.assertEqual(response.data["stream_count"], 0)

    @patch("restreamer.views.e2e_api.build_youtube_client")
    def test_youtube_status_stream_active(self, mock_build):
        from unittest.mock import MagicMock

        mock_youtube = MagicMock()
        mock_build.return_value = mock_youtube
        mock_youtube.liveStreams.return_value.list.return_value.execute.return_value = {
            "items": [
                {
                    "id": "stream123",
                    "snippet": {"title": "E2E Test Stream"},
                    "status": {"streamStatus": "active", "healthStatus": {"status": "good"}},
                }
            ]
        }
        mock_youtube.liveBroadcasts.return_value.list.return_value.execute.return_value = {
            "items": [
                {
                    "id": "bc123",
                    "snippet": {"title": "E2E Test Broadcast"},
                    "status": {"lifeCycleStatus": "testing"},
                }
            ]
        }

        response = self.client.get(
            self.url,
            {"user_uuid": str(self.user.api_key), "event_name": "E2E-Test"},
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data["stream_receiving"])
        self.assertEqual(response.data["stream_count"], 1)
        self.assertEqual(response.data["streams"][0]["stream_id"], "stream123")
        self.assertEqual(response.data["streams"][0]["stream_status"], "active")
        self.assertEqual(response.data["broadcast_count"], 1)
        self.assertEqual(response.data["broadcasts"][0]["life_cycle_status"], "testing")

    @patch("restreamer.views.e2e_api.build_youtube_client")
    def test_youtube_status_stream_inactive(self, mock_build):
        from unittest.mock import MagicMock

        mock_youtube = MagicMock()
        mock_build.return_value = mock_youtube
        mock_youtube.liveStreams.return_value.list.return_value.execute.return_value = {
            "items": [
                {
                    "id": "stream123",
                    "snippet": {"title": "E2E Test Stream"},
                    "status": {"streamStatus": "inactive", "healthStatus": {}},
                }
            ]
        }
        mock_youtube.liveBroadcasts.return_value.list.return_value.execute.return_value = {"items": []}

        response = self.client.get(
            self.url,
            {"user_uuid": str(self.user.api_key), "event_name": "E2E-Test"},
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertFalse(response.data["stream_receiving"])
        self.assertEqual(response.data["streams"][0]["stream_status"], "inactive")

    @patch("restreamer.views.e2e_api.build_youtube_client")
    def test_youtube_status_api_error(self, mock_build):
        from unittest.mock import MagicMock

        mock_youtube = MagicMock()
        mock_build.return_value = mock_youtube
        mock_youtube.liveStreams.return_value.list.return_value.execute.side_effect = Exception("API quota exceeded")

        response = self.client.get(
            self.url,
            {"user_uuid": str(self.user.api_key), "event_name": "E2E-Test"},
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertFalse(response.data["stream_receiving"])
        self.assertIn("API error", response.data["error_detail"])

    def test_youtube_status_missing_uuid(self):
        response = self.client.get(self.url, {"event_name": "E2E-Test"})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_youtube_status_invalid_user(self):
        response = self.client.get(
            self.url,
            {"user_uuid": "00000000-0000-0000-0000-000000000000", "event_name": "E2E-Test"},
        )
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_youtube_status_missing_event(self):
        response = self.client.get(
            self.url,
            {"user_uuid": str(self.user.api_key), "event_name": "Nonexistent"},
        )
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
