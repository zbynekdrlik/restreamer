"""
Tests for the Restreamer Manager Server API endpoints.

These tests verify the API contracts that the Rust local client depends on:
- POST /chunk-upload/ - ChunkUploadView
- POST /api/check-chunk/ - ChunkExistsView
- GET /api/get_active_stream/ - GetActiveStream
- GET /api/get-next-chunk/ - GetNextChunkIdView
"""

from django.test import TestCase
from django.urls import reverse
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APITestCase

from accounts.models import RestreamerUser
from restreamer.models import ChunkRecord, StreamingEvent


class ChunkUploadViewTests(APITestCase):
    """Tests for POST /chunk-upload/ endpoint."""

    def setUp(self):
        """Create test user and streaming event."""
        self.user = RestreamerUser.objects.create_user(
            username="testuser",
            email="test@example.com",
            password="testpass123",
            first_name="Test",
            last_name="User",
        )
        self.streaming_event = StreamingEvent.objects.create(
            user=self.user,
            identifier="test-event-001",
            short_description="Test Event",
            date_of_event=timezone.now(),
            receiving_activated=True,
        )
        self.url = reverse("receive-local-chunks")

    def test_chunk_upload_creates_record(self):
        """Test that chunk upload creates a ChunkRecord with correct fields."""
        data = {
            "chunk_id": 1,
            "chunk_identifier": "test-event-001",
            "chunk_size": 1024,
        }
        response = self.client.post(self.url, data, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(ChunkRecord.objects.count(), 1)

        chunk = ChunkRecord.objects.first()
        self.assertEqual(chunk.local_id, 1)
        self.assertEqual(chunk.identifier, "test-event-001")
        self.assertEqual(chunk.data_size, 1024)
        self.assertEqual(chunk.streaming_event, self.streaming_event)

    def test_chunk_upload_rejects_invalid_event(self):
        """Test that chunk upload returns 400 for invalid streaming event identifier."""
        data = {
            "chunk_id": 1,
            "chunk_identifier": "nonexistent-event",
            "chunk_size": 1024,
        }
        response = self.client.post(self.url, data, format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(ChunkRecord.objects.count(), 0)

    def test_chunk_upload_rejects_missing_fields(self):
        """Test that chunk upload returns 400 for missing required fields."""
        # Missing chunk_id
        data = {
            "chunk_identifier": "test-event-001",
            "chunk_size": 1024,
        }
        response = self.client.post(self.url, data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        # Missing chunk_identifier
        data = {
            "chunk_id": 1,
            "chunk_size": 1024,
        }
        response = self.client.post(self.url, data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        # Missing chunk_size
        data = {
            "chunk_id": 1,
            "chunk_identifier": "test-event-001",
        }
        response = self.client.post(self.url, data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_chunk_upload_handles_duplicate_gracefully(self):
        """Test that uploading the same chunk twice doesn't crash (IntegrityError handled)."""
        data = {
            "chunk_id": 1,
            "chunk_identifier": "test-event-001",
            "chunk_size": 1024,
        }
        # First upload
        response1 = self.client.post(self.url, data, format="json")
        self.assertEqual(response1.status_code, status.HTTP_200_OK)

        # Second upload with same chunk_id - should be handled gracefully
        response2 = self.client.post(self.url, data, format="json")
        self.assertEqual(response2.status_code, status.HTTP_200_OK)

    def test_chunk_upload_multiple_chunks_sequential(self):
        """Test uploading multiple chunks in sequence."""
        for i in range(1, 6):
            data = {
                "chunk_id": i,
                "chunk_identifier": "test-event-001",
                "chunk_size": 1024 * i,
            }
            response = self.client.post(self.url, data, format="json")
            self.assertEqual(response.status_code, status.HTTP_200_OK)

        self.assertEqual(ChunkRecord.objects.count(), 5)
        chunks = ChunkRecord.objects.order_by("local_id")
        for i, chunk in enumerate(chunks, start=1):
            self.assertEqual(chunk.local_id, i)
            self.assertEqual(chunk.data_size, 1024 * i)


class ChunkExistsViewTests(APITestCase):
    """Tests for POST /api/check-chunk/ endpoint."""

    def setUp(self):
        """Create test user, streaming event, and chunk."""
        self.user = RestreamerUser.objects.create_user(
            username="testuser",
            email="test@example.com",
            password="testpass123",
            first_name="Test",
            last_name="User",
        )
        self.streaming_event = StreamingEvent.objects.create(
            user=self.user,
            identifier="test-event-001",
            short_description="Test Event",
            date_of_event=timezone.now(),
            receiving_activated=True,
        )
        self.chunk = ChunkRecord.objects.create(
            streaming_event=self.streaming_event,
            data_size=1024,
            local_id=1,
            identifier="test-event-001",
        )
        self.url = reverse("check_chunk")

    def test_check_chunk_returns_true_when_exists(self):
        """Test that check-chunk returns chunk_exists: true for existing chunk."""
        data = {
            "se_identifier": "test-event-001",
            "chunk_id": 1,
        }
        response = self.client.post(self.url, data, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["chunk_exists"], True)

    def test_check_chunk_returns_false_when_not_exists(self):
        """Test that check-chunk returns chunk_exists: false for non-existent chunk."""
        data = {
            "se_identifier": "test-event-001",
            "chunk_id": 999,
        }
        response = self.client.post(self.url, data, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["chunk_exists"], False)

    def test_check_chunk_returns_false_for_wrong_identifier(self):
        """Test that check-chunk returns false for wrong stream identifier."""
        data = {
            "se_identifier": "wrong-event",
            "chunk_id": 1,
        }
        response = self.client.post(self.url, data, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["chunk_exists"], False)

    def test_check_chunk_rejects_missing_data(self):
        """Test that check-chunk returns 400 for missing required fields."""
        # Missing both
        response = self.client.post(self.url, {}, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)


class GetActiveStreamTests(APITestCase):
    """Tests for GET /api/get_active_stream/ endpoint."""

    def setUp(self):
        """Create test user and streaming event."""
        self.user = RestreamerUser.objects.create_user(
            username="testuser",
            email="test@example.com",
            password="testpass123",
            first_name="Test",
            last_name="User",
        )
        self.streaming_event = StreamingEvent.objects.create(
            user=self.user,
            identifier="test-event-001",
            short_description="Test Event",
            date_of_event=timezone.now(),
            receiving_activated=True,
        )
        self.url = reverse("get_active_stream")

    def test_get_active_stream_returns_event_when_activated(self):
        """Test that get_active_stream returns event when receiving_activated is True."""
        response = self.client.get(self.url, {"user_uuid": str(self.user.api_key)})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["identifier"], "test-event-001")
        self.assertEqual(response.data["short_description"], "Test Event")

    def test_get_active_stream_returns_403_when_not_activated(self):
        """Test that get_active_stream returns 403 when receiving_activated is False."""
        self.streaming_event.receiving_activated = False
        self.streaming_event.save()

        response = self.client.get(self.url, {"user_uuid": str(self.user.api_key)})

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_get_active_stream_returns_404_for_invalid_user(self):
        """Test that get_active_stream returns 404 for non-existent user."""
        response = self.client.get(
            self.url, {"user_uuid": "00000000-0000-0000-0000-000000000000"}
        )

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_get_active_stream_returns_400_without_user_uuid(self):
        """Test that get_active_stream returns 400 when user_uuid is missing."""
        response = self.client.get(self.url)

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)


class GetNextChunkIdViewTests(APITestCase):
    """Tests for GET /api/get-next-chunk/ endpoint."""

    def setUp(self):
        """Create test user, streaming event, and multiple chunks."""
        self.user = RestreamerUser.objects.create_user(
            username="testuser",
            email="test@example.com",
            password="testpass123",
            first_name="Test",
            last_name="User",
        )
        self.streaming_event = StreamingEvent.objects.create(
            user=self.user,
            identifier="test-event-001",
            short_description="Test Event",
            date_of_event=timezone.now(),
            receiving_activated=True,
        )
        # Create chunks 1, 2, 3, 5, 10 (with gaps)
        for chunk_id in [1, 2, 3, 5, 10]:
            ChunkRecord.objects.create(
                streaming_event=self.streaming_event,
                data_size=1024,
                local_id=chunk_id,
                identifier="test-event-001",
            )
        self.url = reverse("get_next_chunk")

    def test_get_next_chunk_returns_sequential_id(self):
        """Test that get_next_chunk returns the next sequential chunk ID."""
        response = self.client.get(
            self.url,
            {"current_local_id": "1", "stream_identifier": "test-event-001"},
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["next_chunk_id"], 2)

    def test_get_next_chunk_skips_gaps(self):
        """Test that get_next_chunk returns next available chunk, skipping gaps."""
        response = self.client.get(
            self.url,
            {"current_local_id": "3", "stream_identifier": "test-event-001"},
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["next_chunk_id"], 5)

    def test_get_next_chunk_returns_404_when_no_more_chunks(self):
        """Test that get_next_chunk returns 404 when no more chunks available."""
        response = self.client.get(
            self.url,
            {"current_local_id": "10", "stream_identifier": "test-event-001"},
        )

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_get_next_chunk_returns_400_for_missing_params(self):
        """Test that get_next_chunk returns 400 for missing parameters."""
        # Missing current_local_id
        response = self.client.get(
            self.url,
            {"stream_identifier": "test-event-001"},
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        # Missing stream_identifier
        response = self.client.get(
            self.url,
            {"current_local_id": "1"},
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_get_next_chunk_returns_400_for_invalid_chunk_id(self):
        """Test that get_next_chunk returns 400 for non-integer chunk_id."""
        response = self.client.get(
            self.url,
            {"current_local_id": "not-a-number", "stream_identifier": "test-event-001"},
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_get_next_chunk_returns_404_for_wrong_identifier(self):
        """Test that get_next_chunk returns 404 for non-existent stream identifier."""
        response = self.client.get(
            self.url,
            {"current_local_id": "1", "stream_identifier": "wrong-event"},
        )

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)


class StreamingEventModelTests(TestCase):
    """Tests for StreamingEvent model methods."""

    def setUp(self):
        """Create test user and streaming event."""
        self.user = RestreamerUser.objects.create_user(
            username="testuser",
            email="test@example.com",
            password="testpass123",
            first_name="Test",
            last_name="User",
        )
        self.streaming_event = StreamingEvent.objects.create(
            user=self.user,
            identifier="test-event-001",
            short_description="Test Event",
            date_of_event=timezone.now(),
        )

    def test_streaming_event_str(self):
        """Test StreamingEvent __str__ returns short_description."""
        self.assertEqual(str(self.streaming_event), "Test Event")

    def test_streaming_event_identifier_is_unique(self):
        """Test that StreamingEvent identifier must be unique."""
        from django.db import IntegrityError

        with self.assertRaises(IntegrityError):
            StreamingEvent.objects.create(
                user=self.user,
                identifier="test-event-001",  # Same identifier
                short_description="Another Event",
                date_of_event=timezone.now(),
            )


class ChunkRecordModelTests(TestCase):
    """Tests for ChunkRecord model."""

    def setUp(self):
        """Create test user and streaming event."""
        self.user = RestreamerUser.objects.create_user(
            username="testuser",
            email="test@example.com",
            password="testpass123",
            first_name="Test",
            last_name="User",
        )
        self.streaming_event = StreamingEvent.objects.create(
            user=self.user,
            identifier="test-event-001",
            short_description="Test Event",
            date_of_event=timezone.now(),
        )

    def test_chunk_record_unique_constraint(self):
        """Test that ChunkRecord has unique constraint on local_id + identifier + streaming_event."""
        from django.db import IntegrityError

        ChunkRecord.objects.create(
            streaming_event=self.streaming_event,
            data_size=1024,
            local_id=1,
            identifier="test-event-001",
        )

        with self.assertRaises(IntegrityError):
            ChunkRecord.objects.create(
                streaming_event=self.streaming_event,
                data_size=2048,
                local_id=1,  # Same local_id
                identifier="test-event-001",  # Same identifier
            )

    def test_chunk_record_cascade_delete(self):
        """Test that ChunkRecords are deleted when StreamingEvent is deleted."""
        ChunkRecord.objects.create(
            streaming_event=self.streaming_event,
            data_size=1024,
            local_id=1,
            identifier="test-event-001",
        )
        self.assertEqual(ChunkRecord.objects.count(), 1)

        self.streaming_event.delete()
        self.assertEqual(ChunkRecord.objects.count(), 0)
