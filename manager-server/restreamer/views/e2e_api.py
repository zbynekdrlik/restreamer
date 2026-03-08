"""
E2E Testing API endpoints.

These endpoints allow CI/CD pipelines to control streaming events without SSH access.
Authentication is via user_uuid (api_key) parameter, same as GetActiveStream.
"""

import logging
import time

import requests as http_requests
from accounts.models import RestreamerUser
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView
from restreamer.models import ChunkRecord, EndPointCfg, StreamingEvent
from restreamer.tasks import init_stream
from restreamer.views.instances import InstanceManager
from services.youtube.client import YouTubeAuthError, build_youtube_client

log = logging.getLogger(__name__)


class E2EActivateReceiving(APIView):
    """Activate receiving for a streaming event (by short_description).

    Also creates the delivering server Linode instance, mirroring the normal
    SetupStream flow so the instance is booting while chunks arrive.
    """

    def post(self, request):
        user_uuid = request.data.get("user_uuid") or request.query_params.get("user_uuid")
        event_name = request.data.get("event_name", "E2E-Test")

        if not user_uuid:
            return Response({"error": "user_uuid required"}, status=status.HTTP_400_BAD_REQUEST)

        try:
            user = RestreamerUser.objects.get(api_key=user_uuid)
            event = StreamingEvent.objects.filter(user=user, short_description=event_name).first()

            if not event:
                return Response({"error": f"Event {event_name} not found"}, status=status.HTTP_404_NOT_FOUND)

            event.receiving_activated = True
            event.save()

            # Create delivering server instance (mirrors normal SetupStream flow)
            instance_created = False
            instance_error = None
            try:
                InstanceManager(user_id=user.id).create_instance()
                instance_created = True
            except Exception as e:
                instance_error = str(e)
                log.warning(f"Instance creation failed (may already exist): {e}")

            return Response(
                {
                    "status": "ok",
                    "event_id": event.id,
                    "event_name": event.short_description,
                    "receiving_activated": event.receiving_activated,
                    "instance_created": instance_created,
                    "instance_error": instance_error,
                }
            )

        except RestreamerUser.DoesNotExist:
            return Response({"error": "User not found"}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            log.exception("E2E activate receiving failed")
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class E2EActivateDelivering(APIView):
    """Activate delivering and trigger init_stream for a streaming event."""

    def post(self, request):
        user_uuid = request.data.get("user_uuid") or request.query_params.get("user_uuid")
        event_name = request.data.get("event_name", "E2E-Test")

        if not user_uuid:
            return Response({"error": "user_uuid required"}, status=status.HTTP_400_BAD_REQUEST)

        try:
            user = RestreamerUser.objects.get(api_key=user_uuid)
            event = StreamingEvent.objects.filter(user=user, short_description=event_name).first()

            if not event:
                return Response({"error": f"Event {event_name} not found"}, status=status.HTTP_404_NOT_FOUND)

            # Get enabled endpoint
            endpoint = EndPointCfg.objects.filter(streamingevent=event, enabled=True).first()
            if not endpoint:
                return Response({"error": "No enabled endpoint found"}, status=status.HTTP_400_BAD_REQUEST)

            # Check we have enough chunks
            chunk_count = ChunkRecord.objects.filter(streaming_event=event).count()
            if chunk_count < 5:
                return Response(
                    {
                        "error": f"Not enough chunks ({chunk_count}), need at least 5",
                        "chunk_count": chunk_count,
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            # Activate delivering
            event.delivering_activated = True
            event.save()

            # Trigger init_stream task
            init_stream.delay(user.id, event.id, endpoint_id=endpoint.id)

            return Response(
                {
                    "status": "ok",
                    "event_id": event.id,
                    "event_name": event.short_description,
                    "delivering_activated": event.delivering_activated,
                    "endpoint": endpoint.alias,
                    "chunk_count": chunk_count,
                    "init_stream_queued": True,
                }
            )

        except RestreamerUser.DoesNotExist:
            return Response({"error": "User not found"}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            log.exception("E2E activate delivering failed")
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class E2EDeliveringStatus(APIView):
    """Check delivering server status including endpoint process info."""

    def get(self, request):
        user_uuid = request.query_params.get("user_uuid")
        event_name = request.query_params.get("event_name", "E2E-Test")

        if not user_uuid:
            return Response({"error": "user_uuid required"}, status=status.HTTP_400_BAD_REQUEST)

        try:
            user = RestreamerUser.objects.get(api_key=user_uuid)
            event = StreamingEvent.objects.filter(user=user, short_description=event_name).first()

            if not event:
                return Response({"error": f"Event {event_name} not found"}, status=status.HTTP_404_NOT_FOUND)

            # Get delivering server IP (always check, not just when delivering_activated)
            im = InstanceManager(user.id)
            server_ip = im.get_my_server_ip()

            # Check server status
            server_ready = False
            endpoints_alive = False
            endpoint_details = []

            if server_ip:
                # Check Django readiness
                try:
                    resp = http_requests.get(f"http://{server_ip}:8000/api/raceive_init_data/", timeout=5)
                    server_ready = resp.status_code == 200
                except Exception as e:
                    log.debug(f"Delivering server readiness check failed: {e}")

                # Check endpoint process status
                if server_ready:
                    try:
                        resp = http_requests.get(f"http://{server_ip}:8000/api/endpoint-status/", timeout=5)
                        if resp.status_code == 200:
                            ep_data = resp.json()
                            endpoint_details = ep_data.get("endpoints", [])
                            endpoints_alive = ep_data.get("endpoint_count", 0) > 0 and any(
                                ep.get("alive", False) for ep in endpoint_details
                            )
                    except Exception as e:
                        log.debug(f"Endpoint status check failed: {e}")

            return Response(
                {
                    "status": "ok",
                    "event_id": event.id,
                    "delivering_activated": event.delivering_activated,
                    "server_ip": server_ip,
                    "server_ready": server_ready,
                    "endpoints_alive": endpoints_alive,
                    "endpoint_details": endpoint_details,
                }
            )

        except RestreamerUser.DoesNotExist:
            return Response({"error": "User not found"}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            log.exception("E2E delivering status failed")
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class E2EChunkVerification(APIView):
    """Verify chunks exist on manager DB for a streaming event."""

    def get(self, request):
        user_uuid = request.query_params.get("user_uuid")
        event_name = request.query_params.get("event_name", "E2E-Test")
        min_chunks = int(request.query_params.get("min_chunks", 0))

        if not user_uuid:
            return Response({"error": "user_uuid required"}, status=status.HTTP_400_BAD_REQUEST)

        try:
            user = RestreamerUser.objects.get(api_key=user_uuid)
            event = StreamingEvent.objects.filter(user=user, short_description=event_name).first()

            if not event:
                return Response({"error": f"Event {event_name} not found"}, status=status.HTTP_404_NOT_FOUND)

            chunk_count = ChunkRecord.objects.filter(streaming_event=event).count()
            has_enough = chunk_count >= min_chunks

            return Response(
                {
                    "status": "ok",
                    "event_id": event.id,
                    "event_name": event.short_description,
                    "chunk_count": chunk_count,
                    "min_chunks_required": min_chunks,
                    "has_enough": has_enough,
                    "identifier": str(event.identifier),
                }
            )

        except RestreamerUser.DoesNotExist:
            return Response({"error": "User not found"}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            log.exception("E2E chunk verification failed")
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class E2EDeactivate(APIView):
    """Deactivate both receiving and delivering for a streaming event.

    After sending the stop signal to the delivering server, verifies that
    endpoints actually stopped (retry loop, max 30s). Returns cleanup status
    in response so CI can assert on it.
    """

    def post(self, request):
        user_uuid = request.data.get("user_uuid") or request.query_params.get("user_uuid")
        event_name = request.data.get("event_name", "E2E-Test")

        if not user_uuid:
            return Response({"error": "user_uuid required"}, status=status.HTTP_400_BAD_REQUEST)

        try:
            user = RestreamerUser.objects.get(api_key=user_uuid)
            event = StreamingEvent.objects.filter(user=user, short_description=event_name).first()

            if not event:
                return Response({"error": f"Event {event_name} not found"}, status=status.HTTP_404_NOT_FOUND)

            server_ip = None
            endpoints_stopped = None
            endpoint_count_after = None

            # Stop delivering server if active
            if event.delivering_activated:
                try:
                    im = InstanceManager(user.id)
                    server_ip = im.get_my_server_ip()
                    if server_ip:
                        http_requests.post(
                            f"http://{server_ip}:8000/api/end_stream/",
                            json={"alias": None},
                            timeout=5,
                        )
                except Exception as e:
                    log.warning(f"Failed to stop delivering server: {e}")

            # Verify endpoints actually stopped (max 30s)
            if server_ip:
                endpoints_stopped = False
                for attempt in range(6):  # 6 × 5s = 30s max
                    time.sleep(5)
                    try:
                        resp = http_requests.get(
                            f"http://{server_ip}:8000/api/endpoint-status/",
                            timeout=5,
                        )
                        if resp.status_code == 200:
                            ep_data = resp.json()
                            endpoint_count_after = ep_data.get("endpoint_count", 0)
                            if endpoint_count_after == 0:
                                endpoints_stopped = True
                                break
                            log.info(
                                f"Endpoints still running (attempt {attempt + 1}/6): "
                                f"count={endpoint_count_after}"
                            )
                    except Exception:
                        pass  # Server may be slow to respond
                if not endpoints_stopped:
                    log.error(
                        "Delivering server endpoints NOT stopped after deactivation "
                        f"(count={endpoint_count_after})"
                    )

            # Deactivate flags
            event.receiving_activated = False
            event.delivering_activated = False
            event.save()

            return Response(
                {
                    "status": "ok",
                    "event_id": event.id,
                    "event_name": event.short_description,
                    "receiving_activated": event.receiving_activated,
                    "delivering_activated": event.delivering_activated,
                    "endpoints_stopped": endpoints_stopped,
                    "endpoint_count_after": endpoint_count_after,
                }
            )

        except RestreamerUser.DoesNotExist:
            return Response({"error": "User not found"}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            log.exception("E2E deactivate failed")
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class E2EYouTubeStreamStatus(APIView):
    """Check if YouTube is receiving stream data via the YouTube Data API.

    Uses liveStreams.list to check actual stream reception status (streamStatus).
    This is more reliable than checking broadcasts because the stream stays in
    "testing" state (auto-start is banned) — the key signal is whether YouTube's
    ingest server is actually receiving data.

    Stream status values:
      - "active"   → YouTube IS receiving stream data
      - "ready"    → Stream was receiving but stopped
      - "inactive" → No data being received
      - "created"  → Stream created but never received data
      - "error"    → Error state

    Uses server-side OAuth tokens stored in the DB with auto-refresh,
    so no browser sessions or Chrome profiles are needed.
    """

    def get(self, request):
        user_uuid = request.query_params.get("user_uuid")
        event_name = request.query_params.get("event_name", "E2E-Test")

        if not user_uuid:
            return Response({"error": "user_uuid required"}, status=status.HTTP_400_BAD_REQUEST)

        try:
            user = RestreamerUser.objects.get(api_key=user_uuid)
            event = StreamingEvent.objects.filter(user=user, short_description=event_name).first()

            if not event:
                return Response({"error": f"Event {event_name} not found"}, status=status.HTTP_404_NOT_FOUND)

            try:
                youtube = build_youtube_client(user)
            except YouTubeAuthError as e:
                return Response(
                    {
                        "status": "ok",
                        "event_id": event.id,
                        "stream_receiving": False,
                        "error_detail": str(e),
                    }
                )

            if not youtube:
                return Response(
                    {
                        "status": "ok",
                        "event_id": event.id,
                        "stream_receiving": False,
                        "error_detail": "YouTube not connected (no OAuth credentials)",
                    }
                )

            # Check liveStreams for actual stream reception status.
            # This tells us whether YouTube's ingest server is receiving data.
            stream_receiving = False
            stream_details = []
            try:
                streams_response = youtube.liveStreams().list(part="id,snippet,status", mine=True).execute()
                for stream in streams_response.get("items", []):
                    stream_status = stream.get("status", {}).get("streamStatus", "unknown")
                    health_status = stream.get("status", {}).get("healthStatus", {})
                    title = stream.get("snippet", {}).get("title", "")
                    stream_details.append(
                        {
                            "stream_id": stream["id"],
                            "title": title,
                            "stream_status": stream_status,
                            "health_status": health_status,
                        }
                    )
                    if stream_status == "active":
                        stream_receiving = True
            except Exception as e:
                log.exception("YouTube liveStreams API query failed")
                return Response(
                    {
                        "status": "ok",
                        "event_id": event.id,
                        "stream_receiving": False,
                        "error_detail": f"YouTube liveStreams API error: {e}",
                    }
                )

            # Also check broadcasts for lifecycle context
            broadcast_info = []
            try:
                broadcasts_response = (
                    youtube.liveBroadcasts()
                    .list(part="id,snippet,status", broadcastStatus="all", broadcastType="all")
                    .execute()
                )
                for bc in broadcasts_response.get("items", []):
                    lcs = bc.get("status", {}).get("lifeCycleStatus", "unknown")
                    # Only include testing/live/ready broadcasts (skip completed)
                    if lcs in ("testing", "live", "ready", "liveStarting"):
                        broadcast_info.append(
                            {
                                "broadcast_id": bc["id"],
                                "title": bc.get("snippet", {}).get("title", ""),
                                "life_cycle_status": lcs,
                            }
                        )
            except Exception as e:
                log.warning(f"YouTube liveBroadcasts query failed (non-critical): {e}")

            return Response(
                {
                    "status": "ok",
                    "event_id": event.id,
                    "stream_receiving": stream_receiving,
                    "stream_count": len(stream_details),
                    "streams": stream_details,
                    "broadcast_count": len(broadcast_info),
                    "broadcasts": broadcast_info,
                }
            )

        except RestreamerUser.DoesNotExist:
            return Response({"error": "User not found"}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            log.exception("E2E YouTube stream status failed")
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
