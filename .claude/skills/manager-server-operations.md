---
name: manager-server-operations
description: Operations guide for restreamer.newlevel.media manager server - SSH, Django admin, streaming events, delivering servers
---

# Manager Server Operations Guide

This skill documents ALL operations for the manager server. **USE THIS SKILL** instead of asking the user for information.

## Quick Reference

| Service         | URL/Host                                            | Credentials                            |
| --------------- | --------------------------------------------------- | -------------------------------------- |
| SSH             | `172.105.95.118:22`                                 | user: `root`, pass: `lm-wC\0d..1)87oQ` |
| Django Admin    | `https://restreamer.newlevel.media/admin/`          | (browser login)                        |
| Django App      | `/root/kristian/manager-server/restreamer-manager/` |                                        |
| Virtualenv      | `/root/.virtualenvs/venv/`                          |                                        |
| Process Manager | tmux session `restreamer`                           |                                        |

## SSH Access

```bash
# Execute command
sshpass -p 'lm-wC\0d..1)87oQ' ssh root@172.105.95.118 "command here"

# Django management command
sshpass -p 'lm-wC\0d..1)87oQ' ssh root@172.105.95.118 "cd /root/kristian/manager-server/restreamer-manager && /root/.virtualenvs/venv/bin/python manage.py shell -c \"
# Python code here
\""
```

## Tmux Session

```bash
# List windows
sshpass -p 'lm-wC\0d..1)87oQ' ssh root@172.105.95.118 "tmux list-windows -t restreamer"

# Windows:
# 0: sh
# 1: gunicorn (main web server)
# 2: celery_worker
# 3: init_stream_worker (handles stream initialization)
# 4: node- (frontend)
# 5: bash

# View window output
sshpass -p 'lm-wC\0d..1)87oQ' ssh root@172.105.95.118 "tmux capture-pane -t restreamer:3 -p | tail -30"
```

## Streaming Events

### List All Events

```bash
sshpass -p 'lm-wC\0d..1)87oQ' ssh root@172.105.95.118 "cd /root/kristian/manager-server/restreamer-manager && /root/.virtualenvs/venv/bin/python manage.py shell -c \"
from restreamer.models import StreamingEvent
for se in StreamingEvent.objects.all()[:10]:
    print(f'ID {se.id}: {se.short_description}, identifier={se.identifier}')
    print(f'  receiving={se.receiving_activated}, delivering={se.delivering_activated}')
\""
```

### Key Streaming Events

| ID  | Name       | Identifier                             | Purpose            |
| --- | ---------- | -------------------------------------- | ------------------ |
| 15  | SNV-stream | `e67fa0d3-29c3-41a1-8834-92df6a05d270` | Church main stream |
| 35  | PP-live    | `53318047-d09b-415d-894c-c414042103f6` | Poprad live        |
| 44  | test       | `04262bdc-9216-4c7c-a915-1026b4150bc9` | Testing            |

### Enable/Disable Streaming

```bash
# Enable receiving and delivering for event 15
sshpass -p 'lm-wC\0d..1)87oQ' ssh root@172.105.95.118 "cd /root/kristian/manager-server/restreamer-manager && /root/.virtualenvs/venv/bin/python manage.py shell -c \"
from restreamer.models import StreamingEvent
se = StreamingEvent.objects.get(id=15)
se.receiving_activated = True
se.delivering_activated = True
se.save()
print(f'Updated: receiving={se.receiving_activated}, delivering={se.delivering_activated}')
\""
```

## Chunk Records

### Check Recent Chunks

```bash
sshpass -p 'lm-wC\0d..1)87oQ' ssh root@172.105.95.118 "cd /root/kristian/manager-server/restreamer-manager && /root/.virtualenvs/venv/bin/python manage.py shell -c \"
from restreamer.models import ChunkRecord, StreamingEvent
from django.utils import timezone
from datetime import timedelta
from django.db.models import Max, Count

# Events with recent chunks
events = StreamingEvent.objects.annotate(
    chunk_count=Count('chunks'),
    last_chunk=Max('chunks__created_at')
).filter(chunk_count__gt=0).order_by('-last_chunk')[:5]

for se in events:
    print(f'{se.short_description}: {se.chunk_count} chunks, last: {se.last_chunk}')
\""
```

### Get Next Chunk ID (what delivering service sees)

```bash
curl -s 'https://restreamer.newlevel.media/api/get-next-chunk/?current_local_id=0&stream_identifier=e67fa0d3-29c3-41a1-8834-92df6a05d270'
```

## Endpoints (YouTube/Facebook/Vimeo)

### List Endpoints for Event

```bash
sshpass -p 'lm-wC\0d..1)87oQ' ssh root@172.105.95.118 "cd /root/kristian/manager-server/restreamer-manager && /root/.virtualenvs/venv/bin/python manage.py shell -c \"
from restreamer.models import StreamingEvent, EndPointCfg
se = StreamingEvent.objects.get(id=15)
for ep in se.end_points.all():
    print(f'ID {ep.id}: {ep.alias} ({ep.service_type})')
    print(f'  enabled={ep.enabled}, stream_key={ep.stream_key[:20]}...')
\""
```

### Key Endpoints

| ID  | Alias       | Service Type | Event           |
| --- | ----------- | ------------ | --------------- |
| 22  | YT KS-BB 4K | YT_RTMP      | 15 (SNV-stream) |

## Delivering Servers (Linode)

### List Running Instances

```bash
sshpass -p 'lm-wC\0d..1)87oQ' ssh root@172.105.95.118 "cd /root/kristian/manager-server/restreamer-manager && /root/.virtualenvs/venv/bin/python manage.py shell -c \"
from django.conf import settings
for linode in settings.LINODE_CLIENT.linode.instances():
    print(f'{linode.label}: {linode.ipv4[0]} ({linode.status})')
\""
```

### Create Delivering Server for User

```bash
sshpass -p 'lm-wC\0d..1)87oQ' ssh root@172.105.95.118 "cd /root/kristian/manager-server/restreamer-manager && /root/.virtualenvs/venv/bin/python manage.py shell -c \"
from restreamer.views.instances import InstanceManager
im = InstanceManager(11)  # user_id
result = im.create_instance()
print(f'Created: {result}')
\""
```

### Check Delivering Server Status

```bash
sshpass -p 'lm-wC\0d..1)87oQ' ssh root@172.105.95.118 "cd /root/kristian/manager-server/restreamer-manager && /root/.virtualenvs/venv/bin/python manage.py shell -c \"
from restreamer.views.instances import InstanceManager
im = InstanceManager(11)  # user_id
print(f'Status: {im.check_status()}')
print(f'IP: {im.get_my_server_ip()}')
\""
```

## Initialize Stream (Start Delivering)

### Trigger init_stream Task

```bash
sshpass -p 'lm-wC\0d..1)87oQ' ssh root@172.105.95.118 "cd /root/kristian/manager-server/restreamer-manager && /root/.virtualenvs/venv/bin/python manage.py shell -c \"
from restreamer.tasks import init_stream
# init_stream(user_id, streaming_event_id, endpoint_id=endpoint_id)
result = init_stream.delay(11, 15, endpoint_id=22)
print(f'Task ID: {result.id}')
\""
```

### Check Delivering Server Django is Ready

```bash
sshpass -p 'lm-wC\0d..1)87oQ' ssh root@172.105.95.118 "curl -s http://DELIVERING_SERVER_IP:8000/api/raceive_init_data/"
# Should return: {"status":"ready","message":"Django is ready to serve responses."}
```

## Users

### List Users

```bash
sshpass -p 'lm-wC\0d..1)87oQ' ssh root@172.105.95.118 "cd /root/kristian/manager-server/restreamer-manager && /root/.virtualenvs/venv/bin/python manage.py shell -c \"
from accounts.models import RestreamerUser
for u in RestreamerUser.objects.all():
    print(f'ID {u.id}: {u.username}')
\""
```

### Key Users

| ID  | Username    | Purpose               |
| --- | ----------- | --------------------- |
| 11  | SNV-stream  | Church streaming user |
| 26  | PP-stream_2 | Poprad streaming      |

## Full Flow Verification

### Step 1: Check Chunks Coming In

```bash
sshpass -p 'lm-wC\0d..1)87oQ' ssh root@172.105.95.118 "cd /root/kristian/manager-server/restreamer-manager && /root/.virtualenvs/venv/bin/python manage.py shell -c \"
from restreamer.models import ChunkRecord
from django.utils import timezone
from datetime import timedelta

recent = timezone.now() - timedelta(minutes=5)
chunks = ChunkRecord.objects.filter(created_at__gte=recent)
print(f'Chunks in last 5 min: {chunks.count()}')
if chunks.exists():
    latest = chunks.order_by('-created_at').first()
    print(f'Latest: local_id={latest.local_id}, created={latest.created_at}')
\""
```

### Step 2: Enable Delivering

```bash
sshpass -p 'lm-wC\0d..1)87oQ' ssh root@172.105.95.118 "cd /root/kristian/manager-server/restreamer-manager && /root/.virtualenvs/venv/bin/python manage.py shell -c \"
from restreamer.models import StreamingEvent
se = StreamingEvent.objects.get(id=15)
se.receiving_activated = True
se.delivering_activated = True
se.save()
print('Enabled')
\""
```

### Step 3: Create/Check Delivering Server

```bash
sshpass -p 'lm-wC\0d..1)87oQ' ssh root@172.105.95.118 "cd /root/kristian/manager-server/restreamer-manager && /root/.virtualenvs/venv/bin/python manage.py shell -c \"
from restreamer.views.instances import InstanceManager
im = InstanceManager(11)
if im.get_instance() is None:
    print('Creating instance...')
    im.create_instance()
else:
    print(f'Instance exists: {im.get_my_server_ip()} ({im.check_status()})')
\""
```

### Step 4: Initialize Stream

```bash
sshpass -p 'lm-wC\0d..1)87oQ' ssh root@172.105.95.118 "cd /root/kristian/manager-server/restreamer-manager && /root/.virtualenvs/venv/bin/python manage.py shell -c \"
from restreamer.tasks import init_stream
result = init_stream.delay(11, 15, endpoint_id=22)
print(f'Task: {result.id}')
\""
```

### Step 5: Watch Task Log

```bash
sshpass -p 'lm-wC\0d..1)87oQ' ssh root@172.105.95.118 "tmux capture-pane -t restreamer:3 -p | tail -20"
```

## Troubleshooting

### No Chunks Arriving

1. Check stream.lan is receiving RTMP and uploading
2. Check S3 credentials on stream.lan
3. Check manager API is accessible from stream.lan

### Delivering Server Not Found

1. Linode instance naming: `delivering-server-{user_id}`
2. Create instance if doesn't exist
3. Wait 1-2 minutes for boot and Django start

### init_stream Task Fails

1. Check correct user_id (must match streaming event's user)
2. Check delivering server is running and Django is ready
3. Check endpoint_id is valid for the event
