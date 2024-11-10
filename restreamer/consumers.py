from channels.generic.websocket import AsyncWebsocketConsumer
import json

class BufferHealthConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        await self.channel_layer.group_add(
            "buffer_health",
            self.channel_name
        )
        await self.accept()

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard(
            "buffer_health",
            self.channel_name
        )

    async def receive(self, text_data):
        data = json.loads(text_data)
        await self.channel_layer.group_send(
            "buffer_health",
            {
                "type": "buffer_health_update",
                "message": data
            }
        )

    async def buffer_health_update(self, event):
        message = event["message"]
        await self.send(text_data=json.dumps({
            "message": message
        }))

class StreamStatusConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        self.user_id = self.scope["url_route"]["kwargs"]["user_id"]
        self.group_name = f"user_{self.user_id}"
        
        # Join user-specific group
        await self.channel_layer.group_add(
            self.group_name,
            self.channel_name
        )

        await self.accept()

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard(
            self.group_name,
            self.channel_name
        )

    # Receive message from the view and send it to WebSocket
    async def stream_update(self, event):
        await self.send(text_data=json.dumps({
            'message': event['message'],
            'event_id': event['event_id']
        }))