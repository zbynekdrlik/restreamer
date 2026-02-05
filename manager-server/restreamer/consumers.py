"""class BufferHealthConsumer(AsyncWebsocketConsumer):
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
}))"""
