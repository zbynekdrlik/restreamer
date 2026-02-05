import logging

import requests

log = logging.getLogger(__name__)

DISCORD_API_URL = "https://discord.com/api"


def send_discord_bot_message(token: str, channel_id: str, content: str):
    """
    Send a message to a specific channel as a Discord bot using
    direct REST API calls.
    :param token: The Discord Bot Token
    :param channel_id: The Discord channel ID
    :param content: The message text
    """
    url = f"{DISCORD_API_URL}/channels/{channel_id}/messages"
    headers = {"Authorization": f"Bot {token}", "Content-Type": "application/json"}
    json_data = {"content": content}
    response = requests.post(url, headers=headers, json=json_data)

    if response.status_code != 200 and response.status_code != 201:
        # In practice, Discord often responds with 200 (OK) or 201 (Created).
        # Sometimes 204 is possible.
        # If there's an error, handle it accordingly:
        log.error(f"Error sending message: {response.status_code} {response.text}")
    else:
        log.info(f"Message successfully sent to channel {channel_id}!")
