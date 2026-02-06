import os
import logging
from typing import Optional
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

logger = logging.getLogger(__name__)


class SlackNotifier:
    
    def __init__(self, token: str, default_channel: str = "#traffic-cameras"):
        self.client = WebClient(token=token)
        self.default_channel = default_channel
        logger.info(f"Slack notifier initialized for channel: {default_channel}")
    
    def send_message(self, message: str, channel: Optional[str] = None) -> bool:
        target_channel = channel or self.default_channel
        try:
            response = self.client.chat_postMessage(
                channel=target_channel,
                text=message
            )
            logger.info(f"Message sent to {target_channel}")
            return True
        except SlackApiError as e:
            logger.error(f"Failed to send message: {e.response['error']}")
            return False
    
    def send_image(self, image_path: str, title: str, comment: str = "",
                   channel: Optional[str] = None) -> bool:
        target_channel = channel or self.default_channel
        try:
            if not os.path.exists(image_path):
                logger.error(f"Image file not found: {image_path}")
                return False
            
            # Use files_upload_v2 with correct parameters
            response = self.client.files_upload_v2(
                channels=[target_channel],  # Must be a list for v2
                file=image_path,
                title=title,
                initial_comment=comment
            )
            logger.info(f"Image uploaded to {target_channel}: {title}")
            return True
        except SlackApiError as e:
            logger.error(f"Failed to upload image: {e.response['error']}")
            return False
    
    def send_camera_alert(self, camera_data: dict, image_path: Optional[str] = None,
                          channel: Optional[str] = None) -> bool:
        target_channel = channel or self.default_channel
        
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"📷 {camera_data.get('name', 'Unknown Camera')}",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Roadway:*\n{camera_data.get('roadway_name', 'N/A')}"},
                    {"type": "mrkdwn", "text": f"*Direction:*\n{camera_data.get('direction_of_travel', 'N/A')}"},
                    {"type": "mrkdwn", "text": f"*Latitude:*\n{camera_data.get('latitude', 'N/A')}"},
                    {"type": "mrkdwn", "text": f"*Longitude:*\n{camera_data.get('longitude', 'N/A')}"}
                ]
            },
            {
                "type": "context",
                "elements": [
                    {"type": "mrkdwn", "text": f"Camera ID: `{camera_data.get('camera_id', 'N/A')}`"}
                ]
            }
        ]
        
        try:
            self.client.chat_postMessage(
                channel=target_channel,
                blocks=blocks,
                text=f"Camera update: {camera_data.get('name', 'Unknown')}"
            )
            
            if image_path and os.path.exists(image_path):
                self.send_image(image_path, camera_data.get('name', 'Camera Image'),
                              channel=target_channel)
            
            return True
        except SlackApiError as e:
            logger.error(f"Failed to send camera alert: {e.response['error']}")
            return False
