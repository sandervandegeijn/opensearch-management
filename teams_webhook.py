import requests
import json
import time
from typing import Optional, Dict, Any
from loguru import logger
from enum import Enum


class SeverityLevel(Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


class TeamsWebhook:
    def __init__(self, webhook_url: str, max_retries: int = 3, retry_delay: float = 1.0):
        self.webhook_url = webhook_url
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.rate_limit_delay = 0.25  # 250ms between requests to stay under 4/second

    def send_alert(self, title: str, message: str, severity: SeverityLevel) -> bool:
        """Send an alert message as Adaptive Card to Teams via Power Automate"""
        
        # Severity color and emoji mapping
        color_map = {
            SeverityLevel.LOW: "Good",      # Green theme
            SeverityLevel.MEDIUM: "Warning", # Yellow theme  
            SeverityLevel.HIGH: "Attention"  # Red theme
        }
        
        emoji_map = {
            SeverityLevel.LOW: "🟢",
            SeverityLevel.MEDIUM: "🟡", 
            SeverityLevel.HIGH: "🔴"
        }
        
        # Adaptive Card payload for Teams
        payload = {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": {
                        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                        "type": "AdaptiveCard",
                        "version": "1.0",
                        "body": [
                            {
                                "type": "TextBlock",
                                "text": f"{emoji_map.get(severity, '⚪')} {title}",
                                "weight": "Bolder",
                                "size": "Medium"
                            },
                            {
                                "type": "TextBlock", 
                                "text": message,
                                "wrap": True
                            },
                            {
                                "type": "FactSet",
                                "facts": [
                                    {
                                        "title": "Severity:",
                                        "value": severity.value
                                    },
                                    {
                                        "title": "Time:",
                                        "value": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
                                    }
                                ]
                            }
                        ]
                    }
                }
            ]
        }
        
        return self._send_with_retry(payload)
    
    def send_simple_message(self, message: str) -> bool:
        """Send a simple message as Adaptive Card to Teams via Power Automate"""
        
        # Adaptive Card payload for simple messages
        payload = {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": {
                        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                        "type": "AdaptiveCard",
                        "version": "1.0",
                        "body": [
                            {
                                "type": "TextBlock",
                                "text": "📊 OpenSearch Health Monitor",
                                "weight": "Bolder",
                                "size": "Medium"
                            },
                            {
                                "type": "TextBlock",
                                "text": message,
                                "wrap": True
                            },
                            {
                                "type": "FactSet",
                                "facts": [
                                    {
                                        "title": "Time:",
                                        "value": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
                                    }
                                ]
                            }
                        ]
                    }
                }
            ]
        }
        
        return self._send_with_retry(payload)
    
    def _send_with_retry(self, payload: Dict[str, Any]) -> bool:
        """Send payload with retry logic and rate limiting"""
        
        for attempt in range(self.max_retries):
            try:
                # Rate limiting - wait between requests
                if attempt > 0:
                    time.sleep(self.retry_delay * (2 ** (attempt - 1)))  # Exponential backoff
                else:
                    time.sleep(self.rate_limit_delay)
                
                response = requests.post(
                    self.webhook_url,
                    headers={"Content-Type": "application/json"},
                    data=json.dumps(payload),
                    timeout=10
                )
                
                if response.status_code in [200, 202]:
                    logger.info(f"Teams webhook message sent successfully (status: {response.status_code})")
                    return True
                elif response.status_code == 429:
                    # Rate limited - wait longer before retry
                    logger.warning(f"Teams webhook rate limited, attempt {attempt + 1}/{self.max_retries}")
                    time.sleep(self.retry_delay * 2)
                    continue
                else:
                    logger.error(f"Teams webhook failed with status {response.status_code}: {response.text}")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Teams webhook request failed (attempt {attempt + 1}/{self.max_retries}): {str(e)}")
                
            if attempt < self.max_retries - 1:
                continue
        
        logger.error(f"Failed to send Teams webhook after {self.max_retries} attempts")
        return False
    
    def test_connection(self) -> bool:
        """Test the webhook connection with a simple message"""
        return self.send_simple_message("🔍 Connection test successful! Health monitoring is ready.")