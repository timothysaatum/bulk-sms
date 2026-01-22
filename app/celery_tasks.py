"""
Celery configuration and background tasks
Handles bulk SMS sending with retry logic and error handling
"""

from celery import Celery, Task
from celery.utils.log import get_task_logger
from app.config import settings
from app.database import SessionLocal
from app.models.campaign import Campaign, Contact, Message, CampaignStatus, MessageStatus
from sqlalchemy.orm import Session
from typing import List, Dict, Any
import httpx
import time
from datetime import datetime, timezone
import urllib.parse

logger = get_task_logger(__name__)

# Initialize Celery
celery_app = Celery("bulk_sms")
celery_app.config_from_object(settings.celery_config)


class DatabaseTask(Task):
    """Base task class that handles database sessions"""
    _db = None
    
    @property
    def db(self) -> Session:
        if self._db is None:
            self._db = SessionLocal()
        return self._db
    
    def after_return(self, *args, **kwargs):
        if self._db is not None:
            self._db.close()
            self._db = None


class ArkeselSMSClient:
    """Arkesel SMS API client for background tasks"""
    
    def __init__(self):
        self.api_key = settings.arkesel_api_key
        self.base_url = settings.arkesel_base_url
        self.timeout = 30.0
    
    def send_sms(self, phone_number: str, sender_id: str, message: str) -> Dict[str, Any]:
        """
        Send SMS via Arkesel API
        
        Returns:
            dict: API response with success status
        """
        encoded_message = urllib.parse.quote(message)
        url = (
            f"{self.base_url}?"
            f"action=send-sms&"
            f"api_key={self.api_key}&"
            f"to={phone_number}&"
            f"from={sender_id}&"
            f"sms={encoded_message}"
        )
        
        try:
            response = httpx.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            result = response.json()
            result['success'] = True
            result['http_status'] = response.status_code
            return result
            
        except httpx.TimeoutException:
            return {
                'success': False,
                'error': 'Request timeout',
                'http_status': None
            }
        except httpx.HTTPError as e:
            return {
                'success': False,
                'error': str(e),
                'http_status': getattr(e.response, 'status_code', None)
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'http_status': None
            }


@celery_app.task(bind=True, base=DatabaseTask, max_retries=3)
def send_single_sms(self, message_id: int) -> Dict[str, Any]:
    """
    Send a single SMS message
    
    Args:
        message_id: Database ID of the message to send
        
    Returns:
        dict: Result of SMS sending operation
    """
    db: Session = self.db
    sms_client = ArkeselSMSClient()
    
    try:
        # Get message from database
        message = db.query(Message).filter(Message.id == message_id).first()
        if not message:
            logger.error(f"Message {message_id} not found")
            return {'success': False, 'error': 'Message not found'}
        
        # Get associated contact
        contact = db.query(Contact).filter(Contact.id == message.contact_id).first()
        if not contact:
            logger.error(f"Contact {message.contact_id} not found")
            return {'success': False, 'error': 'Contact not found'}
        
        # Skip if contact is invalid
        if not contact.is_valid:
            message.status = MessageStatus.INVALID_NUMBER
            message.error_message = contact.validation_error
            message.failed_at = datetime.now(timezone.utc)
            db.commit()
            return {'success': False, 'error': 'Invalid phone number'}
        
        # Update message status to sending
        message.status = MessageStatus.SENDING
        message.queued_at = datetime.now(timezone.utc)
        db.commit()
        
        # Send SMS
        logger.info(f"Sending SMS to {contact.phone_number} (Message ID: {message_id})")
        result = sms_client.send_sms(
            phone_number=contact.phone_number,
            sender_id=message.sender_id,
            message=message.message_text
        )
        
        # Update message based on result
        message.api_response = result
        
        if result.get('success'):
            message.status = MessageStatus.SENT
            message.sent_at = datetime.now(timezone.utc)
            logger.info(f"SMS sent successfully to {contact.phone_number}")
        else:
            message.status = MessageStatus.FAILED
            message.error_message = result.get('error', 'Unknown error')
            message.failed_at = datetime.now(timezone.utc)
            message.retry_count += 1
            logger.error(f"Failed to send SMS to {contact.phone_number}: {message.error_message}")
        
        db.commit()
        
        # Update campaign statistics
        update_campaign_stats.delay(message.campaign_id)
        
        return result
        
    except Exception as e:
        logger.error(f"Error sending SMS {message_id}: {str(e)}")
        
        # Update message status to failed
        if message:
            message.status = MessageStatus.FAILED
            message.error_message = str(e)
            message.failed_at = datetime.now(timezone.utc)
            message.retry_count += 1
            db.commit()
        
        # Retry if not exceeded max retries
        if message and message.retry_count < settings.sms_retry_attempts:
            raise self.retry(exc=e, countdown=settings.sms_retry_delay * (2 ** message.retry_count))
        
        return {'success': False, 'error': str(e)}


@celery_app.task(bind=True, base=DatabaseTask)
def send_bulk_sms(self, campaign_id: int, batch_size: int = None) -> Dict[str, Any]:
    """
    Send SMS to all contacts in a campaign
    Processes messages in batches to avoid overwhelming the API
    
    Args:
        campaign_id: Database ID of the campaign
        batch_size: Number of messages to process in each batch
        
    Returns:
        dict: Summary of bulk sending operation
    """
    db: Session = self.db
    batch_size = batch_size or settings.sms_batch_size
    
    try:
        # Get campaign
        campaign = db.query(Campaign).filter(Campaign.id == campaign_id).first()
        if not campaign:
            logger.error(f"Campaign {campaign_id} not found")
            return {'success': False, 'error': 'Campaign not found'}
        
        # Update campaign status
        campaign.status = CampaignStatus.IN_PROGRESS
        campaign.started_at = datetime.now(timezone.utc)
        db.commit()
        
        logger.info(f"Starting bulk SMS for campaign {campaign_id}: {campaign.name}")
        
        # Get all pending messages
        messages = db.query(Message).filter(
            Message.campaign_id == campaign_id,
            Message.status == MessageStatus.PENDING
        ).all()
        
        total_messages = len(messages)
        logger.info(f"Found {total_messages} pending messages")
        
        if total_messages == 0:
            campaign.status = CampaignStatus.COMPLETED
            campaign.completed_at = datetime.now(timezone.utc)
            db.commit()
            return {'success': True, 'total_sent': 0, 'message': 'No pending messages'}
        
        # Process messages in batches
        sent_count = 0
        failed_count = 0
        
        for i in range(0, total_messages, batch_size):
            batch = messages[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1} ({len(batch)} messages)")
            
            # Queue individual send tasks
            for message in batch:
                send_single_sms.delay(message.id)
            
            sent_count += len(batch)
            
            # Rate limiting: wait between batches
            if i + batch_size < total_messages:
                sleep_time = (batch_size / settings.sms_rate_limit) * 60
                logger.info(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
        
        logger.info(f"Queued {sent_count} messages for campaign {campaign_id}")
        
        # Campaign status will be updated by update_campaign_stats task
        return {
            'success': True,
            'total_queued': sent_count,
            'campaign_id': campaign_id
        }
        
    except Exception as e:
        logger.error(f"Error in bulk SMS for campaign {campaign_id}: {str(e)}")
        
        # Update campaign status to failed
        if campaign:
            campaign.status = CampaignStatus.FAILED
            campaign.error_log = {'error': str(e), 'timestamp': datetime.now(timezone.utc).isoformat()}
            db.commit()
        
        return {'success': False, 'error': str(e)}


@celery_app.task(bind=True, base=DatabaseTask)
def update_campaign_stats(self, campaign_id: int) -> Dict[str, Any]:
    """
    Update campaign statistics based on message statuses
    
    Args:
        campaign_id: Database ID of the campaign
        
    Returns:
        dict: Updated statistics
    """
    db: Session = self.db
    
    try:
        campaign = db.query(Campaign).filter(Campaign.id == campaign_id).first()
        if not campaign:
            return {'success': False, 'error': 'Campaign not found'}
        
        # Count messages by status
        messages = db.query(Message).filter(Message.campaign_id == campaign_id).all()
        
        campaign.total_sent = sum(1 for m in messages if m.status in [MessageStatus.SENT, MessageStatus.DELIVERED])
        campaign.total_delivered = sum(1 for m in messages if m.status == MessageStatus.DELIVERED)
        campaign.total_failed = sum(1 for m in messages if m.status in [MessageStatus.FAILED, MessageStatus.INVALID_NUMBER])
        campaign.total_pending = sum(1 for m in messages if m.status == MessageStatus.PENDING)
        
        # Update campaign status based on message statuses
        if campaign.total_pending == 0:
            if campaign.status == CampaignStatus.IN_PROGRESS:
                campaign.status = CampaignStatus.COMPLETED
                campaign.completed_at = datetime.now(timezone.utc)
        
        db.commit()
        
        logger.info(f"Updated stats for campaign {campaign_id}: {campaign.total_sent} sent, {campaign.total_failed} failed")
        
        return {
            'success': True,
            'total_sent': campaign.total_sent,
            'total_failed': campaign.total_failed,
            'total_pending': campaign.total_pending
        }
        
    except Exception as e:
        logger.error(f"Error updating campaign stats {campaign_id}: {str(e)}")
        return {'success': False, 'error': str(e)}


@celery_app.task(bind=True, base=DatabaseTask)
def retry_failed_messages(self, campaign_id: int) -> Dict[str, Any]:
    """
    Retry all failed messages in a campaign
    
    Args:
        campaign_id: Database ID of the campaign
        
    Returns:
        dict: Summary of retry operation
    """
    db: Session = self.db
    
    try:
        # Get all failed messages that haven't exceeded retry limit
        messages = db.query(Message).filter(
            Message.campaign_id == campaign_id,
            Message.status == MessageStatus.FAILED,
            Message.retry_count < settings.sms_retry_attempts
        ).all()
        
        logger.info(f"Retrying {len(messages)} failed messages for campaign {campaign_id}")
        
        # Reset status to pending and queue for sending
        for message in messages:
            message.status = MessageStatus.PENDING
            message.error_message = None
            send_single_sms.delay(message.id)
        
        db.commit()
        
        return {
            'success': True,
            'total_retried': len(messages)
        }
        
    except Exception as e:
        logger.error(f"Error retrying failed messages {campaign_id}: {str(e)}")
        return {'success': False, 'error': str(e)}


@celery_app.task(bind=True, base=DatabaseTask)
def cleanup_old_campaigns(self, days: int = 90) -> Dict[str, Any]:
    """
    Clean up old completed campaigns
    
    Args:
        days: Number of days after which to clean up campaigns
        
    Returns:
        dict: Summary of cleanup operation
    """
    db: Session = self.db
    
    try:
        from datetime import timedelta
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
        
        # Delete old completed campaigns
        deleted = db.query(Campaign).filter(
            Campaign.status == CampaignStatus.COMPLETED,
            Campaign.completed_at < cutoff_date
        ).delete()
        
        db.commit()
        
        logger.info(f"Cleaned up {deleted} old campaigns")
        
        return {
            'success': True,
            'deleted_campaigns': deleted
        }
        
    except Exception as e:
        logger.error(f"Error cleaning up old campaigns: {str(e)}")
        return {'success': False, 'error': str(e)}