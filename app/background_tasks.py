"""
Background Tasks  (replaces celery_tasks.py)
============================================
All five Celery tasks re-implemented as plain async functions:

  • send_single_sms        – send one SMS with retry-aware error handling
  • send_bulk_sms          – batch-enqueue messages for a campaign
  • update_campaign_stats  – recount campaign counters
  • retry_failed_messages  – re-queue failed messages
  • cleanup_old_campaigns  – purge old completed campaigns

Key differences from the Celery version
----------------------------------------
1. Fully async — uses httpx.AsyncClient and AsyncSession throughout.
2. No broker — jobs are dispatched via QueueManager (Postgres queue).
3. Rate limiting uses asyncio.sleep() instead of time.sleep().
4. DB sessions are scoped per-task (context manager), not shared across
   the worker process lifetime.
5. Retry logic lives in QueueManager.mark_failed(); tasks just raise.
"""

from __future__ import annotations

import asyncio
import logging
import urllib.parse
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.campaign import Campaign, Contact, Message, CampaignStatus, MessageStatus
from app.queue_manager import QueueManager

logger = logging.getLogger(__name__)


# ============================================================================
# SMS API client (async)
# ============================================================================

class ArkeselSMSClient:
    """
    Async Arkesel SMS API client.

    A single shared AsyncClient (with connection pooling) is created per
    worker loop iteration and reused across all SMS calls in that batch,
    matching or exceeding Celery's per-worker httpx performance.
    """

    def __init__(self, client: httpx.AsyncClient):
        self._client = client
        self.api_key  = settings.arkesel_api_key
        self.base_url = settings.arkesel_base_url
        self.timeout  = 30.0

    async def send_sms(
        self,
        phone_number: str,
        sender_id: str,
        message: str,
    ) -> Dict[str, Any]:
        """Send a single SMS.  Never raises — returns a result dict."""
        encoded = urllib.parse.quote(message)
        url = (
            f"{self.base_url}?"
            f"action=send-sms&"
            f"api_key={self.api_key}&"
            f"to={phone_number}&"
            f"from={sender_id}&"
            f"sms={encoded}"
        )

        try:
            response = await self._client.get(url, timeout=self.timeout)
            response.raise_for_status()
            result = response.json()
            result["success"] = True
            result["http_status"] = response.status_code
            return result

        except httpx.TimeoutException:
            return {"success": False, "error": "Request timeout", "http_status": None}

        except httpx.HTTPError as exc:
            return {
                "success": False,
                "error": str(exc),
                "http_status": getattr(getattr(exc, "response", None), "status_code", None),
            }

        except Exception as exc:
            return {"success": False, "error": str(exc), "http_status": None}


# ============================================================================
# Task 1 — send_single_sms
# ============================================================================

async def send_single_sms(
    db: AsyncSession,
    message_id: int,
) -> Dict[str, Any]:
    """
    Send one SMS message identified by `message_id`.

    Mirrors the Celery task of the same name, including:
    - Invalid-number short-circuit
    - Status transitions (PENDING → SENDING → SENT / FAILED)
    - Enqueueing update_campaign_stats when done
    - Raising on failure so QueueManager can handle retries
    """
    from sqlalchemy import select

    # ── Fetch message ──────────────────────────────────────────────────
    message_row = (await db.execute(
        select(Message).where(Message.id == message_id)
    )).scalar_one_or_none()

    if not message_row:
        logger.error(f"Message {message_id} not found")
        return {"success": False, "error": "Message not found"}

    # ── Fetch contact ──────────────────────────────────────────────────
    contact = (await db.execute(
        select(Contact).where(Contact.id == message_row.contact_id)
    )).scalar_one_or_none()

    if not contact:
        logger.error(f"Contact {message_row.contact_id} not found")
        return {"success": False, "error": "Contact not found"}

    # ── Invalid number short-circuit ───────────────────────────────────
    if not contact.is_valid:
        message_row.status = MessageStatus.INVALID_NUMBER
        message_row.error_message = contact.validation_error
        message_row.failed_at = datetime.now(timezone.utc)
        await db.flush()
        return {"success": False, "error": "Invalid phone number"}

    # ── Mark as SENDING ────────────────────────────────────────────────
    message_row.status = MessageStatus.SENDING
    message_row.queued_at = datetime.now(timezone.utc)
    await db.flush()

    # ── Send SMS ───────────────────────────────────────────────────────
    async with httpx.AsyncClient() as client:
        sms_client = ArkeselSMSClient(client)
        logger.info(f"Sending SMS to {contact.phone_number} (message_id={message_id})")
        result = await sms_client.send_sms(
            phone_number=contact.phone_number,
            sender_id=message_row.sender_id,
            message=message_row.message_text,
        )

    # ── Persist result ─────────────────────────────────────────────────
    message_row.api_response = result

    if result.get("success"):
        message_row.status = MessageStatus.SENT
        message_row.sent_at = datetime.now(timezone.utc)
        logger.info(f"SMS sent successfully to {contact.phone_number}")
    else:
        message_row.status = MessageStatus.FAILED
        message_row.error_message = result.get("error", "Unknown error")
        message_row.failed_at = datetime.now(timezone.utc)
        message_row.retry_count += 1
        logger.error(
            f"Failed to send SMS to {contact.phone_number}: {message_row.error_message}"
        )

    await db.flush()

    # ── Enqueue stats update ───────────────────────────────────────────
    await QueueManager.enqueue_update_campaign_stats(db, message_row.campaign_id)

    # Surface failures so the worker can apply retry logic
    if not result.get("success"):
        raise RuntimeError(result.get("error", "SMS send failed"))

    return result


# ============================================================================
# Task 2 — send_bulk_sms
# ============================================================================

async def send_bulk_sms(
    db: AsyncSession,
    campaign_id: int,
    batch_size: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Queue all pending messages for a campaign in rate-limited batches.

    Mirrors the Celery task: batches messages, enqueues individual
    send_single_sms jobs, and sleeps between batches to respect
    `settings.sms_rate_limit`.  Uses asyncio.sleep() instead of
    time.sleep() so the event loop stays unblocked.
    """
    from sqlalchemy import select

    batch_size = batch_size or settings.sms_batch_size

    # ── Fetch campaign ─────────────────────────────────────────────────
    campaign = (await db.execute(
        select(Campaign).where(Campaign.id == campaign_id)
    )).scalar_one_or_none()

    if not campaign:
        logger.error(f"Campaign {campaign_id} not found")
        return {"success": False, "error": "Campaign not found"}

    campaign.status = CampaignStatus.IN_PROGRESS
    campaign.started_at = datetime.now(timezone.utc)
    await db.flush()

    logger.info(f"Starting bulk SMS for campaign {campaign_id}: {campaign.name}")

    # ── Fetch pending messages ─────────────────────────────────────────
    messages = (await db.execute(
        select(Message).where(
            Message.campaign_id == campaign_id,
            Message.status == MessageStatus.PENDING,
        )
    )).scalars().all()

    total = len(messages)
    logger.info(f"Found {total} pending messages for campaign {campaign_id}")

    if total == 0:
        campaign.status = CampaignStatus.COMPLETED
        campaign.completed_at = datetime.now(timezone.utc)
        await db.flush()
        return {"success": True, "total_sent": 0, "message": "No pending messages"}

    # ── Batch-enqueue individual SMS jobs ──────────────────────────────
    queued = 0
    for i in range(0, total, batch_size):
        batch = messages[i : i + batch_size]
        logger.info(f"Enqueueing batch {i // batch_size + 1} ({len(batch)} messages)")

        for msg in batch:
            await QueueManager.enqueue_send_single_sms(db, msg.id)

        queued += len(batch)

        # Rate-limit: pause between batches without blocking the event loop
        if i + batch_size < total:
            sleep_secs = (batch_size / settings.sms_rate_limit) * 60
            logger.info(f"Rate limiting: sleeping {sleep_secs:.2f}s")
            await db.commit()          # Persist enqueued jobs before sleeping
            await asyncio.sleep(sleep_secs)

    logger.info(f"Queued {queued} messages for campaign {campaign_id}")
    return {"success": True, "total_queued": queued, "campaign_id": campaign_id}


# ============================================================================
# Task 3 — update_campaign_stats
# ============================================================================

async def update_campaign_stats(
    db: AsyncSession,
    campaign_id: int,
) -> Dict[str, Any]:
    """
    Recount all message-status buckets for a campaign and persist them.
    Marks the campaign COMPLETED when no messages remain pending.
    """
    from sqlalchemy import select

    campaign = (await db.execute(
        select(Campaign).where(Campaign.id == campaign_id)
    )).scalar_one_or_none()

    if not campaign:
        return {"success": False, "error": "Campaign not found"}

    messages = (await db.execute(
        select(Message).where(Message.campaign_id == campaign_id)
    )).scalars().all()

    campaign.total_sent = sum(
        1 for m in messages
        if m.status in (MessageStatus.SENT, MessageStatus.DELIVERED)
    )
    campaign.total_delivered = sum(
        1 for m in messages if m.status == MessageStatus.DELIVERED
    )
    campaign.total_failed = sum(
        1 for m in messages
        if m.status in (MessageStatus.FAILED, MessageStatus.INVALID_NUMBER)
    )
    campaign.total_pending = sum(
        1 for m in messages if m.status == MessageStatus.PENDING
    )

    if campaign.total_pending == 0 and campaign.status == CampaignStatus.IN_PROGRESS:
        campaign.status = CampaignStatus.COMPLETED
        campaign.completed_at = datetime.now(timezone.utc)

    await db.flush()

    logger.info(
        f"Campaign {campaign_id} stats: "
        f"sent={campaign.total_sent} failed={campaign.total_failed} "
        f"pending={campaign.total_pending}"
    )

    return {
        "success": True,
        "total_sent": campaign.total_sent,
        "total_failed": campaign.total_failed,
        "total_pending": campaign.total_pending,
    }


# ============================================================================
# Task 4 — retry_failed_messages
# ============================================================================

async def retry_failed_messages(
    db: AsyncSession,
    campaign_id: int,
) -> Dict[str, Any]:
    """
    Re-queue all retryable failed messages in a campaign.
    Resets their status to PENDING and enqueues a new send_single_sms job.
    """
    from sqlalchemy import select

    messages = (await db.execute(
        select(Message).where(
            Message.campaign_id == campaign_id,
            Message.status == MessageStatus.FAILED,
            Message.retry_count < settings.sms_retry_attempts,
        )
    )).scalars().all()

    logger.info(f"Retrying {len(messages)} failed messages for campaign {campaign_id}")

    for msg in messages:
        msg.status = MessageStatus.PENDING
        msg.error_message = None
        await QueueManager.enqueue_send_single_sms(db, msg.id)

    await db.flush()

    return {"success": True, "total_retried": len(messages)}


# ============================================================================
# Task 5 — cleanup_old_campaigns
# ============================================================================

async def cleanup_old_campaigns(
    db: AsyncSession,
    days: int = 90,
) -> Dict[str, Any]:
    """
    Hard-delete campaigns that completed more than `days` days ago.
    Cascading deletes in the schema remove related contacts/messages.
    """
    from sqlalchemy import delete, select

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    # Fetch IDs first for logging, then delete
    ids = (await db.execute(
        select(Campaign.id).where(
            Campaign.status == CampaignStatus.COMPLETED,
            Campaign.completed_at < cutoff,
        )
    )).scalars().all()

    if ids:
        await db.execute(
            delete(Campaign).where(Campaign.id.in_(ids))
        )
        await db.flush()

    logger.info(f"Cleaned up {len(ids)} old campaigns (older than {days} days)")
    return {"success": True, "deleted_campaigns": len(ids)}