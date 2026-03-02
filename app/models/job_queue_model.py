"""
PostgreSQL-backed Job Queue Model
Replaces Redis/Celery broker with a persistent, crash-safe job queue.

Uses SELECT FOR UPDATE SKIP LOCKED for safe concurrent worker access —
multiple workers can poll simultaneously without double-processing jobs.
"""

from app.base import Base
from sqlalchemy import (
    Column, Integer, Text, DateTime, JSON,
    Index, Enum as SQLEnum
)
from sqlalchemy.sql import func
import enum


class JobStatus(str, enum.Enum):
    PENDING   = "pending"    # Waiting to be picked up
    RUNNING   = "running"    # Currently being processed by a worker
    COMPLETED = "completed"  # Finished successfully
    FAILED    = "failed"     # Failed after all retries


class JobType(str, enum.Enum):
    SEND_SINGLE_SMS       = "send_single_sms"
    SEND_BULK_SMS         = "send_bulk_sms"
    UPDATE_CAMPAIGN_STATS = "update_campaign_stats"
    RETRY_FAILED_MESSAGES = "retry_failed_messages"
    CLEANUP_OLD_CAMPAIGNS = "cleanup_old_campaigns"


class JobQueue(Base):
    """
    Persistent job queue backed by PostgreSQL.

    Key design decisions:
    - `status` + `scheduled_at` index enables efficient polling
    - Workers use SELECT FOR UPDATE SKIP LOCKED to claim jobs atomically
    - `locked_until` prevents a crashed worker's job from being stuck forever
    - `retry_count` / `max_retries` mirror Celery's retry semantics
    - `result` stores the final return value (mirrors Celery result backend)
    """
    __tablename__ = "job_queue"

    id            = Column(Integer, primary_key=True, index=True)
    job_type      = Column(SQLEnum(JobType), nullable=False, index=True)
    status        = Column(SQLEnum(JobStatus), default=JobStatus.PENDING, nullable=False, index=True)

    # Payload — matches the kwargs each task function accepts
    payload       = Column(JSON, nullable=False, default=dict)

    # Scheduling & locking
    scheduled_at  = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    locked_until  = Column(DateTime(timezone=True), nullable=True)   # Heartbeat / lease expiry
    started_at    = Column(DateTime(timezone=True), nullable=True)
    completed_at  = Column(DateTime(timezone=True), nullable=True)

    # Retry tracking
    retry_count   = Column(Integer, default=0, nullable=False)
    max_retries   = Column(Integer, default=3, nullable=False)

    # Output / error
    result        = Column(JSON, nullable=True)   # Stores return dict on success
    error_message = Column(Text, nullable=True)

    # Timestamps
    created_at    = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at    = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        # Primary polling index: workers query for pending jobs ordered by schedule time
        Index("idx_jobqueue_status_scheduled", "status", "scheduled_at"),
        # Useful for monitoring per job type
        Index("idx_jobqueue_type_status", "job_type", "status"),
    )

    def __repr__(self):
        return f"<JobQueue(id={self.id}, type='{self.job_type}', status='{self.status}')>"