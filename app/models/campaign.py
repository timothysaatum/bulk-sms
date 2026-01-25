"""
Database models for the Bulk SMS System
"""

from app.base import Base
from sqlalchemy import (
    Column, Integer, String, Text, DateTime, Boolean, 
    ForeignKey, Enum as SQLEnum, Index, JSON, Float
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import enum
from datetime import datetime


class CampaignStatus(str, enum.Enum):
    """Campaign status enumeration"""
    DRAFT = "draft"
    PROCESSING = "processing"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class MessageStatus(str, enum.Enum):
    """Message delivery status enumeration"""
    PENDING = "pending"
    QUEUED = "queued"
    SENDING = "sending"
    SENT = "sent"
    DELIVERED = "delivered"
    FAILED = "failed"
    INVALID_NUMBER = "invalid_number"


class Campaign(Base):
    """
    Campaign model for bulk SMS campaigns
    Each campaign can have multiple contacts and messages
    """
    __tablename__ = "campaigns"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text, nullable=True)
    message_template = Column(Text, nullable=False)
    sender_id = Column(String(11), nullable=False)  # Max 11 chars for SMS sender ID
    
    # Status tracking
    status = Column(
        SQLEnum(CampaignStatus),
        default=CampaignStatus.DRAFT,
        nullable=False,
        index=True
    )
    
    # Statistics
    total_contacts = Column(Integer, default=0)
    total_sent = Column(Integer, default=0)
    total_delivered = Column(Integer, default=0)
    total_failed = Column(Integer, default=0)
    total_pending = Column(Integer, default=0)
    
    # Scheduling
    scheduled_at = Column(DateTime(timezone=True), nullable=True)
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    
    # File upload info
    upload_filename = Column(String(255), nullable=True)
    upload_file_path = Column(String(500), nullable=True)
    
    # Metadata
    created_by = Column(String(255), nullable=True)  # User ID or email
    error_log = Column(JSON, nullable=True)  # Store errors as JSON
    
    # Timestamps
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False
    )
    
    # Relationships
    contacts = relationship(
        "Contact",
        back_populates="campaign",
        cascade="all, delete-orphan"
    )
    messages = relationship(
        "Message",
        back_populates="campaign",
        cascade="all, delete-orphan"
    )
    
    # Indexes
    __table_args__ = (
        Index('idx_campaign_status_created', 'status', 'created_at'),
        Index('idx_campaign_created_by', 'created_by'),
    )
    
    def __repr__(self):
        return f"<Campaign(id={self.id}, name='{self.name}', status='{self.status}')>"
    
    @property
    def success_rate(self) -> float:
        """Calculate campaign success rate"""
        if self.total_contacts == 0:
            return 0.0
        return (self.total_delivered / self.total_contacts) * 100
    
    @property
    def is_active(self) -> bool:
        """Check if campaign is currently active"""
        return self.status in [CampaignStatus.PROCESSING, CampaignStatus.IN_PROGRESS]


class Contact(Base):
    """
    Contact model for storing recipient information
    Each contact belongs to a campaign
    """
    __tablename__ = "contacts"
    
    id = Column(Integer, primary_key=True, index=True)
    campaign_id = Column(
        Integer,
        ForeignKey("campaigns.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    
    # Contact information
    name = Column(String(255), nullable=False)
    phone_number = Column(String(20), nullable=False, index=True)
    email = Column(String(255), nullable=True)
    
    # Additional fields for personalization
    custom_fields = Column(JSON, nullable=True)  # Store custom data as JSON
    
    # Validation
    is_valid = Column(Boolean, default=True, nullable=False)
    validation_error = Column(String(500), nullable=True)
    
    # Timestamps
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False
    )
    
    # Relationships
    campaign = relationship("Campaign", back_populates="contacts")
    message = relationship(
        "Message",
        back_populates="contact",
        uselist=False,
        cascade="all, delete-orphan"
    )
    
    # Indexes
    __table_args__ = (
        Index('idx_contact_campaign_phone', 'campaign_id', 'phone_number'),
        Index('idx_contact_valid', 'is_valid'),
    )
    
    def __repr__(self):
        return f"<Contact(id={self.id}, name='{self.name}', phone='{self.phone_number}')>"


class Message(Base):
    """
    Message model for tracking individual SMS messages
    Each message is associated with a contact and campaign
    """
    __tablename__ = "messages"
    
    id = Column(Integer, primary_key=True, index=True)
    campaign_id = Column(
        Integer,
        ForeignKey("campaigns.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    contact_id = Column(
        Integer,
        ForeignKey("contacts.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        unique=True  # One message per contact
    )
    
    # Message content
    message_text = Column(Text, nullable=False)
    sender_id = Column(String(11), nullable=False)
    
    # Status tracking
    status = Column(
        SQLEnum(MessageStatus),
        default=MessageStatus.PENDING,
        nullable=False,
        index=True
    )
    
    # API response tracking
    api_response = Column(JSON, nullable=True)
    api_message_id = Column(String(255), nullable=True, index=True)
    
    # Error handling
    error_message = Column(Text, nullable=True)
    retry_count = Column(Integer, default=0)
    
    # Timestamps
    queued_at = Column(DateTime(timezone=True), nullable=True)
    sent_at = Column(DateTime(timezone=True), nullable=True)
    delivered_at = Column(DateTime(timezone=True), nullable=True)
    failed_at = Column(DateTime(timezone=True), nullable=True)
    
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False
    )
    
    # Relationships
    campaign = relationship("Campaign", back_populates="messages")
    contact = relationship("Contact", back_populates="message")
    
    # Indexes
    __table_args__ = (
        Index('idx_message_status_created', 'status', 'created_at'),
        Index('idx_message_campaign_status', 'campaign_id', 'status'),
    )
    
    def __repr__(self):
        return f"<Message(id={self.id}, status='{self.status}', contact_id={self.contact_id})>"
    
    @property
    def is_final_state(self) -> bool:
        """Check if message is in a final state (no more processing needed)"""
        return self.status in [
            MessageStatus.SENT,
            MessageStatus.DELIVERED,
            MessageStatus.FAILED,
            MessageStatus.INVALID_NUMBER
        ]


class APILog(Base):
    """
    API log model for tracking all API requests and responses
    Useful for debugging and monitoring
    """
    __tablename__ = "api_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Request details
    endpoint = Column(String(500), nullable=False)
    method = Column(String(10), nullable=False)
    request_params = Column(JSON, nullable=True)
    request_body = Column(JSON, nullable=True)
    
    # Response details
    response_status = Column(Integer, nullable=True)
    response_body = Column(JSON, nullable=True)
    response_time_ms = Column(Float, nullable=True)
    
    # Error tracking
    error = Column(Text, nullable=True)
    
    # Associated records
    campaign_id = Column(Integer, ForeignKey("campaigns.id"), nullable=True, index=True)
    message_id = Column(Integer, ForeignKey("messages.id"), nullable=True, index=True)
    
    # Timestamp
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        index=True
    )
    
    # Indexes
    __table_args__ = (
        Index('idx_apilog_endpoint_created', 'endpoint', 'created_at'),
        Index('idx_apilog_campaign', 'campaign_id'),
    )
    
    def __repr__(self):
        return f"<APILog(id={self.id}, endpoint='{self.endpoint}', status={self.response_status})>"