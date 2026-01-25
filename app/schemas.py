"""
Pydantic schemas for request/response validation
"""

from pydantic import BaseModel, Field, validator, EmailStr
from typing import Optional, List, Dict, Any
from datetime import datetime
from app.models.campaign import CampaignStatus, MessageStatus
import phonenumbers


# ============================================================================
# Contact Schemas
# ============================================================================

class ContactBase(BaseModel):
    """Base contact schema"""
    name: str = Field(..., min_length=1, max_length=255, description="Contact name")
    phone_number: str = Field(..., description="Phone number in international format")
    email: Optional[EmailStr] = Field(None, description="Email address")
    custom_fields: Optional[Dict[str, Any]] = Field(None, description="Custom fields for personalization")
    
    @validator('phone_number')
    def validate_phone_number(cls, v):
        """Validate and format phone number"""
        # Remove spaces and special characters
        cleaned = ''.join(filter(str.isdigit, v))
        
        # Add country code if not present (default to Ghana)
        if not cleaned.startswith('233') and cleaned.startswith('0'):
            cleaned = '233' + cleaned[1:]
        elif not cleaned.startswith('233'):
            cleaned = '233' + cleaned
        
        # Validate using phonenumbers library
        try:
            parsed = phonenumbers.parse(f"+{cleaned}", None)
            if not phonenumbers.is_valid_number(parsed):
                raise ValueError("Invalid phone number")
            return cleaned
        except Exception:
            raise ValueError(f"Invalid phone number format: {v}")


class ContactCreate(ContactBase):
    """Schema for creating a contact"""
    pass


class ContactResponse(ContactBase):
    """Schema for contact response"""
    id: int
    campaign_id: int
    is_valid: bool
    validation_error: Optional[str]
    created_at: datetime
    
    class Config:
        from_attributes = True


class ContactBulkCreate(BaseModel):
    """Schema for bulk contact creation"""
    contacts: List[ContactCreate] = Field(..., min_items=1, max_items=10000)


# ============================================================================
# Campaign Schemas
# ============================================================================

class CampaignBase(BaseModel):
    """Base campaign schema"""
    name: str = Field(..., min_length=1, max_length=255, description="Campaign name")
    description: Optional[str] = Field(None, description="Campaign description")
    message_template: str = Field(..., min_length=1, description="SMS message template")
    sender_id: str = Field(..., min_length=1, max_length=11, description="SMS sender ID")
    scheduled_at: Optional[datetime] = Field(None, description="Schedule time for campaign")
    
    @validator('sender_id')
    def validate_sender_id(cls, v):
        """Validate sender ID format"""
        # Remove spaces and special characters
        cleaned = ''.join(c for c in v if c.isalnum())
        if not cleaned:
            raise ValueError("Sender ID must contain alphanumeric characters")
        if len(cleaned) > 11:
            raise ValueError("Sender ID must be 11 characters or less")
        return cleaned


class CampaignCreate(CampaignBase):
    """Schema for creating a campaign"""
    created_by: Optional[str] = Field(None, description="User ID or email")


class CampaignUpdate(BaseModel):
    """Schema for updating a campaign"""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    message_template: Optional[str] = Field(None, min_length=1)
    sender_id: Optional[str] = Field(None, min_length=1, max_length=11)
    scheduled_at: Optional[datetime] = None
    status: Optional[CampaignStatus] = None


class CampaignResponse(CampaignBase):
    """Schema for campaign response"""
    id: int
    status: CampaignStatus
    total_contacts: int
    total_sent: int
    total_delivered: int
    total_failed: int
    total_pending: int
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    upload_filename: Optional[str]
    created_at: datetime
    updated_at: datetime
    success_rate: float
    is_active: bool
    
    class Config:
        from_attributes = True


class CampaignDetailResponse(CampaignResponse):
    """Detailed campaign response with contacts"""
    contacts: Optional[List[ContactResponse]] = None


class CampaignStats(BaseModel):
    """Campaign statistics"""
    total_campaigns: int
    active_campaigns: int
    completed_campaigns: int
    total_messages_sent: int
    total_messages_delivered: int
    total_messages_failed: int
    overall_success_rate: float


# ============================================================================
# Message Schemas
# ============================================================================

class MessageBase(BaseModel):
    """Base message schema"""
    message_text: str
    sender_id: str


class MessageResponse(MessageBase):
    """Schema for message response"""
    id: int
    campaign_id: int
    contact_id: int
    status: MessageStatus
    error_message: Optional[str]
    retry_count: int
    queued_at: Optional[datetime]
    sent_at: Optional[datetime]
    delivered_at: Optional[datetime]
    failed_at: Optional[datetime]
    created_at: datetime
    
    class Config:
        from_attributes = True


# ============================================================================
# File Upload Schemas
# ============================================================================

class FileUploadResponse(BaseModel):
    """Response for file upload"""
    filename: str
    file_path: str
    total_rows: int
    valid_rows: int
    invalid_rows: int
    errors: List[Dict[str, Any]] = []


class ExcelUploadRequest(BaseModel):
    """Request for Excel file upload with campaign creation"""
    campaign_name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    message_template: str = Field(..., min_length=1)
    sender_id: str = Field(..., min_length=1, max_length=11)
    scheduled_at: Optional[datetime] = None
    created_by: Optional[str] = None


# ============================================================================
# Pagination Schemas
# ============================================================================

class PaginationParams(BaseModel):
    """Pagination parameters"""
    page: int = Field(default=1, ge=1, description="Page number")
    page_size: int = Field(default=50, ge=1, le=200, description="Items per page")


class PaginatedResponse(BaseModel):
    """Generic paginated response"""
    items: List[Any]
    total: int
    page: int
    page_size: int
    total_pages: int
    has_next: bool
    has_prev: bool


# ============================================================================
# Campaign Execution Schemas
# ============================================================================

class CampaignExecuteRequest(BaseModel):
    """Request to execute a campaign"""
    campaign_id: int
    force_resend: bool = Field(default=False, description="Resend to all contacts including successful ones")


class CampaignExecuteResponse(BaseModel):
    """Response for campaign execution"""
    campaign_id: int
    status: str
    message: str
    task_id: Optional[str] = None
    total_contacts: int
    estimated_duration_minutes: Optional[float] = None


# ============================================================================
# Batch Operation Schemas
# ============================================================================

class BatchStatusResponse(BaseModel):
    """Response for batch operation status"""
    task_id: str
    status: str
    progress: float
    total: int
    processed: int
    successful: int
    failed: int
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    error: Optional[str] = None


# ============================================================================
# Error Response Schema
# ============================================================================

class ErrorResponse(BaseModel):
    """Standard error response"""
    error: str
    detail: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)