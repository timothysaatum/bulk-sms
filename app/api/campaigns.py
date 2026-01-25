"""
API Router for Campaign Management
Handles all campaign-related endpoints
"""

from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_async_db
from app.schemas import (
    CampaignCreate, CampaignUpdate, CampaignResponse, CampaignDetailResponse,
    CampaignStats, ContactResponse, MessageResponse, PaginationParams, PaginatedResponse, CampaignExecuteResponse, ContactBulkCreate, FileUploadResponse
)
from app.services import CampaignService, ContactService, MessageService
from app.celery_tasks import send_bulk_sms, retry_failed_messages
from app.models.campaign import CampaignStatus
from app.config import settings
from typing import List, Optional, Union
from pathlib import Path
import shutil
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/campaigns", tags=["Campaigns"])


@router.post(
    "/",
    response_model=CampaignResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new campaign"
)
async def create_campaign(
    campaign: CampaignCreate,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Create a new SMS campaign.
    
    - **name**: Campaign name (required)
    - **description**: Campaign description (optional)
    - **message_template**: SMS message template with optional {name} and custom field placeholders
    - **sender_id**: SMS sender ID (max 11 characters)
    - **scheduled_at**: Schedule time for campaign (optional)
    - **created_by**: User ID or email (optional)
    """
    try:
        new_campaign = await CampaignService.create_campaign(db, campaign)
        await db.commit()
        # Convert SQLAlchemy model to Pydantic schema
        return CampaignResponse.model_validate(new_campaign)
    except Exception as e:
        await db.rollback()
        logger.error(f"Error creating campaign: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create campaign: {str(e)}"
        )


@router.get(
    "/",
    response_model=PaginatedResponse,
    summary="List all campaigns"
)
async def list_campaigns(
    page: int = 1,
    page_size: int = 50,
    status_filter: Optional[CampaignStatus] = None,
    search: Optional[str] = None,
    db: AsyncSession = Depends(get_async_db)
):
    """
    List all campaigns with pagination and optional filters.
    
    - **page**: Page number (default: 1)
    - **page_size**: Items per page (default: 50, max: 200)
    - **status**: Filter by campaign status
    - **search**: Search in campaign name and description
    """
    try:
        # Validate page size
        if page_size > settings.max_page_size:
            page_size = settings.max_page_size
        
        pagination = PaginationParams(page=page, page_size=page_size)
        campaigns, total = await CampaignService.list_campaigns(
            db, pagination, status_filter, search
        )
        
        total_pages = (total + page_size - 1) // page_size
        
        # FIX: Convert SQLAlchemy models to Pydantic schemas
        campaign_responses = [
            CampaignResponse.model_validate(campaign) 
            for campaign in campaigns
        ]
        
        return {
            "items": campaign_responses,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
            "has_next": page < total_pages,
            "has_prev": page > 1
        }
    except Exception as e:
        logger.error(f"Error listing campaigns: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list campaigns: {str(e)}"
        )


@router.get(
    "/{campaign_id}",
    response_model=Union[CampaignDetailResponse, CampaignResponse],
    summary="Get campaign details"
)
async def get_campaign(
    campaign_id: int,
    include_contacts: bool = False,
    db: AsyncSession = Depends(get_async_db)
):
    campaign = await CampaignService.get_campaign(db, campaign_id, include_contacts)
    
    if not campaign:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Campaign {campaign_id} not found"
        )

    # 1. If contacts were requested and loaded, use the Detail schema
    if include_contacts:
        return CampaignDetailResponse.model_validate(campaign)
    
    return CampaignResponse.model_validate(campaign)


@router.patch(
    "/{campaign_id}",
    response_model=CampaignResponse,
    summary="Update a campaign"
)
async def update_campaign(
    campaign_id: int,
    campaign_update: CampaignUpdate,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Update an existing campaign.
    Note: Cannot update active campaigns.
    """
    try:
        updated_campaign = await CampaignService.update_campaign(
            db, campaign_id, campaign_update
        )
        if not updated_campaign:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Campaign {campaign_id} not found"
            )
        await db.commit()
        # Convert SQLAlchemy model to Pydantic schema
        return CampaignResponse.model_validate(updated_campaign)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        await db.rollback()
        logger.error(f"Error updating campaign {campaign_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update campaign: {str(e)}"
        )


@router.delete(
    "/{campaign_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a campaign"
)
async def delete_campaign(
    campaign_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Delete a campaign and all associated contacts and messages.
    Note: Cannot delete active campaigns.
    """
    try:
        deleted = await CampaignService.delete_campaign(db, campaign_id)
        if not deleted:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Campaign {campaign_id} not found"
            )
        await db.commit()
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        await db.rollback()
        logger.error(f"Error deleting campaign {campaign_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete campaign: {str(e)}"
        )


@router.get(
    "/stats/overview",
    response_model=CampaignStats,
    summary="Get campaign statistics"
)
async def get_campaign_stats(
    db: AsyncSession = Depends(get_async_db)
):
    """Get overall campaign statistics and metrics."""
    try:
        stats = await CampaignService.get_campaign_stats(db)
        return stats
    except Exception as e:
        logger.error(f"Error getting campaign stats: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get statistics: {str(e)}"
        )


@router.post(
    "/{campaign_id}/contacts",
    response_model=List[ContactResponse],
    status_code=status.HTTP_201_CREATED,
    summary="Add contacts to campaign"
)
async def add_contacts(
    campaign_id: int,
    contacts: ContactBulkCreate,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Add multiple contacts to a campaign manually.
    
    - **contacts**: Array of contact objects (max 10,000)
    """
    try:
        # Verify campaign exists
        campaign = await CampaignService.get_campaign(db, campaign_id)
        if not campaign:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Campaign {campaign_id} not found"
            )
        
        # Create contacts
        created_contacts, errors = await ContactService.create_contacts_bulk(
            db, campaign_id, contacts.contacts
        )
        
        await db.commit()
        
        # Return created contacts (already Pydantic models)
        return created_contacts
        
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Error adding contacts to campaign {campaign_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to add contacts: {str(e)}"
        )


@router.post(
    "/{campaign_id}/upload",
    response_model=FileUploadResponse,
    summary="Upload contacts file"
)
async def upload_contacts(
    campaign_id: int,
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Upload an Excel/CSV file with contacts.
    
    Required columns: name, phone_number
    Optional columns: email, and any custom fields
    """
    try:
        # Verify campaign exists
        campaign = await CampaignService.get_campaign(db, campaign_id)
        if not campaign:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Campaign {campaign_id} not found"
            )
        
        # Validate file extension
        file_ext = Path(file.filename).suffix.lower()
        if file_ext not in settings.allowed_extensions:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid file type. Allowed: {', '.join(settings.allowed_extensions)}"
            )
        
        # Save file
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"{campaign_id}_{timestamp}{file_ext}"
        file_path = Path(settings.upload_dir) / filename
        
        with file_path.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        logger.info(f"Saved upload file: {file_path}")
        
        # Update campaign with file info
        campaign.upload_filename = file.filename
        campaign.upload_file_path = str(file_path)
        
        # Process file
        contacts_data, errors = await ContactService.process_excel_file(
            file_path, campaign_id
        )
        
        # Create contacts in database
        created_contacts, db_errors = await ContactService.create_contacts_bulk(
            db, campaign_id, contacts_data
        )
        
        # Combine errors
        all_errors = errors + db_errors
        
        await db.commit()
        
        return {
            "filename": file.filename,
            "file_path": str(file_path),
            "total_rows": len(contacts_data) + len(errors),
            "valid_rows": len(created_contacts),
            "invalid_rows": len(all_errors),
            "errors": all_errors[:100]  # Limit errors to first 100
        }
        
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Error uploading file for campaign {campaign_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process file: {str(e)}"
        )


@router.post(
    "/{campaign_id}/execute",
    response_model=CampaignExecuteResponse,
    summary="Execute campaign (send SMS)"
)
async def execute_campaign(
    campaign_id: int,
    force_resend: bool = False,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Execute a campaign to send SMS to all contacts.
    
    This operation:
    1. Creates message records for all valid contacts
    2. Queues messages for background sending
    3. Returns immediately with task ID for status tracking
    
    - **campaign_id**: Campaign to execute
    - **force_resend**: If true, resend to all contacts including previously successful ones
    """
    try:
        # Get campaign
        campaign = await CampaignService.get_campaign(db, campaign_id)
        if not campaign:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Campaign {campaign_id} not found"
            )
        
        # Check if campaign can be executed
        if campaign.status in [CampaignStatus.PROCESSING, CampaignStatus.IN_PROGRESS]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Campaign is already running"
            )
        
        # Create messages for contacts
        message_count = await MessageService.create_messages_for_campaign(db, campaign_id)
        
        if message_count == 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No valid contacts found in campaign"
            )
        
        # Update campaign status
        campaign.status = CampaignStatus.PROCESSING
        await db.commit()
        
        # Queue background task
        task = send_bulk_sms.delay(campaign_id)
        
        # Estimate duration based on rate limit
        estimated_minutes = (message_count / settings.sms_rate_limit)
        
        logger.info(f"Queued campaign {campaign_id} for execution. Task ID: {task.id}")
        
        return {
            "campaign_id": campaign_id,
            "status": "queued",
            "message": f"Campaign queued for execution. {message_count} messages will be sent.",
            "task_id": task.id,
            "total_contacts": message_count,
            "estimated_duration_minutes": round(estimated_minutes, 2)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Error executing campaign {campaign_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to execute campaign: {str(e)}"
        )


@router.post(
    "/{campaign_id}/retry",
    response_model=dict,
    summary="Retry failed messages"
)
async def retry_failed(
    campaign_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Retry all failed messages in a campaign.
    Only retries messages that haven't exceeded the retry limit.
    """
    try:
        # Verify campaign exists
        campaign = await CampaignService.get_campaign(db, campaign_id)
        if not campaign:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Campaign {campaign_id} not found"
            )
        
        # Queue retry task
        task = retry_failed_messages.delay(campaign_id)
        
        return {
            "campaign_id": campaign_id,
            "status": "queued",
            "message": "Failed messages queued for retry",
            "task_id": task.id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrying failed messages for campaign {campaign_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retry messages: {str(e)}"
        )


@router.get(
    "/{campaign_id}/messages",
    response_model=PaginatedResponse,
    summary="Get campaign messages"
)
async def get_campaign_messages(
    campaign_id: int,
    page: int = 1,
    page_size: int = 50,
    status_filter: Optional[str] = None,
    db: AsyncSession = Depends(get_async_db)
):
    """
    Get all messages for a campaign with pagination.
    
    - **campaign_id**: Campaign ID
    - **page**: Page number
    - **page_size**: Items per page
    - **status**: Filter by message status
    """
    try:
        from app.models.campaign import MessageStatus
        
        # Validate page size
        if page_size > settings.max_page_size:
            page_size = settings.max_page_size
        
        pagination = PaginationParams(page=page, page_size=page_size)
        
        # Parse status filter
        msg_status = None
        if status_filter:
            try:
                msg_status = MessageStatus(status_filter)
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid status: {status_filter}"
                )
        
        messages, total = await MessageService.get_campaign_messages(
            db, campaign_id, msg_status, pagination
        )
        
        total_pages = (total + page_size - 1) // page_size
        messages_responses = [
            MessageResponse.model_validate(message) 
            for message in messages
        ]
        return {
            "items": messages_responses,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
            "has_next": page < total_pages,
            "has_prev": page > 1
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting messages for campaign {campaign_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get messages: {str(e)}"
        )