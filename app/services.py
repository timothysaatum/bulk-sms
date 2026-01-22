"""
Service layer for business logic
Handles campaign and message operations
"""

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, or_
from sqlalchemy.orm import selectinload
from app.models.campaign import Campaign, Contact, Message, CampaignStatus, MessageStatus
from app.schemas import (
    CampaignCreate, CampaignUpdate, ContactCreate,
    CampaignResponse, ContactResponse, MessageResponse,
    PaginationParams
)
from typing import List, Optional, Tuple, Dict, Any
from datetime import datetime
import logging
import pandas as pd
from pathlib import Path
import phonenumbers

logger = logging.getLogger(__name__)


class CampaignService:
    """Service class for campaign operations"""
    
    @staticmethod
    async def create_campaign(
        db: AsyncSession,
        campaign_data: CampaignCreate
    ) -> Campaign:
        """Create a new campaign"""
        campaign = Campaign(
            name=campaign_data.name,
            description=campaign_data.description,
            message_template=campaign_data.message_template,
            sender_id=campaign_data.sender_id,
            scheduled_at=campaign_data.scheduled_at,
            created_by=campaign_data.created_by,
            status=CampaignStatus.DRAFT
        )
        
        db.add(campaign)
        await db.flush()
        await db.refresh(campaign)
        
        logger.info(f"Created campaign {campaign.id}: {campaign.name}")
        return campaign
    
    @staticmethod
    async def get_campaign(
        db: AsyncSession,
        campaign_id: int,
        include_contacts: bool = False
    ) -> Optional[Campaign]:
        """Get a campaign by ID"""
        query = select(Campaign).where(Campaign.id == campaign_id)
        
        if include_contacts:
            query = query.options(selectinload(Campaign.contacts))
        
        result = await db.execute(query)
        return result.scalar_one_or_none()
    
    @staticmethod
    async def list_campaigns(
        db: AsyncSession,
        pagination: PaginationParams,
        status: Optional[CampaignStatus] = None,
        search: Optional[str] = None
    ) -> Tuple[List[Campaign], int]:
        """List campaigns with pagination and filters"""
        # Build query
        query = select(Campaign)
        count_query = select(func.count()).select_from(Campaign)
        
        # Apply filters
        filters = []
        if status:
            filters.append(Campaign.status == status)
        if search:
            filters.append(
                or_(
                    Campaign.name.ilike(f"%{search}%"),
                    Campaign.description.ilike(f"%{search}%")
                )
            )
        
        if filters:
            query = query.where(and_(*filters))
            count_query = count_query.where(and_(*filters))
        
        # Order by created_at desc
        query = query.order_by(Campaign.created_at.desc())
        
        # Apply pagination
        query = query.offset((pagination.page - 1) * pagination.page_size)
        query = query.limit(pagination.page_size)
        
        # Execute queries
        result = await db.execute(query)
        campaigns = result.scalars().all()
        
        count_result = await db.execute(count_query)
        total = count_result.scalar()
        
        return campaigns, total
    
    @staticmethod
    async def update_campaign(
        db: AsyncSession,
        campaign_id: int,
        campaign_data: CampaignUpdate
    ) -> Optional[Campaign]:
        """Update a campaign"""
        campaign = await CampaignService.get_campaign(db, campaign_id)
        if not campaign:
            return None
        
        # Don't allow updates to active campaigns
        if campaign.is_active:
            raise ValueError("Cannot update an active campaign")
        
        # Update fields
        update_data = campaign_data.dict(exclude_unset=True)
        for field, value in update_data.items():
            setattr(campaign, field, value)
        
        await db.flush()
        await db.refresh(campaign)
        
        logger.info(f"Updated campaign {campaign_id}")
        return campaign
    
    @staticmethod
    async def delete_campaign(
        db: AsyncSession,
        campaign_id: int
    ) -> bool:
        """Delete a campaign"""
        campaign = await CampaignService.get_campaign(db, campaign_id)
        if not campaign:
            return False
        
        # Don't allow deletion of active campaigns
        if campaign.is_active:
            raise ValueError("Cannot delete an active campaign")
        
        await db.delete(campaign)
        logger.info(f"Deleted campaign {campaign_id}")
        return True
    
    @staticmethod
    async def get_campaign_stats(db: AsyncSession) -> Dict[str, Any]:
        """Get overall campaign statistics"""
        # Total campaigns
        total_result = await db.execute(select(func.count()).select_from(Campaign))
        total_campaigns = total_result.scalar()
        
        # Active campaigns
        active_result = await db.execute(
            select(func.count()).select_from(Campaign).where(
                Campaign.status.in_([CampaignStatus.PROCESSING, CampaignStatus.IN_PROGRESS])
            )
        )
        active_campaigns = active_result.scalar()
        
        # Completed campaigns
        completed_result = await db.execute(
            select(func.count()).select_from(Campaign).where(
                Campaign.status == CampaignStatus.COMPLETED
            )
        )
        completed_campaigns = completed_result.scalar()
        
        # Message statistics
        total_sent_result = await db.execute(
            select(func.sum(Campaign.total_sent)).select_from(Campaign)
        )
        total_sent = total_sent_result.scalar() or 0
        
        total_delivered_result = await db.execute(
            select(func.sum(Campaign.total_delivered)).select_from(Campaign)
        )
        total_delivered = total_delivered_result.scalar() or 0
        
        total_failed_result = await db.execute(
            select(func.sum(Campaign.total_failed)).select_from(Campaign)
        )
        total_failed = total_failed_result.scalar() or 0
        
        # Calculate success rate
        success_rate = (total_delivered / total_sent * 100) if total_sent > 0 else 0
        
        return {
            'total_campaigns': total_campaigns,
            'active_campaigns': active_campaigns,
            'completed_campaigns': completed_campaigns,
            'total_messages_sent': total_sent,
            'total_messages_delivered': total_delivered,
            'total_messages_failed': total_failed,
            'overall_success_rate': round(success_rate, 2)
        }


class ContactService:
    """Service class for contact operations"""
    
    @staticmethod
    def validate_phone_number(phone: str) -> Tuple[bool, str, Optional[str]]:
        """
        Validate and format phone number
        Returns: (is_valid, formatted_number, error_message)
        """
        try:
            # Clean the number
            cleaned = ''.join(filter(str.isdigit, phone))
            
            # Add country code if not present (default to Ghana)
            if not cleaned.startswith('233') and cleaned.startswith('0'):
                cleaned = '233' + cleaned[1:]
            elif not cleaned.startswith('233'):
                cleaned = '233' + cleaned
            
            # Validate using phonenumbers library
            parsed = phonenumbers.parse(f"+{cleaned}", None)
            if not phonenumbers.is_valid_number(parsed):
                return False, phone, "Invalid phone number"
            
            return True, cleaned, None
            
        except Exception as e:
            return False, phone, f"Phone validation error: {str(e)}"
    
    @staticmethod
    async def create_contact(
        db: AsyncSession,
        campaign_id: int,
        contact_data: ContactCreate
    ) -> Contact:
        """Create a new contact"""
        # Validate phone number
        is_valid, formatted_phone, error = ContactService.validate_phone_number(
            contact_data.phone_number
        )
        
        contact = Contact(
            campaign_id=campaign_id,
            name=contact_data.name,
            phone_number=formatted_phone,
            email=contact_data.email,
            custom_fields=contact_data.custom_fields,
            is_valid=is_valid,
            validation_error=error
        )
        
        db.add(contact)
        await db.flush()
        await db.refresh(contact)
        
        return contact
    
    @staticmethod
    async def create_contacts_bulk(
        db: AsyncSession,
        campaign_id: int,
        contacts_data: List[ContactCreate]
    ) -> Tuple[List[Contact], List[Dict[str, Any]]]:
        """
        Create multiple contacts in bulk
        Returns: (created_contacts, errors)
        """
        created_contacts = []
        errors = []
        
        for idx, contact_data in enumerate(contacts_data):
            try:
                contact = await ContactService.create_contact(db, campaign_id, contact_data)
                created_contacts.append(contact)
            except Exception as e:
                errors.append({
                    'index': idx,
                    'contact': contact_data.dict(),
                    'error': str(e)
                })
        
        # Update campaign total contacts
        campaign = await CampaignService.get_campaign(db, campaign_id)
        if campaign:
            campaign.total_contacts = len(created_contacts)
            campaign.total_pending = len([c for c in created_contacts if c.is_valid])
        
        return created_contacts, errors
    
    @staticmethod
    async def process_excel_file(
        file_path: Path,
        campaign_id: int
    ) -> Tuple[List[ContactCreate], List[Dict[str, Any]]]:
        """
        Process Excel file and extract contacts
        Returns: (contacts, errors)
        """
        contacts = []
        errors = []
        
        try:
            # Read Excel file
            df = pd.read_excel(file_path)
            
            # Expected columns: name, phone_number, email (optional)
            required_columns = ['name', 'phone_number']
            
            # Check for required columns (case-insensitive)
            df.columns = df.columns.str.lower().str.strip()
            
            missing_columns = set(required_columns) - set(df.columns)
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            # Process each row
            for idx, row in df.iterrows():
                try:
                    # Extract data
                    contact_data = {
                        'name': str(row['name']).strip(),
                        'phone_number': str(row['phone_number']).strip(),
                    }
                    
                    # Add optional fields
                    if 'email' in df.columns and pd.notna(row['email']):
                        contact_data['email'] = str(row['email']).strip()
                    
                    # Add custom fields (any extra columns)
                    custom_fields = {}
                    for col in df.columns:
                        if col not in ['name', 'phone_number', 'email'] and pd.notna(row[col]):
                            custom_fields[col] = str(row[col]).strip()
                    
                    if custom_fields:
                        contact_data['custom_fields'] = custom_fields
                    
                    contacts.append(ContactCreate(**contact_data))
                    
                except Exception as e:
                    errors.append({
                        'row': idx + 2,  # Excel row number (1-indexed + header)
                        'data': row.to_dict(),
                        'error': str(e)
                    })
            
            logger.info(f"Processed Excel file: {len(contacts)} valid contacts, {len(errors)} errors")
            
        except Exception as e:
            logger.error(f"Error processing Excel file: {str(e)}")
            raise
        
        return contacts, errors


class MessageService:
    """Service class for message operations"""
    
    @staticmethod
    async def create_messages_for_campaign(
        db: AsyncSession,
        campaign_id: int
    ) -> int:
        """
        Create message records for all valid contacts in a campaign
        Returns: Number of messages created
        """
        # Get campaign
        campaign = await CampaignService.get_campaign(db, campaign_id, include_contacts=True)
        if not campaign:
            raise ValueError("Campaign not found")
        
        # Get all valid contacts without messages
        query = select(Contact).where(
            and_(
                Contact.campaign_id == campaign_id,
                Contact.is_valid == True
            )
        ).outerjoin(Message).where(Message.id == None)
        
        result = await db.execute(query)
        contacts = result.scalars().all()
        
        # Create messages
        created_count = 0
        for contact in contacts:
            # Personalize message if custom fields exist
            message_text = campaign.message_template
            if contact.custom_fields:
                for key, value in contact.custom_fields.items():
                    placeholder = f"{{{key}}}"
                    message_text = message_text.replace(placeholder, str(value))
            
            # Also replace {name} placeholder
            message_text = message_text.replace("{name}", contact.name)
            
            message = Message(
                campaign_id=campaign_id,
                contact_id=contact.id,
                message_text=message_text,
                sender_id=campaign.sender_id,
                status=MessageStatus.PENDING
            )
            db.add(message)
            created_count += 1
        
        await db.flush()
        logger.info(f"Created {created_count} messages for campaign {campaign_id}")
        
        return created_count
    
    @staticmethod
    async def get_campaign_messages(
        db: AsyncSession,
        campaign_id: int,
        status: Optional[MessageStatus] = None,
        pagination: Optional[PaginationParams] = None
    ) -> Tuple[List[Message], int]:
        """Get messages for a campaign with optional filtering"""
        query = select(Message).where(Message.campaign_id == campaign_id)
        count_query = select(func.count()).select_from(Message).where(
            Message.campaign_id == campaign_id
        )
        
        if status:
            query = query.where(Message.status == status)
            count_query = count_query.where(Message.status == status)
        
        query = query.order_by(Message.created_at.desc())
        
        if pagination:
            query = query.offset((pagination.page - 1) * pagination.page_size)
            query = query.limit(pagination.page_size)
        
        result = await db.execute(query)
        messages = result.scalars().all()
        
        count_result = await db.execute(count_query)
        total = count_result.scalar()
        
        return messages, total