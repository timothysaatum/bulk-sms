"""
Configuration module for the Bulk SMS System
Handles all application settings using Pydantic Settings
"""

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, validator
from typing import List
import os


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""
    
    # Application Settings
    app_name: str = Field(default="Bulk SMS System", env="APP_NAME")
    app_version: str = Field(default="1.0.0", env="APP_VERSION")
    debug: bool = Field(default=False, env="DEBUG")
    environment: str = Field(default="production", env="ENVIRONMENT")
    
    # Server Settings
    host: str = Field(default="0.0.0.0", env="HOST")
    port: int = Field(default=8000, env="PORT")
    workers: int = Field(default=4, env="WORKERS")
    
    # Database Configuration
    database_url: str = Field(..., env="DATABASE_URL")
    database_url_sync: str = Field(..., env="DATABASE_URL_SYNC")
    
    # Redis Configuration
    redis_url: str = Field(default="redis://localhost:6379/0", env="REDIS_URL")
    celery_broker_url: str = Field(default="redis://localhost:6379/0", env="CELERY_BROKER_URL")
    celery_result_backend: str = Field(default="redis://localhost:6379/0", env="CELERY_RESULT_BACKEND")

    
    # Security
    secret_key: str = Field(..., env="SECRET_KEY")
    algorithm: str = Field(default="HS256", env="ALGORITHM")
    access_token_expire_minutes: int = Field(default=30, env="ACCESS_TOKEN_EXPIRE_MINUTES")
    
    # Arkesel SMS API
    arkesel_api_key: str = Field(..., env="ARKESEL_API_KEY")
    arkesel_base_url: str = Field(
        default="https://sms.arkesel.com/sms/api", 
        env="ARKESEL_BASE_URL"
    )
    arkesel_contacts_url: str = Field(
        default="https://sms.arkesel.com/contacts/api",
        env="ARKESEL_CONTACTS_URL"
    )
    arkesel_default_sender_id: str = Field(default="MyApp", env="ARKESEL_DEFAULT_SENDER_ID")
    
    # SMS Settings
    sms_rate_limit: int = Field(default=60, env="SMS_RATE_LIMIT")
    sms_batch_size: int = Field(default=100, env="SMS_BATCH_SIZE")
    sms_retry_attempts: int = Field(default=3, env="SMS_RETRY_ATTEMPTS")
    sms_retry_delay: int = Field(default=5, env="SMS_RETRY_DELAY")
    
    # File Upload Settings
    max_upload_size: int = Field(default=10485760, env="MAX_UPLOAD_SIZE")  # 10MB
    upload_dir: str = Field(default="./uploads", env="UPLOAD_DIR")
    
    # Pagination
    default_page_size: int = Field(default=50, env="DEFAULT_PAGE_SIZE")
    max_page_size: int = Field(default=200, env="MAX_PAGE_SIZE")
    
    # Logging
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    log_file: str = Field(default="./logs/app.log", env="LOG_FILE")
    
    # CORS Settings
    allowed_extensions: List[str] = ['.xlsx', '.xls', '.csv']
    cors_origins: List[str] = ['http://localhost:3000', 'http://localhost:8080']
    
    # Rate Limiting
    rate_limit_per_minute: int = Field(default=60, env="RATE_LIMIT_PER_MINUTE")
    rate_limit_per_hour: int = Field(default=1000, env="RATE_LIMIT_PER_HOUR")
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    @validator("cors_origins", pre=True)
    def parse_cors_origins(cls, v):
        """Parse CORS origins from comma-separated string"""
        if isinstance(v, str):
            return [origin.strip() for origin in v.split(",")]
        return v
    
    @validator("allowed_extensions", pre=True)
    def parse_allowed_extensions(cls, v):
        """Parse allowed extensions from comma-separated string"""
        if isinstance(v, str):
            return [ext.strip() for ext in v.split(",")]
        return v
    
    @property
    def celery_config(self) -> dict:
        """Get Celery configuration"""
        return {
            "broker_url": self.celery_broker_url,
            "result_backend": self.celery_result_backend,
            "task_serializer": "json",
            "result_serializer": "json",
            "accept_content": ["json"],
            "timezone": "UTC",
            "enable_utc": True,
            "task_track_started": True,
            "task_time_limit": 3600,  # 1 hour
            "task_soft_time_limit": 3300,  # 55 minutes
            "worker_prefetch_multiplier": 1,
            "worker_max_tasks_per_child": 1000,
            "broker_connection_retry_on_startup": True,
        }
    
    def create_upload_dir(self):
        """Create upload directory if it doesn't exist"""
        os.makedirs(self.upload_dir, exist_ok=True)
    
    def create_log_dir(self):
        """Create log directory if it doesn't exist"""
        log_dir = os.path.dirname(self.log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)


# Create settings instance
settings = Settings()

# Create necessary directories
settings.create_upload_dir()
settings.create_log_dir()