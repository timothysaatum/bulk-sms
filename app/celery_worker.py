"""
Celery Worker Entry Point
Start Celery worker for background task processing
"""

from app.celery_tasks import celery_app
from app.config import settings
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    logger.info("Starting Celery worker...")
    logger.info(f"Broker: {settings.celery_broker_url}")
    logger.info(f"Backend: {settings.celery_result_backend}")
    
    # Start worker
    celery_app.worker_main([
        'worker',
        '--loglevel=info',
        '--concurrency=4',
        '--max-tasks-per-child=1000',
        '--time-limit=3600',
        '--soft-time-limit=3300'
    ])