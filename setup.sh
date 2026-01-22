#!/bin/bash

echo "Setting up Bulk SMS System..."

# Create virtual environment
python3.10 -m venv venv
source venv/bin/activate

# Install requirements
pip install --upgrade pip
pip install -r requirements.txt

# Copy env file
if [ ! -f .env ]; then
    cp .env.example .env
    echo "Created .env file - please edit with your settings"
fi

echo ""
echo "Setup complete!"
echo ""
echo "Next steps:"
echo "1. Edit .env file with your configuration"
echo "2. Setup PostgreSQL: sudo -u postgres createdb bulksms_db"
echo "3. Initialize database: python -c 'import asyncio; from app.database import init_db; asyncio.run(init_db())'"
echo "4. Start API: uvicorn app.main:app --reload"
echo "5. Start Celery: celery -A app.celery_tasks worker --loglevel=info"
