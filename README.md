# Bulk SMS Messaging System

A production-ready FastAPI application for sending bulk SMS messages via the Arkesel API with Excel file upload support, background processing, and comprehensive campaign management.

## Features

✅ **Campaign Management** - Create, update, delete, and track SMS campaigns  
✅ **Contact Management** - Manual entry or Excel upload (up to 10,000 contacts)  
✅ **Message Processing** - Background processing with Celery and retry logic  
✅ **Monitoring** - Real-time statistics and comprehensive logging  
✅ **Production Ready** - Async operations, security, error handling  

## Quick Start

### 1. Install Dependencies

```bash
# Install system dependencies
sudo apt update
sudo apt install python3.10 python3.10-venv postgresql postgresql-contrib redis-server

# Create virtual environment
python3.10 -m venv venv
source venv/bin/activate

# Install Python packages
pip install -r requirements.txt
```

### 2. Setup Database

```bash
# Create database
sudo -u postgres createdb bulksms_db

# Initialize tables
python -c "import asyncio; from app.database import init_db; asyncio.run(init_db())"
```

### 3. Configure Environment

```bash
# Copy example environment file
cp .env.example .env

# Edit with your settings
nano .env
```

Required settings in `.env`:
- `DATABASE_URL` - PostgreSQL connection string
- `ARKESEL_API_KEY` - Your Arkesel API key
- `SECRET_KEY` - Random secure key

### 4. Run Application

```bash
# Terminal 1: Start FastAPI
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# Terminal 2: Start Celery Worker
celery -A app.celery_tasks worker --loglevel=info

# Terminal 3 (Optional): Start Flower for monitoring
celery -A app.celery_tasks flower --port=5555
```

### 5. Access API

- **API Documentation**: http://localhost:8000/docs
- **Alternative Docs**: http://localhost:8000/redoc
- **Celery Monitor**: http://localhost:5555

## Usage Example

### Create Campaign and Upload Contacts

```bash
# 1. Create campaign
curl -X POST "http://localhost:8000/api/campaigns/" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Welcome Campaign",
    "message_template": "Hello {name}, welcome to our service!",
    "sender_id": "MyApp"
  }'

# 2. Upload Excel file with contacts
curl -X POST "http://localhost:8000/api/campaigns/1/upload" \
  -F "file=@contacts.xlsx"

# 3. Execute campaign (send SMS)
curl -X POST "http://localhost:8000/api/campaigns/1/execute"

# 4. Check status
curl "http://localhost:8000/api/campaigns/1"
```

## Excel File Format

Your Excel file should have these columns:

| name       | phone_number  | email              |
|------------|---------------|--------------------|
| John Doe   | 233544919953  | john@example.com   |
| Jane Smith | 0501234567    | jane@example.com   |

- **name** (required): Contact name
- **phone_number** (required): Phone number (will be auto-formatted)
- **email** (optional): Email address
- Additional columns become custom fields for personalization

## Message Personalization

Use placeholders in your message template:

```
Hello {name}, your order #{order_id} has been confirmed!
```

The system replaces `{name}` and any custom field like `{order_id}`.

## Production Deployment

### Using Docker Compose

```bash
docker-compose up -d
```

### Using Systemd

See `docs/DEPLOYMENT.md` for systemd service configuration.

## API Endpoints

- `POST /api/campaigns/` - Create campaign
- `GET /api/campaigns/` - List campaigns
- `GET /api/campaigns/{id}` - Get campaign details
- `PUT /api/campaigns/{id}` - Update campaign
- `DELETE /api/campaigns/{id}` - Delete campaign
- `POST /api/campaigns/{id}/upload` - Upload contacts Excel
- `POST /api/campaigns/{id}/contacts` - Add contacts manually
- `POST /api/campaigns/{id}/execute` - Execute campaign
- `POST /api/campaigns/{id}/retry` - Retry failed messages
- `GET /api/campaigns/{id}/messages` - Get messages
- `GET /api/campaigns/stats/overview` - Statistics

## Configuration

All settings in `.env`:

```env
# Database
DATABASE_URL=postgresql+asyncpg://user:pass@localhost:5432/bulksms_db

# Redis
REDIS_URL=redis://localhost:6379/0

# Arkesel API
ARKESEL_API_KEY=your_api_key_here
ARKESEL_DEFAULT_SENDER_ID=YourApp

# SMS Settings
SMS_RATE_LIMIT=60
SMS_BATCH_SIZE=100
SMS_RETRY_ATTEMPTS=3
```

## Troubleshooting

### Database Connection Error
```bash
# Check PostgreSQL is running
sudo systemctl status postgresql

# Test connection
psql -U postgres -d bulksms_db -c "SELECT 1"
```

### Celery Not Processing
```bash
# Check worker status
celery -A app.celery_tasks inspect active

# Restart worker
celery -A app.celery_tasks worker --loglevel=info
```

## License

MIT License

## Support

For issues, check the logs:
```bash
tail -f logs/app.log
```
