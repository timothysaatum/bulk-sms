from logging.config import fileConfig

from app.base import Base
from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context
from app.models import (
    Campaign,
    Message,
    Contact,
    CampaignStatus,
    MessageStatus,
    APILog,
)
from app.models.job_queue_model import JobQueue  # ← new model

# FIX: read DATABASE_URL_SYNC from .env instead of alembic.ini
from app.config import settings

config = context.config

# FIX: inject the URL before Alembic tries to connect
config.set_main_option("sqlalchemy.url", settings.database_url_sync)

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()