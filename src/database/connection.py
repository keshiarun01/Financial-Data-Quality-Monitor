"""
Database connection manager for the Financial Data Quality Monitor.
Provides a singleton engine, session factory, and context manager
for safe session handling throughout the application.
"""

from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.utils.config import get_settings
from src.models.schema import Base  # noqa: Uses declarative_base() for SA 1.4 compat

import structlog

logger = structlog.get_logger(__name__)

# Module-level singletons (initialized lazily)
_engine: Engine | None = None
_session_factory: sessionmaker | None = None


def get_engine() -> Engine:
    """
    Return a singleton SQLAlchemy engine.
    Creates the engine on first call using settings from config.
    """
    global _engine
    if _engine is None:
        settings = get_settings()
        _engine = create_engine(
            settings.db.url,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,          # Verify connections before use
            pool_recycle=3600,           # Recycle connections after 1 hour
            echo=False,                  # Set True for SQL debugging
        )
        logger.info(
            "Database engine created",
            host=settings.db.host,
            database=settings.db.name,
        )
    return _engine


def get_session_factory() -> sessionmaker:
    """Return a singleton session factory."""
    global _session_factory
    if _session_factory is None:
        _session_factory = sessionmaker(
            bind=get_engine(),
            autocommit=False,
            autoflush=False,
            expire_on_commit=False,
        )
    return _session_factory


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """
    Context manager that yields a database session.
    Automatically commits on success, rolls back on exception.

    Usage:
        with get_session() as session:
            session.add(some_record)
            # auto-commits when block exits cleanly
    """
    factory = get_session_factory()
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        logger.exception("Database session error — rolled back")
        raise
    finally:
        session.close()


def create_all_tables() -> None:
    """
    Create all tables defined in schema.py.
    Safe to call multiple times — only creates tables that don't exist.
    """
    engine = get_engine()
    Base.metadata.create_all(engine)
    logger.info("All database tables created/verified")


def drop_all_tables() -> None:
    """Drop all tables. USE WITH CAUTION — for testing only."""
    engine = get_engine()
    Base.metadata.drop_all(engine)
    logger.warning("All database tables dropped")


def check_connection() -> bool:
    """
    Verify database connectivity.
    Returns True if connection is healthy, False otherwise.
    """
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connection check passed")
        return True
    except Exception as e:
        logger.error("Database connection check failed", error=str(e))
        return False


def get_table_row_counts() -> dict:
    """
    Return row counts for all application tables.
    Useful for health checks and the Streamlit dashboard.
    """
    engine = get_engine()
    counts = {}
    with engine.connect() as conn:
        for table in Base.metadata.sorted_tables:
            result = conn.execute(
                text(f"SELECT COUNT(*) FROM {table.name}")
            )
            counts[table.name] = result.scalar()
    return counts