import os
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base, Session

# Database URL via env (do not hardcode). Expected from certification_database container:
# POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_PORT
def _build_db_url_from_env() -> str:
    """
    Compose a SQLAlchemy PostgreSQL URL from environment variables.
    Supports both a full POSTGRES_URL override or individual parts.
    """
    url = os.getenv("POSTGRES_URL")
    if url:
        return url

    user = os.getenv("POSTGRES_USER", "")
    password = os.getenv("POSTGRES_PASSWORD", "")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "postgres")

    auth = user
    if password:
        auth = f"{user}:{password}"
    return f"postgresql+psycopg2://{auth}@{host}:{port}/{db}"


DATABASE_URL = _build_db_url_from_env()

# Create SQLAlchemy engine and session factory
engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, future=True)
Base = declarative_base()


@contextmanager
def session_scope() -> Generator[Session, None, None]:
    """
    Context manager to provide transactional scope for a series of DB operations.
    """
    session: Session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
