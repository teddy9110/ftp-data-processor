from os import environ

from sqlalchemy import Engine, create_engine, text, NullPool
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Session, scoped_session, sessionmaker

from app.sqlalchemy_schemas import Base


def get_engine() -> Engine:
    """Return a synchronous sqlalchemy engine"""
    POSTGRES_USER = environ["POSTGRES_USER"]
    POSTGRES_PASSWORD = environ["POSTGRES_PASSWORD"]
    POSTGRES_HOST = environ["POSTGRES_HOST"]
    POSTGRES_PORT = environ["POSTGRES_PORT"]
    POSTGRES_REC_DB_NAME = environ["POSTGRES_REC_DB_NAME"]

    engine = create_engine(
        f"postgresql+psycopg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST},{POSTGRES_PORT}/{POSTGRES_REC_DB_NAME}",
        connect_args={"options": "-c timezone=UTC"},
        pool_pre_ping=True,
        # pool_size=10,
        poolclass=NullPool,
        # pool_recycle=3600,
    )
    return engine


def get_async_engine() -> AsyncEngine:
    """Return an asynchronous sqlalchemy engine"""
    POSTGRES_USER = environ["POSTGRES_USER"]
    POSTGRES_PASSWORD = environ["POSTGRES_PASSWORD"]
    POSTGRES_HOST = environ["POSTGRES_HOST"]
    POSTGRES_PORT = environ["POSTGRES_PORT"]
    POSTGRES_REC_DB_NAME = environ["POSTGRES_REC_DB_NAME"]

    async_engine = create_async_engine(
        f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_REC_DB_NAME}",
        connect_args={"options": "-c timezone=UTC"},
        pool_pre_ping=True,
        pool_size=10,
        pool_recycle=3600,
    )
    return async_engine


engine = get_engine()
async_engine = get_async_engine()
session_maker = sessionmaker(bind=engine)
session_maker_async = async_sessionmaker(bind=async_engine)
session_scoped = scoped_session(session_maker)


def get_session() -> Session:
    return session_maker()


async def get_async_session() -> AsyncSession:
    return session_maker_async()


def get_scoped_session() -> scoped_session[Session]:
    """Return a scoped session. This is used for FastAPI dependency injection."""
    return scoped_session(session_maker)


def session_dependency():
    with get_session() as session:
        try:
            yield session
        finally:
            session.close()


async def async_session_dependency():
    async with get_async_session() as session:
        try:
            yield session
        finally:
            session.close()


def create_tables():
    Base.metadata.create_all(engine)


def drop_tables(terminate_connections: bool = False):  # pragma: no cover
    """Do not ever use this, under any circumstances, except for testing in a local environment"""
    # Base.metadata.drop_all(engine)
    session = get_session()
    while True:
        if terminate_connections:
            session.execute(
                text(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND query NOT LIKE 'DROP SCHEMA%';"
                )
            )
        session.close()
        session = get_session()
        try:
            session.execute(text("DROP SCHEMA IF EXISTS public CASCADE;"))
            break
        except Exception as e:
            session.rollback()
            if not terminate_connections:
                raise e
            else:
                print("Retrying to drop schema...")
    session.execute(text("CREATE SCHEMA public;"))
    session.commit()
    session.close()
