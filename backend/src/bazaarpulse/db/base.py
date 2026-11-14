"""Async SQLAlchemy engine and session factory.

One ``Database`` object is created per process and passed to whoever needs it.
Nothing here is global, so tests can point a second instance at a test database.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class Database:
    def __init__(self, url: str, *, pool_size: int = 10, echo: bool = False) -> None:
        self.engine: AsyncEngine = create_async_engine(
            url,
            pool_size=pool_size,
            max_overflow=pool_size,
            pool_pre_ping=True,  # survive a Postgres restart without handing out dead connections
            echo=echo,
        )
        # expire_on_commit=False: objects stay readable after commit, which is
        # what request handlers want when they return a row they just wrote.
        self._sessionmaker = async_sessionmaker(self.engine, expire_on_commit=False)

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """A session without an open transaction; callers use ``async with s.begin()``."""
        async with self._sessionmaker() as session:
            yield session

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSession]:
        """A session wrapped in one transaction: commit on success, roll back on error."""
        async with self._sessionmaker() as session, session.begin():
            yield session

    async def dispose(self) -> None:
        await self.engine.dispose()
