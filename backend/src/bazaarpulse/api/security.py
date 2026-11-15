"""Passwords, tokens and request throttling."""

from __future__ import annotations

import asyncio
import time
import uuid
from datetime import UTC, datetime, timedelta

import jwt
from argon2 import PasswordHasher
from argon2.exceptions import VerificationError
from redis.asyncio import Redis

from bazaarpulse.config import Settings

_hasher = PasswordHasher()  # Argon2id with the library's current recommended parameters


async def hash_password(password: str) -> str:
    # Argon2 is deliberately slow (tens of milliseconds of CPU); run it off the event loop.
    return await asyncio.to_thread(_hasher.hash, password)


async def verify_password(stored_hash: str, password: str) -> bool:
    def check() -> bool:
        try:
            return _hasher.verify(stored_hash, password)
        except (VerificationError, ValueError):
            return False

    return await asyncio.to_thread(check)


def issue_token(settings: Settings, user_id: uuid.UUID) -> str:
    now = datetime.now(UTC)
    claims = {"sub": str(user_id), "iat": now, "exp": now + timedelta(minutes=settings.jwt_ttl_minutes)}
    return jwt.encode(claims, settings.jwt_secret, algorithm="HS256")


def read_token(settings: Settings, token: str) -> uuid.UUID | None:
    try:
        # Pinning the algorithm list is what stops "alg: none" and key-confusion tokens.
        claims = jwt.decode(token, settings.jwt_secret, algorithms=["HS256"])
        return uuid.UUID(claims["sub"])
    except (jwt.PyJWTError, KeyError, ValueError):
        return None


class RateLimiter:
    """Fixed-window counter in Redis: at most ``limit`` hits per ``window`` seconds per key.

    Shared by every API replica, because the count lives in Redis rather than
    in one process's memory.
    """

    def __init__(self, redis: Redis, prefix: str) -> None:
        self.redis = redis
        self.prefix = prefix

    async def allow(self, key: str, *, limit: int, window: int) -> bool:
        bucket = f"{self.prefix}rl.{key}.{int(time.time() // window)}"
        pipe = self.redis.pipeline(transaction=False)
        pipe.incr(bucket)
        pipe.expire(bucket, window * 2)
        count, _ = await pipe.execute()
        return int(count) <= limit
