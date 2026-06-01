import json
import logging
import os

import redis

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
CACHE_TTL = 300  # 5 minutes — specs

_client = redis.from_url(REDIS_URL, decode_responses=True)


def cache_get(key: str) -> dict | list | None:
    try:
        value = _client.get(key)
        return json.loads(value) if value else None
    except Exception as exc:
        logger.warning("Cache GET error [%s]: %s", key, exc)
        return None


def cache_set(key: str, value: dict | list, ttl: int = CACHE_TTL) -> None:
    try:
        _client.setex(key, ttl, json.dumps(value, default=str))
    except Exception as exc:
        logger.warning("Cache SET error [%s]: %s", key, exc)
