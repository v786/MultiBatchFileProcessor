"""Shared resilience primitives, built lazily on the process's Redis client."""

from pipeline.redis_client import get_redis
from pipeline.resilience.breaker import CircuitBreaker
from pipeline.resilience.fair_queue import FairQueue, Job
from pipeline.resilience.semaphore import ThrottleLimiter, VendorSemaphore

_cache: dict[tuple[str, int], object] = {}


def _get(name: str, factory):
    r = get_redis()
    key = (name, id(r))
    if key not in _cache:
        _cache[key] = factory(r)
    return _cache[key]


def limiter() -> ThrottleLimiter:
    return _get("limiter", ThrottleLimiter)


def semaphore() -> VendorSemaphore:
    return _get("semaphore", lambda r: VendorSemaphore(r, limiter=limiter()))


def breaker() -> CircuitBreaker:
    return _get("breaker", CircuitBreaker)


def fair_queue() -> FairQueue:
    return _get("fair_queue", FairQueue)


__all__ = ["Job", "breaker", "fair_queue", "limiter", "semaphore"]
