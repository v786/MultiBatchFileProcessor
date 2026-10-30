"""Global vendor concurrency limit: a 20-slot semaphore in Redis shared by every process.

Redis sorted set `sem:{vendor}`: member = lease id, score = expiry time. Acquire drops expired members and adds
one if fewer than `effective_slots()` are held, in a single Lua call. A crashed holder's slot frees itself when
its lease expires.
"""

import math
import time
from uuid import uuid4

from redis import Redis

from pipeline.config import settings
from pipeline.redis_client import LuaScript

ACQUIRE_LUA = """
local key, now, ttl, limit, lease = KEYS[1], tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), ARGV[4]
redis.call('ZREMRANGEBYSCORE', key, '-inf', now)
if redis.call('ZCARD', key) < limit then
  redis.call('ZADD', key, now + ttl, lease)
  return 1
end
return 0
"""


class ThrottleLimiter:
    """Reacts to vendor 429s: honours Retry-After and shrinks the slot count (AIMD-style).

    Three 429s within 60 s cut effective slots by 25%; one slot grows back every 30 s up to `vendor_slots`.
    """

    def __init__(self, r: Redis, vendor: str = settings.vendor_name):
        self.r = r
        self.paused_key = f"limiter:{vendor}:paused_until"
        self.hits_key = f"limiter:{vendor}:429s"
        self.cut_key = f"limiter:{vendor}:cut"

    @property
    def base_slots(self) -> int:
        return min(settings.vendor_slots, settings.vendor_max_concurrent)

    def effective_slots(self, now: float | None = None) -> int:
        now = time.time() if now is None else now
        cut = self.r.hgetall(self.cut_key)
        if not cut:
            return self.base_slots
        regained = int((now - float(cut["at"])) // settings.throttle_regrow_s)
        slots = min(self.base_slots, int(cut["slots"]) + regained)
        if slots >= self.base_slots:
            self.r.delete(self.cut_key)
        return slots

    def on_429(self, retry_after_s: float) -> None:
        now = time.time()
        until = now + retry_after_s
        current = float(self.r.get(self.paused_key) or 0)
        if until > current:
            self.r.set(self.paused_key, until, ex=max(1, math.ceil(retry_after_s)) + 1)
        self.r.zadd(self.hits_key, {uuid4().hex: now})
        self.r.zremrangebyscore(self.hits_key, "-inf", now - 60)
        self.r.expire(self.hits_key, 120)
        if self.r.zcard(self.hits_key) >= settings.throttle_cut_after:
            new_slots = max(1, math.floor(self.effective_slots(now) * 0.75))
            self.r.hset(self.cut_key, mapping={"slots": new_slots, "at": now})
            self.r.delete(self.hits_key)

    def paused_for(self) -> float:
        until = float(self.r.get(self.paused_key) or 0)
        return max(0.0, until - time.time())


class VendorSemaphore:
    def __init__(self, r: Redis, vendor: str = settings.vendor_name, lease_s: int | None = None,
                 limiter: ThrottleLimiter | None = None):
        self.r, self.key = r, f"sem:{vendor}"
        self.lease_s = lease_s or settings.vendor_slot_lease_s
        self.limiter = limiter or ThrottleLimiter(r, vendor)
        self._acquire = LuaScript(r, ACQUIRE_LUA)

    def acquire(self) -> str | None:
        lease = uuid4().hex
        ok = self._acquire(keys=[self.key], args=[time.time(), self.lease_s, self.limiter.effective_slots(), lease])
        return lease if ok else None

    def refresh(self, lease: str) -> None:
        self.r.zadd(self.key, {lease: time.time() + self.lease_s}, xx=True)

    def release(self, lease: str) -> None:
        self.r.zrem(self.key, lease)

    def in_use(self) -> int:
        self.r.zremrangebyscore(self.key, "-inf", time.time())
        return int(self.r.zcard(self.key))
