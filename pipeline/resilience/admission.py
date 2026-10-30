"""Per-user token bucket guarding the API (batch creates per minute)."""

import time

from redis import Redis

from pipeline.redis_client import LuaScript

# ARGV: now, capacity, refill_per_s. Returns {allowed (0/1), seconds until next token}
_BUCKET_LUA = """
local key, now, cap, rate = KEYS[1], tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local h = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(h[1]) or cap
local ts = tonumber(h[2]) or now
tokens = math.min(cap, tokens + (now - ts) * rate)
local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end
redis.call('HSET', key, 'tokens', tokens, 'ts', now)
redis.call('EXPIRE', key, math.ceil(cap / rate) + 1)
local wait = 0
if allowed == 0 then wait = (1 - tokens) / rate end
return {allowed, tostring(wait)}
"""


class TokenBucket:
    def __init__(self, r: Redis, name: str, per_minute: int):
        self.r, self.name = r, name
        self.capacity = per_minute
        self.rate = per_minute / 60.0
        self._take = LuaScript(r, _BUCKET_LUA)

    def take(self, user_id: str) -> tuple[bool, float]:
        allowed, wait = self._take(keys=[f"bucket:{self.name}:{user_id}"], args=[time.time(), self.capacity, self.rate])
        return bool(allowed), float(wait)
