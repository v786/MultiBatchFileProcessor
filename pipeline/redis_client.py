"""Process-wide Redis client. Re-created after fork so prefork children never share a socket."""

import hashlib
import os
from contextlib import contextmanager
from uuid import uuid4

from redis import Redis
from redis.exceptions import NoScriptError

from pipeline.config import settings

_client: Redis | None = None
_pid: int | None = None


def get_redis() -> Redis:
    global _client, _pid
    if _client is None or _pid != os.getpid():
        _client = Redis.from_url(settings.redis_url, decode_responses=True)
        _pid = os.getpid()
    return _client


def set_redis(client: Redis) -> None:
    """Test hook: inject a fakeredis instance."""
    global _client, _pid
    _client, _pid = client, os.getpid()


_RELEASE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
"""


@contextmanager
def redis_lock(name: str, ttl_s: int = 30):
    """Non-blocking singleton lock for beat tasks. Yields True if this process holds it."""
    r = get_redis()
    token = uuid4().hex
    got = bool(r.set(name, token, nx=True, ex=ttl_s))
    try:
        yield got
    finally:
        if got:
            r.eval(_RELEASE_LUA, 1, name, token)


class LuaScript:
    """EVALSHA with a fallback to EVAL on NOSCRIPT. Unlike redis-py's Script, it does not depend on SCRIPT LOAD
    having reached the same server/connection (failover, SCRIPT FLUSH, proxies); EVAL re-caches it."""

    def __init__(self, r: Redis, source: str):
        self.r, self.source = r, source
        self.sha = hashlib.sha1(source.encode()).hexdigest()

    def __call__(self, keys=(), args=()):
        try:
            return self.r.evalsha(self.sha, len(keys), *keys, *args)
        except NoScriptError:
            return self.r.eval(self.source, len(keys), *keys, *args)
