"""Per-user delivery queues in Redis with round-robin hand-out.

Keys
  q:deliver:{user}   list of file ids waiting for a vendor slot
  fq:active          sorted set of users with waiting jobs; score = round-robin turn (lowest goes next)
  fq:rr              counter that hands out turns
  fq:jobs            hash file_id -> job json, for every job that is waiting or delayed (membership index)
  fq:inflight        hash user -> slots currently held
  delayed:deliver    sorted set file_id -> due time (backoff, Retry-After, breaker waits)

Every operation that touches more than one key is a Lua script, so a job is never in two places at once.
"""

import json
import time
from dataclasses import asdict, dataclass

from redis import Redis

from pipeline.config import settings
from pipeline.redis_client import LuaScript

ACTIVE = "fq:active"
RR = "fq:rr"
JOBS = "fq:jobs"
INFLIGHT = "fq:inflight"
DELAYED = "delayed:deliver"
DISPATCHED = "fq:dispatched"  # file_id -> time handed to Celery; lets the sweeper tell in-transit from lost
QPREFIX = "q:deliver:"


@dataclass
class Job:
    file_id: str
    user_id: str
    attempt: int = 0

    def dumps(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def loads(cls, raw: str) -> "Job":
        return cls(**json.loads(raw))


# ARGV: file_id, user_id, job_json, due (0 = now)
_PUSH_LUA = """
local file_id, user, job, due = ARGV[1], ARGV[2], ARGV[3], tonumber(ARGV[4])
if redis.call('HSETNX', KEYS[1], file_id, job) == 0 then return 0 end
if due > 0 then
  redis.call('ZADD', KEYS[2], due, file_id)
else
  redis.call('RPUSH', KEYS[3] .. user, file_id)
  if redis.call('ZSCORE', KEYS[4], user) == false then
    redis.call('ZADD', KEYS[4], redis.call('INCR', KEYS[5]), user)
  end
end
return 1
"""

# Move due delayed jobs to their user's list. ARGV: now, limit
_PROMOTE_LUA = """
local ids = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
for _, id in ipairs(ids) do
  redis.call('ZREM', KEYS[2], id)
  local raw = redis.call('HGET', KEYS[1], id)
  if raw then
    local user = cjson.decode(raw)['user_id']
    redis.call('RPUSH', KEYS[3] .. user, id)
    if redis.call('ZSCORE', KEYS[4], user) == false then
      redis.call('ZADD', KEYS[4], redis.call('INCR', KEYS[5]), user)
    end
  end
end
return #ids
"""

# Round robin over users with waiting jobs; a user at the per-user cap is skipped while others wait.
# ARGV: per_user_cap, now
_POP_LUA = """
local cap = tonumber(ARGV[1])
local users = redis.call('ZRANGE', KEYS[4], 0, -1)
local n = #users
local function take(user)
  local id = redis.call('LPOP', KEYS[3] .. user)
  if not id then
    redis.call('ZREM', KEYS[4], user)
    return nil
  end
  if redis.call('LLEN', KEYS[3] .. user) == 0 then
    redis.call('ZREM', KEYS[4], user)
  else
    redis.call('ZADD', KEYS[4], redis.call('INCR', KEYS[5]), user)
  end
  redis.call('HINCRBY', KEYS[6], user, 1)
  redis.call('ZADD', KEYS[7], tonumber(ARGV[2]), id)
  local raw = redis.call('HGET', KEYS[1], id)
  redis.call('HDEL', KEYS[1], id)
  return raw
end
for pass = 1, 2 do
  for _, user in ipairs(users) do
    local held = tonumber(redis.call('HGET', KEYS[6], user) or '0')
    if pass == 2 or n == 1 or held < cap then
      local raw = take(user)
      if raw then return raw end
    end
  end
end
return false
"""


class FairQueue:
    def __init__(self, r: Redis):
        self.r = r
        self._push = LuaScript(r, _PUSH_LUA)
        self._promote = LuaScript(r, _PROMOTE_LUA)
        self._pop = LuaScript(r, _POP_LUA)
        self._keys = [JOBS, DELAYED, QPREFIX, ACTIVE, RR, INFLIGHT, DISPATCHED]

    def push(self, job: Job, due: float | None = None) -> bool:
        """Idempotent: a file that is already waiting or delayed is not added twice."""
        return bool(self._push(keys=self._keys, args=[job.file_id, job.user_id, job.dumps(), due or 0]))

    def delay(self, job: Job, due: float) -> bool:
        return self.push(job, due=max(due, 0.001))

    def promote_due_delayed(self, now: float | None = None, limit: int = 500) -> int:
        return int(self._promote(keys=self._keys, args=[now or time.time(), limit]))

    def pop_next(self, per_user_cap: int | None = None) -> Job | None:
        raw = self._pop(keys=self._keys, args=[per_user_cap or settings.vendor_per_user_slots, time.time()])
        return Job.loads(raw) if raw else None

    def slot_done(self, user_id: str) -> None:
        if self.r.hincrby(INFLIGHT, user_id, -1) <= 0:
            self.r.hdel(INFLIGHT, user_id)

    def claimed(self, file_id: str) -> None:
        """The worker has claimed the delivery row; the job is no longer in transit."""
        self.r.zrem(DISPATCHED, file_id)

    def in_transit(self, file_id: str, within_s: float = 300) -> bool:
        score = self.r.zscore(DISPATCHED, file_id)
        return score is not None and score > time.time() - within_s

    def prune_dispatched(self, older_than_s: float = 3600) -> None:
        self.r.zremrangebyscore(DISPATCHED, "-inf", time.time() - older_than_s)

    def has_waiting(self) -> bool:
        return self.r.zcard(ACTIVE) > 0

    def contains(self, file_id: str) -> bool:
        return bool(self.r.hexists(JOBS, file_id))

    def waiting_count(self) -> int:
        """Jobs waiting for a slot or delayed (backlog size)."""
        return int(self.r.hlen(JOBS))

    def ready_count(self) -> int:
        return self.waiting_count() - int(self.r.zcard(DELAYED))

    def waiting_by_user(self) -> dict[str, int]:
        return {u: int(self.r.llen(QPREFIX + u)) for u in self.r.zrange(ACTIVE, 0, -1)}

    def inflight_by_user(self) -> dict[str, int]:
        return {k: int(v) for k, v in self.r.hgetall(INFLIGHT).items()}

    def position(self, user_id: str, file_id: str) -> int | None:
        """Estimated global queue position: own index x number of users taking turns, capped by the backlog."""
        idx = self.r.lpos(QPREFIX + user_id, file_id)
        if idx is None:
            return None
        users = max(1, int(self.r.zcard(ACTIVE)))
        return min(idx * users + 1, max(1, self.ready_count()))

    def delayed_until(self, file_id: str) -> float | None:
        return self.r.zscore(DELAYED, file_id)
