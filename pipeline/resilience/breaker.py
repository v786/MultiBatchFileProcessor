"""Vendor circuit breaker, stored in Redis and shared by the dispatcher and every worker.

closed    -> open       when the 60 s window has >= 20 calls and >= 50% failed, or 10 consecutive failures
open      -> half_open  after the cooldown (30 s, doubling on each re-open, max 5 min)
half_open -> closed     when all 3 probe calls succeed; -> open if any probe fails

Outcomes of calls that were already in flight when the breaker opened are recorded but do not restart the
cooldown. Every transition is one Lua script so concurrent workers cannot race each other.
"""

import time
from enum import StrEnum

from redis import Redis

from pipeline.config import settings
from pipeline.redis_client import LuaScript
from pipeline.log import get_logger

log = get_logger(__name__)


class State(StrEnum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


STATE_GAUGE = {State.CLOSED: 0, State.HALF_OPEN: 1, State.OPEN: 2}

# KEYS: state hash, window zset.  Hash fields: state, open_until, reopens, consecutive, probes_issued, probes_ok
_ALLOW_LUA = """
local h, now, probes = KEYS[1], tonumber(ARGV[1]), tonumber(ARGV[2])
local state = redis.call('HGET', h, 'state') or 'closed'
if state == 'closed' then return 'closed' end
if state == 'open' then
  local until_ = tonumber(redis.call('HGET', h, 'open_until') or '0')
  if now < until_ then return '' end
  redis.call('HSET', h, 'state', 'half_open', 'probes_issued', 0, 'probes_ok', 0)
  state = 'half_open'
end
local issued = redis.call('HINCRBY', h, 'probes_issued', 1)
if issued <= probes then return 'probe' end
redis.call('HINCRBY', h, 'probes_issued', -1)
return ''
"""

_RECORD_LUA = """
local h, win, now, ok, gate = KEYS[1], KEYS[2], tonumber(ARGV[1]), ARGV[2] == '1', ARGV[3]
local window_s, min_calls, ratio = tonumber(ARGV[4]), tonumber(ARGV[5]), tonumber(ARGV[6])
local max_consec, cooldown, cooldown_max, probes = tonumber(ARGV[7]), tonumber(ARGV[8]), tonumber(ARGV[9]), tonumber(ARGV[10])
local member = ARGV[11]

redis.call('ZADD', win, now, member .. (ok and ':ok' or ':fail'))
redis.call('ZREMRANGEBYSCORE', win, '-inf', now - window_s)
redis.call('EXPIRE', win, window_s * 2)
local state = redis.call('HGET', h, 'state') or 'closed'

local function open_breaker()
  local reopens = tonumber(redis.call('HGET', h, 'reopens') or '0')
  local cd = math.min(cooldown * (2 ^ reopens), cooldown_max)
  redis.call('HSET', h, 'state', 'open', 'open_until', now + cd, 'reopens', reopens + 1,
             'consecutive', 0, 'opened_at', now)
  redis.call('DEL', win)
  return 'open'
end

if ok then
  redis.call('HSET', h, 'consecutive', 0)
  if state == 'half_open' and gate == 'probe' then
    local okc = redis.call('HINCRBY', h, 'probes_ok', 1)
    if okc >= probes then
      redis.call('HSET', h, 'state', 'closed', 'reopens', 0, 'probes_issued', 0, 'probes_ok', 0)
      redis.call('DEL', win)
      return 'closed'
    end
  end
  return state
end

-- failure
if state == 'half_open' then
  if gate == 'probe' then return open_breaker() end
  return state
end
if state == 'open' then return state end   -- in-flight stragglers do not restart the cooldown
local consec = redis.call('HINCRBY', h, 'consecutive', 1)
if consec >= max_consec then return open_breaker() end
local total = redis.call('ZCARD', win)
if total >= min_calls then
  local fails = 0
  for _, m in ipairs(redis.call('ZRANGE', win, 0, -1)) do
    if string.sub(m, -5) == ':fail' then fails = fails + 1 end
  end
  if fails / total >= ratio then return open_breaker() end
end
return 'closed'
"""


class CircuitBreaker:
    def __init__(self, r: Redis, vendor: str = settings.vendor_name):
        self.r = r
        self.vendor = vendor
        self.key = f"breaker:{vendor}"
        self.window_key = f"breaker:{vendor}:window"
        self._allow = LuaScript(r, _ALLOW_LUA)
        self._record = LuaScript(r, _RECORD_LUA)
        self._seq = 0

    def allow(self) -> str | None:
        """Returns the gate the call runs under: "closed", "probe", or None while calls are not allowed."""
        before = self.state()
        gate = self._allow(keys=[self.key], args=[time.time(), settings.breaker_probes]) or None
        if before is State.OPEN and self.state() is State.HALF_OPEN:
            log.info("breaker_state", vendor=self.vendor, state=State.HALF_OPEN, breaker_state=1)
        return gate

    def return_probe(self, gate: str | None) -> None:
        """A probe slot was handed out but no call was made."""
        if gate == "probe" and self.state() is State.HALF_OPEN:
            self.r.hincrby(self.key, "probes_issued", -1)

    def on_success(self, gate: str | None) -> None:
        self._record_outcome(True, gate)

    def on_failure(self, gate: str | None) -> None:
        self._record_outcome(False, gate)

    def _record_outcome(self, ok: bool, gate: str | None) -> None:
        self._seq += 1
        before = self.state()
        after = self._record(
            keys=[self.key, self.window_key],
            args=[time.time(), "1" if ok else "0", gate or "", settings.breaker_window_s, settings.breaker_min_calls,
                  settings.breaker_failure_ratio, settings.breaker_consecutive_failures, settings.breaker_cooldown_s,
                  settings.breaker_cooldown_max_s, settings.breaker_probes, f"{time.time_ns()}:{id(self)}:{self._seq}"],
        )
        if after != before:
            log.warning("breaker_state", vendor=self.vendor, state=after, breaker_state=STATE_GAUGE[State(after)])

    def state(self) -> State:
        return State(self.r.hget(self.key, "state") or State.CLOSED)

    def seconds_until_probe(self) -> float:
        h = self.r.hgetall(self.key)
        if h.get("state") == State.OPEN:
            return max(0.0, float(h.get("open_until", 0)) - time.time())
        if h.get("state") == State.HALF_OPEN:
            return 1.0
        return 0.0

    def probe_at(self) -> float:
        return time.time() + self.seconds_until_probe()

    def snapshot(self) -> dict:
        h = self.r.hgetall(self.key)
        state = State(h.get("state") or State.CLOSED)
        return {
            "state": state.value,
            "gauge": STATE_GAUGE[state],
            "openUntil": float(h["open_until"]) if state is State.OPEN and "open_until" in h else None,
            "openedAt": float(h["opened_at"]) if "opened_at" in h and state is not State.CLOSED else None,
            "reopens": int(h.get("reopens", 0)),
        }
