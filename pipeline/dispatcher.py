"""Fair dispatcher: hands out the vendor's 20 slots round-robin across users.

Run with `python -m pipeline.dispatcher`. One instance dispatches at a time (leader lock, renewed every 3 s);
a standby takes over within 10 s. The semaphore is global, so even two leaders could not exceed 20 calls.
"""

import signal
import threading
import time
from uuid import uuid4

from pipeline.celery_app import app
from pipeline.config import settings
from pipeline.log import get_logger
from pipeline.redis_client import LuaScript, get_redis
from pipeline.resilience import breaker, fair_queue, limiter, semaphore

log = get_logger("dispatcher")

LEADER_KEY = "dispatcher:leader"
LEADER_TTL_MS = 10_000
EXTRACT_PAUSED_KEY = "dispatcher:extract_paused"

_RENEW_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('PEXPIRE', KEYS[1], ARGV[2]) end
return 0
"""


class Dispatcher:
    def __init__(self):
        self.r = get_redis()
        self.sem, self.breaker, self.fq, self.limiter = semaphore(), breaker(), fair_queue(), limiter()
        self.node = uuid4().hex
        self._renew = LuaScript(self.r, _RENEW_LUA)
        self._last_renew = 0.0
        self._last_backlog_check = 0.0
        self.is_leader = False

    # --- leadership --------------------------------------------------------------------------------------

    def hold_leadership(self) -> bool:
        now = time.monotonic()
        if self.is_leader and now - self._last_renew < 3:
            return True
        if self.is_leader:
            self.is_leader = bool(self._renew(keys=[LEADER_KEY], args=[self.node, LEADER_TTL_MS]))
        if not self.is_leader:
            self.is_leader = bool(self.r.set(LEADER_KEY, self.node, nx=True, px=LEADER_TTL_MS))
            if self.is_leader:
                log.info("dispatcher_leader", node=self.node)
        self._last_renew = now
        return self.is_leader

    def resign(self) -> None:
        if self.is_leader and self.r.get(LEADER_KEY) == self.node:
            self.r.delete(LEADER_KEY)
        self.is_leader = False

    # --- backpressure ------------------------------------------------------------------------------------

    def check_backlog(self) -> None:
        """Too many deliveries waiting: stop consuming the extract queue until the backlog halves."""
        now = time.monotonic()
        if now - self._last_backlog_check < 5:
            return
        self._last_backlog_check = now
        waiting = self.fq.waiting_count()
        paused = bool(self.r.get(EXTRACT_PAUSED_KEY))
        if waiting > settings.backlog_pause and not paused:
            app.control.cancel_consumer("extract")
            self.r.set(EXTRACT_PAUSED_KEY, "1")
            log.warning("extract_paused", deliver_waiting_total=waiting)
        elif waiting < settings.backlog_resume and paused:
            app.control.add_consumer("extract", reply=False)
            self.r.delete(EXTRACT_PAUSED_KEY)
            log.info("extract_resumed", deliver_waiting_total=waiting)

    # --- the loop ----------------------------------------------------------------------------------------

    def tick(self) -> float:
        """Hand out at most one slot. Returns how long to wait before the next tick (0 = go again now)."""
        from pipeline.tasks.deliver import deliver_file

        self.fq.promote_due_delayed()  # delayed:deliver -> user lists
        self.check_backlog()
        if not self.fq.has_waiting():
            return 0.2
        paused = self.limiter.paused_for()  # vendor said Retry-After
        if paused > 0:
            return min(paused, 1.0)
        lease = self.sem.acquire()  # one of the 20 slots
        if lease is None:
            return 0.1  # all slots busy
        gate = self.breaker.allow()  # None while open; "closed" or "probe"
        if gate is None:
            self.sem.release(lease)
            return min(max(self.breaker.seconds_until_probe(), 0.2), 1.0)
        job = self.fq.pop_next()  # round robin, max 10 slots per user while others wait
        if job is None:
            self.sem.release(lease)
            self.breaker.return_probe(gate)
            return 0.2
        deliver_file.apply_async(
            args=[job.file_id, lease, gate],
            kwargs={"user_id": job.user_id},
            task_id=f"deliver:{job.file_id}:{job.attempt}:{lease[:8]}",
        )
        return 0.0

    def run(self, stop: threading.Event) -> None:
        log.info("dispatcher_started", node=self.node, slots=settings.vendor_slots)
        while not stop.is_set():
            try:
                if not self.hold_leadership():
                    stop.wait(1.0)
                    continue
                wait = self.tick()
                if wait:
                    stop.wait(wait)
            except Exception:
                log.exception("dispatcher_error")
                stop.wait(1.0)
        self.resign()
        log.info("dispatcher_stopped", node=self.node)


def main() -> None:
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    Dispatcher().run(stop)


if __name__ == "__main__":
    main()
