import random
import time

from freezegun import freeze_time

from pipeline.config import settings
from pipeline.resilience import Job, breaker, fair_queue, limiter, semaphore
from pipeline.resilience.admission import TokenBucket
from pipeline.resilience.backoff import full_jitter
from pipeline.resilience.breaker import State


def test_semaphore_never_exceeds_limit(redis):
    sem = semaphore()
    leases = [sem.acquire() for _ in range(25)]
    held = [lease for lease in leases if lease]
    assert len(held) == settings.vendor_slots == 20
    assert sem.in_use() == 20
    sem.release(held[0])
    assert sem.acquire() is not None
    assert sem.acquire() is None


def test_semaphore_lease_expires(redis):
    sem = semaphore()
    with freeze_time("2026-10-30 12:00:00") as frozen:
        for _ in range(20):
            assert sem.acquire()
        assert sem.acquire() is None
        frozen.tick(46)  # lease is 45 s: a crashed holder's slot frees itself
        assert sem.acquire() is not None


def test_three_429s_cut_slots_then_regrow(redis):
    lim = limiter()
    with freeze_time("2026-10-30 12:00:00") as frozen:
        for _ in range(3):
            lim.on_429(2)
        assert lim.effective_slots() == 15
        assert lim.paused_for() > 0
        frozen.tick(30 * 5)
        assert lim.effective_slots() == 20


def test_breaker_opens_on_consecutive_failures_and_recovers(redis):
    b = breaker()
    with freeze_time("2026-10-30 12:00:00") as frozen:
        for _ in range(settings.breaker_consecutive_failures):
            assert b.allow() == "closed"
            b.on_failure("closed")
        assert b.state() is State.OPEN
        assert b.allow() is None
        frozen.tick(31)
        gates = [b.allow() for _ in range(4)]
        assert gates == ["probe", "probe", "probe", None]  # exactly 3 probes
        for _ in range(3):
            b.on_success("probe")
        assert b.state() is State.CLOSED


def test_breaker_failed_probe_reopens_with_doubled_cooldown(redis):
    b = breaker()
    with freeze_time("2026-10-30 12:00:00") as frozen:
        for _ in range(10):
            b.on_failure("closed")
        frozen.tick(31)
        assert b.allow() == "probe"
        b.on_failure("probe")
        assert b.state() is State.OPEN
        assert 59 <= b.seconds_until_probe() <= 60  # 30 s doubled


def test_breaker_window_ratio(redis):
    b = breaker()
    with freeze_time("2026-10-30 12:00:00"):
        for i in range(20):  # alternate: never 10 in a row, but 50% of >= 20 calls
            (b.on_failure if i % 2 else b.on_success)("closed")
        assert b.state() is State.OPEN


def test_breaker_ignores_stragglers_while_open(redis):
    b = breaker()
    with freeze_time("2026-10-30 12:00:00") as frozen:
        for _ in range(10):
            b.on_failure("closed")
        until = b.seconds_until_probe()
        frozen.tick(5)
        b.on_failure("closed")  # an in-flight call finishing late
        assert b.seconds_until_probe() == until - 5


def test_fair_queue_round_robin_and_cap(redis):
    fq = fair_queue()
    for i in range(30):
        fq.push(Job(f"big-{i}", "big"))
    for i in range(3):
        fq.push(Job(f"small-{i}", "small"))
    assert not fq.push(Job("big-0", "big"))  # idempotent
    order = [fq.pop_next().user_id for _ in range(8)]
    assert order[:6] == ["big", "small", "big", "small", "big", "small"]
    # the small user is drained; the big user now gets every slot (alone, no cap)
    assert order[6:] == ["big", "big"]


def test_fair_queue_caps_user_while_others_wait(redis):
    fq = fair_queue()
    for i in range(15):
        fq.push(Job(f"a-{i}", "a"))
    for _ in range(10):
        assert fq.pop_next().user_id == "a"  # alone: no cap
    for i in range(5):
        fq.push(Job(f"b-{i}", "b"))
    # "a" holds 10 slots, "b" is waiting: "a" is skipped until b's jobs run out
    assert [fq.pop_next().user_id for _ in range(5)] == ["b"] * 5
    assert fq.pop_next().user_id == "a"


def test_fair_queue_delayed_promotion(redis):
    fq = fair_queue()
    fq.delay(Job("f1", "u"), time.time() + 60)
    assert not fq.has_waiting()
    assert fq.contains("f1")
    assert fq.promote_due_delayed(now=time.time() + 61) == 1
    assert fq.pop_next().file_id == "f1"
    assert not fq.contains("f1")


def test_full_jitter_bounds():
    rng = random.Random(1)
    for attempt in range(12):
        d = full_jitter(attempt, base=5, cap=600, rng=rng)
        assert 0 <= d <= min(600, 5 * 2 ** attempt)


def test_token_bucket(redis):
    tb = TokenBucket(redis, "t", per_minute=10)
    with freeze_time("2026-10-30 12:00:00") as frozen:
        assert all(tb.take("u")[0] for _ in range(10))
        allowed, wait = tb.take("u")
        assert not allowed and 0 < wait <= 6
        frozen.tick(6)
        assert tb.take("u")[0]
