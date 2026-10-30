import random


def full_jitter(attempt: int, base: float, cap: float, rng: random.Random | None = None) -> float:
    """delay = random(0, min(cap, base * 2^attempt)), attempt counted from 0."""
    return (rng or random).uniform(0, min(cap, base * (2 ** max(0, attempt))))
