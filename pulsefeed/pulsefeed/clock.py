"""Clock abstraction.

Everything in the pipeline reads time through a ``Clock`` rather than calling
``time.time()`` directly. That buys two things: tests can run deterministically,
and the replay harness can run an hour of event traffic in seconds while still
reporting latency in *simulated* seconds, so measured p99 means the same thing
whether the run took 2 seconds or 2 hours.
"""

from __future__ import annotations

import asyncio
import time
from typing import Protocol


class Clock(Protocol):
    def now(self) -> float:
        ...

    async def sleep(self, seconds: float) -> None:
        ...


class RealClock:
    """Wall-clock time. What production uses."""

    def now(self) -> float:
        return time.time()

    async def sleep(self, seconds: float) -> None:
        if seconds > 0:
            await asyncio.sleep(seconds)


class ScaledClock:
    """Simulated time that advances faster than wall time.

    ``scale=0.02`` means one simulated second costs 20ms of real time, so a
    600-second trace replays in about 12 seconds. Every duration in the system —
    provider latency, queue waits, coalescing windows, deadlines — is scaled by
    the same factor, so the relative dynamics that produce queueing behaviour
    are preserved exactly; only the wall-clock cost of observing them changes.

    The limit is real work: once simulated durations shrink below the CPU cost
    of the code doing the work, the simulation stops being faithful. Keep scale
    high enough that provider latency stays well above per-event compute.
    """

    def __init__(self, scale: float = 0.02, origin: float | None = None) -> None:
        if not 0 < scale <= 1.0:
            raise ValueError("scale must be in (0, 1]")
        self.scale = scale
        self.origin = origin if origin is not None else time.time()
        self._t0 = time.monotonic()

    def now(self) -> float:
        return self.origin + (time.monotonic() - self._t0) / self.scale

    async def sleep(self, seconds: float) -> None:
        if seconds > 0:
            await asyncio.sleep(seconds * self.scale)

    def real_seconds(self, simulated_seconds: float) -> float:
        return simulated_seconds * self.scale


class VirtualClock:
    """Discrete-event simulated time. No relation to wall time at all.

    ``ScaledClock`` was the first attempt and it does not survive contact with
    a real trace: every ``asyncio.sleep`` overshoots by a millisecond or so of
    scheduling overhead, and at a 50x scale that millisecond is 50 simulated
    milliseconds. Across thousands of events the simulated clock runs far ahead
    of the trace, and events start arriving "after" their own freshness
    deadlines — which silently corrupts exactly the measurements the harness
    exists to produce.

    Here, time only moves when nothing is runnable. ``sleep()`` parks the caller
    on a heap keyed by wake time; the driver drains the ready queue, and when
    every task is parked it jumps the clock straight to the earliest wake time.
    Simulated durations are therefore exact, and the run goes as fast as the CPU
    allows rather than as fast as the scale factor permits.

    Requirement: nothing in the system under test may call ``asyncio.sleep``
    with a nonzero duration directly — all waiting goes through the clock.
    """

    def __init__(self, origin: float = 1_700_000_000.0) -> None:
        self._t = origin
        self._waiters: list = []
        self._seq = 0
        self._idle_rounds = 8

    def now(self) -> float:
        return self._t

    async def sleep(self, seconds: float) -> None:
        if seconds <= 0:
            await asyncio.sleep(0)
            return
        import heapq

        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._seq += 1
        heapq.heappush(self._waiters, (self._t + seconds, self._seq, future))
        await future

    def _wake_due(self) -> int:
        import heapq

        woken = 0
        while self._waiters and self._waiters[0][0] <= self._t:
            _, _, future = heapq.heappop(self._waiters)
            if not future.done():
                future.set_result(None)
                woken += 1
        return woken

    async def run(self, until_done, max_steps: int = 5_000_000) -> None:
        """Drive simulated time until ``until_done()`` returns True.

        The idle test is cooperative: yield to the event loop several times and,
        if no task became runnable and nothing woke up, conclude that everyone
        is parked on a timer and advance the clock to the earliest one.
        """
        import heapq

        steps = 0
        while steps < max_steps:
            steps += 1
            for _ in range(self._idle_rounds):
                await asyncio.sleep(0)
            if self._wake_due():
                continue
            if until_done():
                return
            if not self._waiters:
                # Nothing is waiting on time and the caller is not finished, so
                # progress depends on real work still in flight. Yield again.
                await asyncio.sleep(0)
                if until_done():
                    return
                continue
            self._t = max(self._t, self._waiters[0][0])
        raise RuntimeError("VirtualClock.run exceeded max_steps without finishing")

    def advance_to(self, timestamp: float) -> None:
        self._t = max(self._t, timestamp)

    @property
    def pending_timers(self) -> int:
        return len(self._waiters)


class ManualClock:
    """Fully controlled time for unit tests. Never sleeps."""

    def __init__(self, start: float = 1_700_000_000.0) -> None:
        self._t = start

    def now(self) -> float:
        return self._t

    def advance(self, seconds: float) -> None:
        self._t += seconds

    async def sleep(self, seconds: float) -> None:
        self._t += seconds
        await asyncio.sleep(0)
