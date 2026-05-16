from typing import Optional
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

@dataclass
class RateLimitExceeded(Exception):
    wait_seconds: int
    message: str


class ApiLimiter:
    def __init__(self, per_min: int = 60, per_hour: int = 2000):
        self.per_min = per_min
        self.per_hour = per_hour
        self.min_queue = deque()
        self.hour_queue = deque()

    def _flush_old(self, now: float, queue: deque, window: float) -> None:
        while queue and queue[0] <= now - window:
            queue.popleft()

    def _parse_retry_after(self, retry_after_value: str) -> Optional[int]:
        if not retry_after_value:
            return None
        value = retry_after_value.strip()
        if value.isdigit():
            return max(0, int(value))
        try:
            dt = parsedate_to_datetime(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            delta = (dt - datetime.now(timezone.utc)).total_seconds()
            return max(0, int(delta))
        except Exception:
            return None

    def get_wait_time(self) -> int:
        now = time.monotonic()
        self._flush_old(now, self.min_queue, 60.0)
        self._flush_old(now, self.hour_queue, 3600.0)

        wait_for_min = 0.0
        wait_for_hour = 0.0

        if len(self.min_queue) >= self.per_min:
            wait_for_min = 60.0 - (now - self.min_queue[0])

        if len(self.hour_queue) >= self.per_hour:
            wait_for_hour = 3600.0 - (now - self.hour_queue[0])

        return max(0, int(max(wait_for_min, wait_for_hour)))

    def acquire(self) -> None:
        while True:
            wait_time = self.get_wait_time()
            if wait_time > 0:
                time.sleep(wait_time)
                continue

            now = time.monotonic()
            self.min_queue.append(now)
            self.hour_queue.append(now)
            return

    def acquire_or_fail_if_hour_exceeded(self) -> None:
        wait_time = self.get_wait_time()

        now = time.monotonic()
        self._flush_old(now, self.min_queue, 60.0)
        self._flush_old(now, self.hour_queue, 3600.0)

        if len(self.hour_queue) >= self.per_hour:
            raise RateLimitExceeded(
                wait_seconds=wait_time if wait_time > 0 else 3600,
                message=f"Достигнут лимит {self.per_hour} запросов в час. Запуск минимум через {wait_time if wait_time > 0 else 3600} секунд."
            )

        if wait_time > 0:
            time.sleep(wait_time)

        now = time.monotonic()
        self.min_queue.append(now)
        self.hour_queue.append(now)

    def handle_429(self, retry_after: Optional[str] = None) -> int:
        wait_time = self._parse_retry_after(retry_after)
        if wait_time is None:
            wait_time = 60
        return wait_time
