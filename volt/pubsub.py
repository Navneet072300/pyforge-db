"""
Pub/Sub engine for Volt.

Clients SUBSCRIBE to channels; any client can PUBLISH to a channel.
Delivery is in-process via queues (for the embedded API) and via
asyncio callbacks (for the TCP server layer).
"""
from __future__ import annotations

import threading
from collections import defaultdict
from typing import Callable, Dict, List, Optional, Set


class PubSub:
    """Thread-safe publish / subscribe hub."""

    def __init__(self):
        self._lock: threading.Lock = threading.Lock()
        # channel → set of subscriber callbacks
        self._subs: Dict[str, Set[Callable]] = defaultdict(set)

    def subscribe(self, channel: str, callback: Callable[[str, str], None]) -> int:
        """
        Subscribe `callback` to `channel`.
        callback signature: callback(channel, message)
        Returns total subscription count for this callback's channels.
        """
        with self._lock:
            self._subs[channel].add(callback)
        return self._channel_count(callback)

    def unsubscribe(self, channel: str, callback: Callable) -> int:
        with self._lock:
            self._subs[channel].discard(callback)
            if not self._subs[channel]:
                del self._subs[channel]
        return self._channel_count(callback)

    def unsubscribe_all(self, callback: Callable):
        with self._lock:
            for ch in list(self._subs):
                self._subs[ch].discard(callback)
                if not self._subs[ch]:
                    del self._subs[ch]

    def publish(self, channel: str, message: str) -> int:
        """Deliver message to all subscribers. Returns receiver count."""
        with self._lock:
            callbacks = list(self._subs.get(channel, set()))
        for cb in callbacks:
            try:
                cb(channel, message)
            except Exception:
                pass
        return len(callbacks)

    def channels(self, pattern: Optional[str] = None) -> List[str]:
        with self._lock:
            names = [ch for ch, subs in self._subs.items() if subs]
        if pattern:
            import fnmatch
            names = [ch for ch in names if fnmatch.fnmatch(ch, pattern)]
        return names

    def numsub(self, *channels: str) -> Dict[str, int]:
        with self._lock:
            return {ch: len(self._subs.get(ch, set())) for ch in channels}

    def _channel_count(self, callback: Callable) -> int:
        with self._lock:
            return sum(1 for subs in self._subs.values() if callback in subs)
