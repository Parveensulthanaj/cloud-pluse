"""
Cloud-Native Asynchronous Event Bus.
Emulates enterprise Pub/Sub and Queue patterns (such as AWS EventBridge, SQS/SNS,
and GCP Pub/Sub) with asynchronous subscribers, topics, and Dead Letter Queue (DLQ).
"""
import asyncio
import logging
import time
from collections import defaultdict, deque
from typing import Any, Callable, Coroutine, Dict, List, Set

logger = logging.getLogger("cloudpulse.event_bus")


class EventBus:
    def __init__(self, dlq_max_size: int = 500):
        self._subscribers: Dict[str, Set[Callable[[str, Any], Coroutine[Any, Any, None]]]] = defaultdict(set)
        self._dlq: deque = deque(maxlen=dlq_max_size)
        self._event_stats: Dict[str, int] = {
            "published": 0,
            "delivered": 0,
            "failed": 0,
            "dlq_stored": 0,
        }
        self._recent_events: deque = deque(maxlen=100)
        self._lock = asyncio.Lock()

    def subscribe(self, topic: str, handler: Callable[[str, Any], Coroutine[Any, Any, None]]):
        """Register an async subscriber for a given topic."""
        self._subscribers[topic].add(handler)
        logger.info(f"Subscribed handler {handler.__name__} to topic '{topic}'")

    def unsubscribe(self, topic: str, handler: Callable[[str, Any], Coroutine[Any, Any, None]]):
        """Unregister a subscriber."""
        if handler in self._subscribers[topic]:
            self._subscribers[topic].remove(handler)

    async def publish(self, topic: str, payload: Any, retry_count: int = 2) -> bool:
        """
        Publish an event to a topic. All matching subscribers receive the payload concurrently.
        If handling persistently fails, the event is routed to the Dead Letter Queue (DLQ).
        """
        async with self._lock:
            self._event_stats["published"] += 1
            self._recent_events.append({
                "topic": topic,
                "timestamp": time.time(),
                "summary": str(payload)[:120],
            })

        handlers = list(self._subscribers.get(topic, []))
        # Also include wildcard subscribers if topic has namespaces (e.g. 'telemetry.*')
        prefix = topic.split(".")[0] + ".*"
        if prefix in self._subscribers:
            handlers.extend(list(self._subscribers[prefix]))

        if not handlers:
            return True

        # Dispatch concurrently to subscribers
        tasks = []
        for handler in handlers:
            tasks.append(self._dispatch_with_retry(handler, topic, payload, retry_count))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        delivered = True
        for res in results:
            if isinstance(res, Exception):
                delivered = False
                logger.error(f"Event dispatch failure on topic {topic}: {res}")

        return delivered

    async def _dispatch_with_retry(self, handler, topic: str, payload: Any, retries_left: int):
        attempt = 0
        backoff = 0.05
        while attempt <= retries_left:
            try:
                await handler(topic, payload)
                async with self._lock:
                    self._event_stats["delivered"] += 1
                return
            except Exception as e:
                attempt += 1
                if attempt > retries_left:
                    async with self._lock:
                        self._event_stats["failed"] += 1
                        self._event_stats["dlq_stored"] += 1
                        self._dlq.append({
                            "topic": topic,
                            "payload": payload,
                            "error": str(e),
                            "timestamp": time.time(),
                            "handler": handler.__name__,
                        })
                    logger.warning(f"Routed event to DLQ after {retries_left} retries: {e}")
                    raise
                await asyncio.sleep(backoff)
                backoff *= 2

    def get_stats(self) -> Dict[str, Any]:
        """Return operational telemetry of the event bus."""
        return {
            "metrics": dict(self._event_stats),
            "dlq_size": len(self._dlq),
            "active_topics": list(self._subscribers.keys()),
            "subscriber_counts": {t: len(subs) for t, subs in self._subscribers.items()},
        }

    def get_dlq_events(self) -> List[Dict[str, Any]]:
        """Inspect contents of Dead Letter Queue for cloud diagnostic audits."""
        return list(self._dlq)

    def get_recent_events(self) -> List[Dict[str, Any]]:
        """Return recently processed bus events."""
        return list(self._recent_events)


# Global singleton event bus instance
event_bus = EventBus()
