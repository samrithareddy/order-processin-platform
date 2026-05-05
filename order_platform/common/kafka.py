from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError

from order_platform.common.config import Settings
from order_platform.common.events import EventEnvelope, EventType, Topic, make_event
from order_platform.common.metrics import (
    events_dlq_total,
    events_failed_total,
    events_processed_total,
    events_published_total,
    events_retried_total,
)
from order_platform.common.tracing import build_trace_headers, extract_trace_id

logger = logging.getLogger(__name__)

EventHandler = Callable[[EventEnvelope], Awaitable[None] | None]


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    initial_backoff_seconds: float = 0.25


class EventProducer:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.settings.kafka_bootstrap_servers,
            client_id=f"{self.settings.service_name}-publisher",
        )
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    async def __aenter__(self) -> EventProducer:
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop()

    async def publish(self, topic: Topic | str, event: EventEnvelope) -> None:
        if self._producer is None:
            raise RuntimeError("Kafka producer has not been started")
        topic_value = topic.value if isinstance(topic, Topic) else topic
        await self._producer.send_and_wait(
            topic_value,
            event.model_dump_json().encode("utf-8"),
            key=event.aggregate_id.encode("utf-8"),
            headers=build_trace_headers(event.trace_id),
        )
        events_published_total.labels(
            service=self.settings.service_name,
            event_type=event.event_type,
            topic=topic_value,
        ).inc()


class EventConsumer:
    def __init__(
        self,
        settings: Settings,
        topics: list[Topic | str],
        group_id: str,
        handler: EventHandler,
        *,
        producer: EventProducer | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        self.settings = settings
        self.topics = [topic.value if isinstance(topic, Topic) else topic for topic in topics]
        self.group_id = group_id
        self.handler = handler
        self.producer = producer
        self.retry_policy = retry_policy or RetryPolicy(
            max_attempts=settings.retry_attempts,
            initial_backoff_seconds=settings.retry_base_delay_seconds,
        )
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=self.settings.kafka_bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            client_id=f"{self.group_id}-consumer",
        )
        await self._consumer.start()

    async def stop(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None

    async def run_forever(self) -> None:
        if self._consumer is None:
            raise RuntimeError("Kafka consumer has not been started")
        async for message in self._consumer:
            event = EventEnvelope.model_validate_json(message.value)
            header_trace_id = extract_trace_id(message.headers or [])
            if header_trace_id:
                event.trace_id = header_trace_id
            await self._process_with_retries(event)
            await self._consumer.commit()

    async def _process_with_retries(self, event: EventEnvelope) -> None:
        for attempt in range(1, self.retry_policy.max_attempts + 1):
            try:
                result = self.handler(event)
                if inspect.isawaitable(result):
                    await result
                events_processed_total.labels(
                    service=self.settings.service_name,
                    event_type=event.event_type,
                ).inc()
                return
            except Exception as exc:
                events_failed_total.labels(
                    service=self.settings.service_name,
                    event_type=event.event_type,
                ).inc()
                if attempt >= self.retry_policy.max_attempts:
                    logger.exception("Event processing failed permanently: %s", event.event_id)
                    if self.producer is not None:
                        await self._publish_dlq(event, exc)
                    return
                events_retried_total.labels(
                    service=self.settings.service_name,
                    event_type=event.event_type,
                ).inc()
                await asyncio.sleep(self.retry_policy.initial_backoff_seconds * 2 ** (attempt - 1))

    async def _publish_dlq(self, event: EventEnvelope, error: Exception) -> None:
        if self.producer is None:
            return
        dlq_event = make_event(
            EventType.DLQ_MESSAGE,
            aggregate_id=event.aggregate_id,
            trace_id=event.trace_id,
            payload={
                "event_id": event.event_id,
                "original_topic": event.event_type,
                "event_type": event.event_type,
                "payload": event.payload,
                "error": str(error),
                "consumer_group": self.group_id,
                "attempts": self.retry_policy.max_attempts,
            },
        )
        await self.producer.publish(Topic.DLQ, dlq_event)
        events_dlq_total.labels(
            service=self.settings.service_name,
            source_topic=event.event_type,
        ).inc()


async def wait_for_kafka(settings: Settings, timeout_seconds: int = 60) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while True:
        producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
        try:
            await producer.start()
            await producer.stop()
            return
        except KafkaError:
            if asyncio.get_running_loop().time() >= deadline:
                raise
            await asyncio.sleep(1)
        finally:
            if producer.client is not None:
                await producer.stop()
