from __future__ import annotations

import asyncio
import json

from order_platform.common.config import settings
from order_platform.common.database import Database
from order_platform.common.events import EventEnvelope, Topic
from order_platform.common.kafka import EventConsumer
from order_platform.common.metrics import events_dlq_total, start_metrics_server


class DlqMonitor:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def record(self, event: EventEnvelope) -> None:
        payload = event.payload
        async with self.database.transaction() as connection:
            await connection.execute(
                """
                INSERT INTO dlq_messages (event_id, original_topic, payload, error)
                VALUES ($1, $2, $3::jsonb, $4)
                ON CONFLICT (event_id) DO NOTHING
                """,
                payload.get("event_id"),
                payload.get("original_topic", "unknown"),
                json.dumps(payload.get("payload", {})),
                payload.get("error", "unknown"),
            )
        events_dlq_total.labels(
            service="dlq-monitor",
            source_topic=str(payload.get("original_topic", "unknown")),
        ).inc()


async def run() -> None:
    service_settings = settings.model_copy(update={"service_name": "dlq-monitor"})
    start_metrics_server(service_settings.dlq_metrics_port)
    database = Database(service_settings)
    await database.connect()
    monitor = DlqMonitor(database)
    consumer = EventConsumer(
        settings=service_settings,
        topics=[Topic.DLQ],
        group_id="dlq-monitor",
        handler=monitor.record,
    )
    await consumer.start()
    try:
        await consumer.run_forever()
    finally:
        await consumer.stop()
        await database.close()


if __name__ == "__main__":
    asyncio.run(run())
