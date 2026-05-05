from __future__ import annotations

import asyncio

from prometheus_client import Counter

from order_platform.common.config import settings
from order_platform.common.database import Database
from order_platform.common.events import EventEnvelope, EventType, make_event
from order_platform.common.kafka import EventConsumer, EventProducer
from order_platform.common.metrics import start_metrics_server

saga_compensations_total = Counter(
    "order_platform_saga_compensations_total",
    "Saga compensation workflows started.",
    ["reason"],
)


def compensation_commands(event: EventEnvelope) -> list[EventEnvelope]:
    reason = str(event.payload.get("reason", "payment_failed"))
    reservation_id = str(event.payload.get("reservation_id", ""))
    return [
        make_event(
            EventType.INVENTORY_RELEASE_REQUESTED,
            aggregate_id=event.aggregate_id,
            trace_id=event.trace_id,
            payload={
                "order_id": event.aggregate_id,
                "reservation_id": reservation_id,
                "reason": reason,
            },
        ),
        make_event(
            EventType.ORDER_CANCEL_REQUESTED,
            aggregate_id=event.aggregate_id,
            trace_id=event.trace_id,
            payload={"order_id": event.aggregate_id, "reason": reason},
        ),
    ]


class SagaOrchestrator:
    def __init__(self, database: Database, producer: EventProducer) -> None:
        self.database = database
        self.producer = producer

    async def handle_payment_failed(self, event: EventEnvelope) -> None:
        if event.type != EventType.PAYMENT_FAILED:
            return

        reason = str(event.payload.get("reason", "payment_failed"))
        async with self.database.transaction() as connection:
            await connection.execute(
                """
                INSERT INTO saga_compensations(order_id, status, reason)
                VALUES($1, 'COMPENSATING', $2)
                ON CONFLICT (order_id) DO UPDATE
                SET status = 'COMPENSATING', reason = EXCLUDED.reason, updated_at = now()
                """,
                event.aggregate_id,
                reason,
            )

        for command in compensation_commands(event):
            await self.producer.publish(command.event_type, command)
            async with self.database.transaction() as connection:
                if command.type == EventType.INVENTORY_RELEASE_REQUESTED:
                    await connection.execute(
                        """
                        UPDATE saga_compensations
                        SET inventory_released = true, updated_at = now()
                        WHERE order_id = $1
                        """,
                        event.aggregate_id,
                    )
                elif command.type == EventType.ORDER_CANCEL_REQUESTED:
                    await connection.execute(
                        """
                        UPDATE saga_compensations
                        SET order_cancelled = true, status = 'COMPENSATED', updated_at = now()
                        WHERE order_id = $1
                        """,
                        event.aggregate_id,
                    )

        saga_compensations_total.labels(reason=reason).inc()


async def run() -> None:
    start_metrics_server(settings.saga_metrics_port)
    database = Database(settings)
    producer = EventProducer(settings)
    await database.connect()
    await producer.start()
    orchestrator = SagaOrchestrator(database, producer)
    consumer = EventConsumer(
        settings=settings,
        topics=[EventType.PAYMENT_FAILED],
        group_id="saga-orchestrator",
        handler=orchestrator.handle_payment_failed,
        producer=producer,
    )
    await consumer.start()
    try:
        await consumer.run_forever()
    finally:
        await consumer.stop()
        await producer.stop()
        await database.close()


if __name__ == "__main__":
    asyncio.run(run())
