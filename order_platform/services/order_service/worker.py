from __future__ import annotations

import asyncio

from order_platform.common.config import settings
from order_platform.common.database import Database, record_processed_event
from order_platform.common.events import EventEnvelope, EventType, make_event
from order_platform.common.kafka import EventConsumer, EventProducer
from order_platform.common.metrics import events_published_total, start_metrics_server

SERVICE = "order-service"


def calculate_total_cents(items: list[dict[str, object]]) -> int:
    return sum(int(item["quantity"]) * int(item["unit_price_cents"]) for item in items)


class OrderService:
    def __init__(self, database: Database, producer: EventProducer) -> None:
        self.database = database
        self.producer = producer

    async def handle(self, event: EventEnvelope) -> None:
        if event.type == EventType.ORDER_CREATED:
            await self.create_order(event)
        elif event.type == EventType.PAYMENT_COMPLETED:
            await self.mark_order(event, "COMPLETED")
        elif event.type == EventType.ORDER_CANCEL_REQUESTED:
            await self.mark_order(event, "CANCELLED")

    async def create_order(self, event: EventEnvelope) -> None:
        items = list(event.payload["items"])
        total_cents = calculate_total_cents(items)
        async with self.database.transaction() as connection:
            if not await record_processed_event(connection, SERVICE, event.event_id):
                return
            await connection.execute(
                """
                INSERT INTO orders (
                    order_id, customer_id, payment_method_id, status, total_cents, trace_id
                )
                VALUES ($1, $2, $3, 'ACCEPTED', $4, $5)
                ON CONFLICT (order_id) DO NOTHING
                """,
                event.order_id,
                event.payload["customer_id"],
                event.payload["payment_method_id"],
                total_cents,
                event.trace_id,
            )
            for item in items:
                await connection.execute(
                    """
                    INSERT INTO order_items (order_id, sku, quantity, unit_price_cents)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (order_id, sku) DO NOTHING
                    """,
                    event.order_id,
                    item["sku"],
                    int(item["quantity"]),
                    int(item["unit_price_cents"]),
                )

        accepted = make_event(
            EventType.ORDER_ACCEPTED,
            aggregate_id=event.order_id,
            trace_id=event.trace_id,
            payload={
                "order_id": event.order_id,
                "customer_id": event.payload["customer_id"],
                "items": items,
                "total_cents": total_cents,
                "payment_method_id": event.payload["payment_method_id"],
            },
        )
        await self.producer.publish(EventType.ORDER_ACCEPTED, accepted)
        events_published_total.labels(
            service=SERVICE,
            event_type=accepted.event_type,
            topic=EventType.ORDER_ACCEPTED.value,
        ).inc()

    async def mark_order(self, event: EventEnvelope, status: str) -> None:
        async with self.database.transaction() as connection:
            if not await record_processed_event(connection, SERVICE, event.event_id):
                return
            await connection.execute(
                "UPDATE orders SET status = $1, updated_at = now() WHERE order_id = $2",
                status,
                event.order_id,
            )


async def run() -> None:
    service_settings = settings.model_copy(update={"service_name": SERVICE})
    start_metrics_server(service_settings.order_metrics_port)
    database = Database(service_settings)
    producer = EventProducer(service_settings)
    await database.connect()
    await producer.start()
    service = OrderService(database, producer)
    consumer = EventConsumer(
        service_settings,
        topics=[
            EventType.ORDER_CREATED,
            EventType.PAYMENT_COMPLETED,
            EventType.ORDER_CANCEL_REQUESTED,
        ],
        group_id=SERVICE,
        handler=service.handle,
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
