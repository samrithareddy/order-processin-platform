from __future__ import annotations

import asyncio
from uuid import NAMESPACE_URL, uuid5

from order_platform.common.config import get_settings
from order_platform.common.database import Database, record_processed_event
from order_platform.common.events import EventEnvelope, EventType, make_event
from order_platform.common.kafka import EventConsumer, EventProducer
from order_platform.common.metrics import events_published_total, start_metrics_server


class ShippingService:
    def __init__(self, database: Database, producer: EventProducer) -> None:
        self.database = database
        self.producer = producer

    async def schedule_shipping(self, event: EventEnvelope) -> None:
        order_id = event.aggregate_id
        shipment_id = str(uuid5(NAMESPACE_URL, f"shipment:{order_id}"))

        async with self.database.transaction() as connection:
            if not await record_processed_event(connection, "shipping-service", event.event_id):
                return
            await connection.execute(
                """
                INSERT INTO shipments (shipment_id, order_id, status, trace_id)
                VALUES ($1, $2, 'SCHEDULED', $3)
                ON CONFLICT (order_id)
                DO UPDATE SET status = 'SCHEDULED', updated_at = now()
                """,
                shipment_id,
                order_id,
                event.trace_id,
            )

        scheduled = make_event(
            EventType.SHIPPING_SCHEDULED,
            aggregate_id=order_id,
            trace_id=event.trace_id,
            payload={"order_id": order_id, "shipment_id": shipment_id, "status": "SCHEDULED"},
        )
        await self.producer.publish(EventType.SHIPPING_SCHEDULED, scheduled)
        events_published_total.labels(
            service="shipping-service",
            event_type=scheduled.event_type,
            topic=EventType.SHIPPING_SCHEDULED.value,
        ).inc()


async def main() -> None:
    settings = get_settings().model_copy(update={"service_name": "shipping-service"})
    start_metrics_server(settings.shipping_metrics_port)

    database = Database(settings)
    producer = EventProducer(settings)
    service = ShippingService(database, producer)
    consumer = EventConsumer(
        settings=settings,
        topics=[EventType.PAYMENT_COMPLETED],
        group_id="shipping-service",
        handler=service.schedule_shipping,
        producer=producer,
    )

    await database.connect()
    await producer.start()
    await consumer.start()
    try:
        await consumer.run_forever()
    finally:
        await consumer.stop()
        await producer.stop()
        await database.close()


if __name__ == "__main__":
    asyncio.run(main())
