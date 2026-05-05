from __future__ import annotations

import asyncio
from uuid import uuid4

from order_platform.common.config import Settings, get_settings
from order_platform.common.database import Database, record_processed_event
from order_platform.common.events import EventEnvelope, EventType, make_event
from order_platform.common.kafka import EventConsumer, EventProducer
from order_platform.common.metrics import reserved_stock_units, start_metrics_server


class InventoryService:
    def __init__(self, settings: Settings, database: Database, producer: EventProducer) -> None:
        self.settings = settings
        self.database = database
        self.producer = producer

    async def handle(self, event: EventEnvelope) -> None:
        if event.type == EventType.ORDER_ACCEPTED:
            await self.reserve(event)
        elif event.type == EventType.INVENTORY_RELEASE_REQUESTED:
            await self.release(event)

    async def reserve(self, event: EventEnvelope) -> None:
        reservation_id = str(uuid4())
        async with self.database.transaction() as connection:
            if not await record_processed_event(
                connection,
                self.settings.service_name,
                event.event_id,
            ):
                return

            for item in event.payload["items"]:
                row = await connection.fetchrow(
                    "SELECT available, reserved FROM inventory WHERE sku = $1 FOR UPDATE",
                    item["sku"],
                )
                quantity = int(item["quantity"])
                if row is None or int(row["available"]) < quantity:
                    raise ValueError(f"insufficient inventory for {item['sku']}")
                await connection.execute(
                    """
                    UPDATE inventory
                    SET available = available - $2,
                        reserved = reserved + $2,
                        updated_at = now()
                    WHERE sku = $1
                    """,
                    item["sku"],
                    quantity,
                )
                await connection.execute(
                    """
                    INSERT INTO inventory_reservations(
                        order_id, reservation_id, sku, quantity, status
                    )
                    VALUES ($1, $2, $3, $4, 'RESERVED')
                    ON CONFLICT (order_id, sku) DO UPDATE
                    SET reservation_id = EXCLUDED.reservation_id,
                        quantity = EXCLUDED.quantity,
                        status = 'RESERVED',
                        updated_at = now()
                    """,
                    event.aggregate_id,
                    reservation_id,
                    item["sku"],
                    quantity,
                )
                reserved_stock_units.labels(sku=item["sku"]).set(int(row["reserved"]) + quantity)

        reserved = make_event(
            EventType.INVENTORY_RESERVED,
            aggregate_id=event.aggregate_id,
            trace_id=event.trace_id,
            payload={
                **event.payload,
                "reservation_id": reservation_id,
            },
        )
        await self.producer.publish(EventType.INVENTORY_RESERVED, reserved)

    async def release(self, event: EventEnvelope) -> None:
        async with self.database.transaction() as connection:
            if not await record_processed_event(
                connection,
                self.settings.service_name,
                event.event_id,
            ):
                return

            rows = await connection.fetch(
                """
                SELECT sku, quantity, status
                FROM inventory_reservations
                WHERE order_id = $1 AND reservation_id = $2
                FOR UPDATE
                """,
                event.aggregate_id,
                event.payload["reservation_id"],
            )
            for row in rows:
                if row["status"] == "RELEASED":
                    continue
                current = await connection.fetchrow(
                    "SELECT reserved FROM inventory WHERE sku = $1 FOR UPDATE",
                    row["sku"],
                )
                await connection.execute(
                    """
                    UPDATE inventory
                    SET available = available + $2,
                        reserved = GREATEST(reserved - $2, 0),
                        updated_at = now()
                    WHERE sku = $1
                    """,
                    row["sku"],
                    int(row["quantity"]),
                )
                if current is not None:
                    reserved_stock_units.labels(sku=row["sku"]).set(
                        max(int(current["reserved"]) - int(row["quantity"]), 0)
                    )
            await connection.execute(
                """
                UPDATE inventory_reservations
                SET status = 'RELEASED', updated_at = now()
                WHERE order_id = $1 AND reservation_id = $2
                """,
                event.aggregate_id,
                event.payload["reservation_id"],
            )

        released = make_event(
            EventType.INVENTORY_RELEASED,
            aggregate_id=event.aggregate_id,
            trace_id=event.trace_id,
            payload={
                "order_id": event.aggregate_id,
                "reservation_id": event.payload["reservation_id"],
                "reason": event.payload.get("reason", "compensation"),
            },
        )
        await self.producer.publish(EventType.INVENTORY_RELEASED, released)


async def main() -> None:
    service_settings = get_settings().model_copy(update={"service_name": "inventory-service"})
    start_metrics_server(service_settings.inventory_metrics_port)
    database = Database(service_settings)
    producer = EventProducer(service_settings)
    await database.connect()
    await producer.start()

    service = InventoryService(service_settings, database, producer)
    consumer = EventConsumer(
        service_settings,
        [EventType.ORDER_ACCEPTED, EventType.INVENTORY_RELEASE_REQUESTED],
        group_id="inventory-service",
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
    asyncio.run(main())
