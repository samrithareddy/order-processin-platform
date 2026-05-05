from __future__ import annotations

import asyncio
from uuid import NAMESPACE_URL, uuid5

from order_platform.common.config import get_settings
from order_platform.common.database import Database, record_processed_event
from order_platform.common.events import EventEnvelope, EventType, make_event
from order_platform.common.kafka import EventConsumer, EventProducer
from order_platform.common.metrics import start_metrics_server


class PaymentDeclinedError(Exception):
    """Raised by the local payment adapter for deterministic failure scenarios."""


class PaymentProcessor:
    def __init__(self, failure_prefix: str) -> None:
        self.failure_prefix = failure_prefix

    async def charge(self, payment_method_id: str, idempotency_key: str) -> str:
        if payment_method_id.startswith(self.failure_prefix):
            raise PaymentDeclinedError("payment method declined")
        return f"charge-{uuid5(NAMESPACE_URL, idempotency_key)}"


class PaymentService:
    def __init__(
        self,
        database: Database,
        producer: EventProducer,
        processor: PaymentProcessor,
    ) -> None:
        self.database = database
        self.producer = producer
        self.processor = processor

    async def handle(self, event: EventEnvelope) -> None:
        if event.type != EventType.INVENTORY_RESERVED:
            return

        async with self.database.transaction() as connection:
            if not await record_processed_event(connection, "payment-service", event.event_id):
                return

            order_id = event.aggregate_id
            amount_cents = int(event.payload["total_cents"])
            attempt = int(event.payload.get("payment_attempt", 1))
            idempotency_key = f"{order_id}:{attempt}"
            payment = await connection.fetchrow(
                """
                INSERT INTO payments (order_id, idempotency_key, amount_cents, status)
                VALUES ($1, $2, $3, 'PENDING')
                ON CONFLICT (idempotency_key) DO UPDATE
                    SET updated_at = now()
                RETURNING payment_id, status, charge_id, failure_reason
                """,
                order_id,
                idempotency_key,
                amount_cents,
            )

            if payment and payment["status"] == "COMPLETED":
                completed = self._completed_event(event, str(payment["payment_id"]), amount_cents)
                await self.producer.publish(EventType.PAYMENT_COMPLETED, completed)
                return
            if payment and payment["status"] == "FAILED":
                failed = self._failed_event(event, str(payment["failure_reason"]))
                await self.producer.publish(EventType.PAYMENT_FAILED, failed)
                return

            try:
                charge_id = await self.processor.charge(
                    str(event.payload["payment_method_id"]), idempotency_key
                )
            except PaymentDeclinedError as exc:
                reason = str(exc)
                await connection.execute(
                    """
                    UPDATE payments
                    SET status = 'FAILED', failure_reason = $1, updated_at = now()
                    WHERE idempotency_key = $2
                    """,
                    reason,
                    idempotency_key,
                )
                failed = self._failed_event(event, reason)
                await self.producer.publish(EventType.PAYMENT_FAILED, failed)
                return

            await connection.execute(
                """
                UPDATE payments
                SET status = 'COMPLETED', charge_id = $1, updated_at = now()
                WHERE idempotency_key = $2
                """,
                charge_id,
                idempotency_key,
            )
            completed = self._completed_event(event, str(payment["payment_id"]), amount_cents)
            await self.producer.publish(EventType.PAYMENT_COMPLETED, completed)

    def _completed_event(
        self, source: EventEnvelope, payment_id: str, amount_cents: int
    ) -> EventEnvelope:
        return make_event(
            EventType.PAYMENT_COMPLETED,
            aggregate_id=source.aggregate_id,
            trace_id=source.trace_id,
            payload={
                "order_id": source.aggregate_id,
                "payment_id": payment_id,
                "amount_cents": amount_cents,
            },
        )

    def _failed_event(self, source: EventEnvelope, reason: str) -> EventEnvelope:
        return make_event(
            EventType.PAYMENT_FAILED,
            aggregate_id=source.aggregate_id,
            trace_id=source.trace_id,
            payload={
                "order_id": source.aggregate_id,
                "reservation_id": source.payload["reservation_id"],
                "reason": reason,
            },
        )


async def run() -> None:
    settings = get_settings().model_copy(update={"service_name": "payment-service"})
    start_metrics_server(settings.payment_metrics_port)
    database = Database(settings)
    producer = EventProducer(settings)
    service = PaymentService(database, producer, PaymentProcessor(settings.payment_failure_prefix))
    consumer = EventConsumer(
        settings,
        topics=[EventType.INVENTORY_RESERVED],
        group_id="payment-service",
        handler=service.handle,
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
    asyncio.run(run())
