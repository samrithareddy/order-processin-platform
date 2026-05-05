from __future__ import annotations

import json
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, PositiveInt, field_validator


class EventType(StrEnum):
    ORDER_CREATED = "order.created"
    ORDER_ACCEPTED = "order.accepted"
    ORDER_COMPLETED = "order.completed"
    ORDER_CANCEL_REQUESTED = "order.cancel.requested"
    INVENTORY_RESERVED = "inventory.reserved"
    INVENTORY_RELEASE_REQUESTED = "inventory.release.requested"
    INVENTORY_RELEASED = "inventory.released"
    PAYMENT_COMPLETED = "payment.completed"
    PAYMENT_FAILED = "payment.failed"
    SHIPPING_SCHEDULED = "shipping.scheduled"
    DLQ_MESSAGE = "message.failed"


class Topic(StrEnum):
    ORDER_CREATED = "order.created"
    ORDER_ACCEPTED = "order.accepted"
    ORDER_COMPLETED = "order.completed"
    ORDER_CANCEL_REQUESTED = "order.cancel.requested"
    INVENTORY_RESERVED = "inventory.reserved"
    INVENTORY_RELEASE_REQUESTED = "inventory.release.requested"
    INVENTORY_RELEASED = "inventory.released"
    PAYMENT_COMPLETED = "payment.completed"
    PAYMENT_FAILED = "payment.failed"
    SHIPPING_SCHEDULED = "shipping.scheduled"
    DLQ = "orders.dlq"


class OrderItem(BaseModel):
    sku: str = Field(min_length=1)
    quantity: PositiveInt
    unit_price_cents: PositiveInt


class EventEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: str
    aggregate_id: str
    trace_id: str
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    payload: dict[str, Any]

    @field_validator("occurred_at")
    @classmethod
    def ensure_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value

    @property
    def type(self) -> EventType | str:
        try:
            return EventType(self.event_type)
        except ValueError:
            return self.event_type

    @property
    def order_id(self) -> str:
        return self.aggregate_id

    @property
    def traceparent(self) -> str:
        return self.trace_id

    def child(
        self,
        event_type: EventType | str,
        payload: BaseModel | dict[str, Any],
        *,
        aggregate_id: str | None = None,
    ) -> EventEnvelope:
        return make_event(event_type, aggregate_id or self.aggregate_id, self.trace_id, payload)

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, raw: str | bytes) -> EventEnvelope:
        return cls.model_validate_json(raw)


class OrderCreatedPayload(BaseModel):
    order_id: str
    customer_id: str
    items: list[OrderItem] = Field(min_length=1)
    payment_method_id: str


class OrderAcceptedPayload(BaseModel):
    order_id: str
    customer_id: str
    items: list[OrderItem] = Field(min_length=1)
    total_cents: int
    payment_method_id: str


class InventoryReservedPayload(BaseModel):
    order_id: str
    customer_id: str
    items: list[OrderItem] = Field(min_length=1)
    total_cents: int
    payment_method_id: str
    reservation_id: str


class PaymentCompletedPayload(BaseModel):
    order_id: str
    payment_id: str
    amount_cents: int


class PaymentFailedPayload(BaseModel):
    order_id: str
    reservation_id: str
    reason: str


class ShippingScheduledPayload(BaseModel):
    order_id: str
    shipment_id: str
    status: Literal["SCHEDULED"] = "SCHEDULED"


class InventoryReleaseRequestedPayload(BaseModel):
    order_id: str
    reservation_id: str
    reason: str


class OrderCancelRequestedPayload(BaseModel):
    order_id: str
    reason: str


class DlqPayload(BaseModel):
    original_topic: str
    consumer_group: str
    event: dict[str, Any]
    error: str
    attempts: int


def make_event(
    event_type: EventType | str,
    aggregate_id: str,
    trace_id: str,
    payload: BaseModel | dict[str, Any],
) -> EventEnvelope:
    event_type_value = event_type.value if isinstance(event_type, EventType) else event_type
    body = payload.model_dump(mode="json") if isinstance(payload, BaseModel) else payload
    return EventEnvelope(
        event_type=event_type_value,
        aggregate_id=aggregate_id,
        trace_id=trace_id,
        payload=body,
    )


def topic_for_event(event_type: EventType | str) -> Topic:
    value = event_type.value if isinstance(event_type, EventType) else event_type
    if value == EventType.DLQ_MESSAGE:
        return Topic.DLQ
    return Topic(value)


def encode_event(event: EventEnvelope) -> bytes:
    return event.to_json().encode("utf-8")


def decode_event(payload: bytes | str | dict[str, Any]) -> EventEnvelope:
    if isinstance(payload, dict):
        return EventEnvelope.model_validate(payload)
    return EventEnvelope.model_validate_json(payload)


def event_to_dict(event: EventEnvelope) -> dict[str, Any]:
    return json.loads(event.to_json())


