from order_platform.common.events import EventEnvelope, EventType, OrderItem


def test_domain_event_round_trip_preserves_traceparent() -> None:
    item = OrderItem(sku="SKU-RED-CHAIR", quantity=1, unit_price_cents=4999)
    event = EventEnvelope(
        event_type=EventType.ORDER_CREATED,
        aggregate_id="order-123",
        payload={"items": [item.model_dump()]},
        trace_id="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )

    encoded = event.to_json()
    decoded = EventEnvelope.from_json(encoded)

    assert decoded.event_id == event.event_id
    assert decoded.event_type == "order.created"
    assert decoded.trace_id == event.trace_id
    assert decoded.payload["items"][0]["sku"] == "SKU-RED-CHAIR"


def test_domain_event_rejects_unknown_fields() -> None:
    payload = (
        '{"event_id":"evt","event_type":"order.created","aggregate_id":"order-1",'
        '"trace_id":"trace-1","occurred_at":"2026-05-05T16:08:00Z",'
        '"payload":{},"unexpected":true}'
    )

    try:
        EventEnvelope.from_json(payload)
    except ValueError as exc:
        assert "unexpected" in str(exc)
    else:
        raise AssertionError("EventEnvelope should reject unknown fields")
