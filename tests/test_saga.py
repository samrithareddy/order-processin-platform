from order_platform.common.events import EventEnvelope, EventType
from order_platform.services.saga_orchestrator.worker import compensation_commands


def test_payment_failure_triggers_inventory_release_and_order_cancel() -> None:
    failed = EventEnvelope(
        event_type=EventType.PAYMENT_FAILED,
        aggregate_id="order-1",
        trace_id="trace-1",
        payload={
            "order_id": "order-1",
            "reservation_id": "reservation-1",
            "reason": "processor_declined",
        },
    )

    commands = compensation_commands(failed)

    assert [command.event_type for command in commands] == [
        EventType.INVENTORY_RELEASE_REQUESTED,
        EventType.ORDER_CANCEL_REQUESTED,
    ]
    assert commands[0].payload["reservation_id"] == "reservation-1"
    assert commands[1].payload["reason"] == "processor_declined"
