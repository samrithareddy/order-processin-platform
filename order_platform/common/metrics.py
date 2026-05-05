from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, start_http_server

events_published_total = Counter(
    "order_platform_events_published_total",
    "Events published successfully.",
    ["service", "event_type", "topic"],
)

events_processed_total = Counter(
    "order_platform_events_processed_total",
    "Events processed successfully.",
    ["service", "event_type"],
)

events_retried_total = Counter(
    "order_platform_events_retried_total",
    "Event processing retries.",
    ["service", "event_type"],
)

events_failed_total = Counter(
    "order_platform_events_failed_total",
    "Events that failed processing.",
    ["service", "event_type"],
)

events_dlq_total = Counter(
    "order_platform_events_dlq_total",
    "Events sent to or read from the dead-letter topic.",
    ["service", "source_topic"],
)

event_processing_seconds = Histogram(
    "order_platform_event_processing_seconds",
    "Event processing duration in seconds.",
    ["service", "event_type"],
)

reserved_stock_units = Gauge(
    "order_platform_reserved_stock_units",
    "Units currently reserved.",
    ["sku"],
)


def start_metrics_server(port: int) -> None:
    start_http_server(port)
