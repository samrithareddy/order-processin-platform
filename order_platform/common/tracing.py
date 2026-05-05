from __future__ import annotations

import secrets


def new_traceparent() -> str:
    """Create a W3C traceparent value for local requests."""
    return f"00-{secrets.token_hex(16)}-{secrets.token_hex(8)}-01"


def current_trace_context(traceparent: str) -> str:
    parts = traceparent.split("-")
    if len(parts) >= 4:
        return parts[1]
    return traceparent


def build_trace_headers(trace_id: str) -> list[tuple[str, bytes]]:
    return [
        ("traceparent", trace_id.encode("utf-8")),
        ("x-trace-id", current_trace_context(trace_id).encode("utf-8")),
    ]


def extract_trace_id(headers: list[tuple[str, bytes]] | None) -> str | None:
    for key, value in headers or []:
        if key in {"traceparent", "x-trace-id"}:
            return value.decode("utf-8") if isinstance(value, bytes) else str(value)
    return None
