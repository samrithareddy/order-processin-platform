from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Request, status
from pydantic import BaseModel, Field

from order_platform.common.config import get_settings
from order_platform.common.events import EventType, make_event
from order_platform.common.kafka import EventProducer
from order_platform.common.metrics import events_published_total, start_metrics_server
from order_platform.common.tracing import new_traceparent


class OrderItemRequest(BaseModel):
    sku: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    unit_price_cents: int = Field(gt=0)


class CreateOrderRequest(BaseModel):
    customer_id: str = Field(min_length=1)
    items: list[OrderItemRequest] = Field(min_length=1)
    payment_method_id: str = Field(min_length=1)


class CreateOrderResponse(BaseModel):
    order_id: str
    status: str
    trace_id: str


class InMemoryRateLimiter:
    def __init__(self, max_requests: int, window_seconds: int = 60) -> None:
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._requests: dict[str, list[float]] = {}
        self._lock = asyncio.Lock()

    async def check(self, client_id: str) -> None:
        now = time.monotonic()
        cutoff = now - self.window_seconds
        async with self._lock:
            attempts = [ts for ts in self._requests.get(client_id, []) if ts > cutoff]
            if len(attempts) >= self.max_requests:
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail="rate limit exceeded",
                )
            attempts.append(now)
            self._requests[client_id] = attempts


settings = get_settings()
producer = EventProducer(settings)
rate_limiter = InMemoryRateLimiter(settings.api_rate_limit_per_minute)


async def require_auth(authorization: Annotated[str | None, Header()] = None) -> None:
    if authorization != f"Bearer {settings.auth_token}":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid token")


@asynccontextmanager
async def lifespan(_: FastAPI):
    await producer.start()
    start_metrics_server(settings.api_gateway_metrics_port)
    try:
        yield
    finally:
        await producer.stop()


app = FastAPI(title="Order Processing API Gateway", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/orders", response_model=CreateOrderResponse, dependencies=[Depends(require_auth)])
async def create_order(request: Request, payload: CreateOrderRequest) -> CreateOrderResponse:
    client_id = request.client.host if request.client else "unknown"
    await rate_limiter.check(client_id)

    order_id = str(uuid4())
    trace_id = (
        request.headers.get("traceparent")
        or request.headers.get("x-trace-id")
        or new_traceparent()
    )
    event = make_event(
        EventType.ORDER_CREATED,
        aggregate_id=order_id,
        trace_id=trace_id,
        payload={
            "order_id": order_id,
            "customer_id": payload.customer_id,
            "items": [item.model_dump() for item in payload.items],
            "payment_method_id": payload.payment_method_id,
        },
    )
    await producer.publish(EventType.ORDER_CREATED, event)
    events_published_total.labels(
        service="api-gateway",
        event_type=event.event_type,
        topic=EventType.ORDER_CREATED.value,
    ).inc()
    return CreateOrderResponse(order_id=order_id, status="PENDING", trace_id=trace_id)
