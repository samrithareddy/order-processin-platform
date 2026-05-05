from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    service_name: str = Field(default="order-platform", alias="SERVICE_NAME")
    environment: str = Field(default="local", alias="ENVIRONMENT")
    kafka_bootstrap_servers: str = Field(default="localhost:9092", alias="KAFKA_BOOTSTRAP_SERVERS")
    database_url: str = Field(
        default="postgresql://orders:orders@localhost:5432/orders",
        alias="DATABASE_URL",
    )
    auth_token: str = Field(default="local-dev-token", alias="AUTH_TOKEN")
    api_rate_limit_per_minute: int = Field(default=120, alias="API_RATE_LIMIT_PER_MINUTE")
    retry_attempts: int = Field(default=3, alias="RETRY_ATTEMPTS")
    retry_base_delay_seconds: float = Field(default=0.25, alias="RETRY_BASE_DELAY_SECONDS")
    metrics_port: int = Field(default=9000, alias="METRICS_PORT")
    payment_failure_prefix: str = Field(default="fail_", alias="PAYMENT_FAILURE_PREFIX")

    api_gateway_metrics_port: int = Field(default=8001, alias="API_GATEWAY_METRICS_PORT")
    order_metrics_port: int = Field(default=8010, alias="ORDER_METRICS_PORT")
    inventory_metrics_port: int = Field(default=8020, alias="INVENTORY_METRICS_PORT")
    payment_metrics_port: int = Field(default=8030, alias="PAYMENT_METRICS_PORT")
    shipping_metrics_port: int = Field(default=8040, alias="SHIPPING_METRICS_PORT")
    saga_metrics_port: int = Field(default=8050, alias="SAGA_METRICS_PORT")
    dlq_metrics_port: int = Field(default=8060, alias="DLQ_METRICS_PORT")

    dlq_topic: str = Field(default="orders.dlq", alias="DLQ_TOPIC")


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
