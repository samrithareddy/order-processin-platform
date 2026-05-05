FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY order_platform ./order_platform

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir .

CMD ["uvicorn", "order_platform.services.api_gateway.app:app", "--host", "0.0.0.0", "--port", "8000"]
