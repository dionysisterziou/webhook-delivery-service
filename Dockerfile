FROM python:3.14-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src

RUN python -m pip install --no-cache-dir .

COPY migrations ./migrations
COPY alembic.ini ./

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "webhook_delivery_service.main:app", "--host", "0.0.0.0", "--port", "8000"]