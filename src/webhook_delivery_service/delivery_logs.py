"""One JSON line per delivery event, on the process's standard output."""

import json
import logging
import sys
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import TextIO
from uuid import UUID

_logger = logging.getLogger("webhook_delivery_service.delivery_events")


class _EventFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(
            {
                "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
                "level": record.levelname,
                "event": record.getMessage(),
                **record.delivery_fields,
            }
        )


@contextmanager
def delivery_log_output(stream: TextIO | None = None) -> Generator[None]:
    """Install one application handler without changing Uvicorn or root logging."""
    if any(isinstance(h.formatter, _EventFormatter) for h in _logger.handlers):
        yield
        return

    handler = logging.StreamHandler(stream if stream is not None else sys.stdout)
    handler.setFormatter(_EventFormatter())
    previous_level, previous_propagate = _logger.level, _logger.propagate
    _logger.setLevel(logging.INFO)
    _logger.propagate = False
    _logger.addHandler(handler)
    try:
        yield
    finally:
        _logger.removeHandler(handler)
        handler.close()
        _logger.setLevel(previous_level)
        _logger.propagate = previous_propagate


def log_event(
    component: str,
    event: str,
    delivery_id: UUID,
    *,
    message_id: UUID | None = None,
    attempt_count: int | None = None,
    status: str | None = None,
    status_code: int | None = None,
    next_attempt_at: datetime | None = None,
    reason: str | None = None,
    level: int = logging.INFO,
) -> None:
    # Only pass known operational fields. Never log request bodies, destination
    # URLs, idempotency keys, or exception strings (which may contain a URL).
    fields = {
        "component": component,
        "delivery_id": str(delivery_id),
        "message_id": str(message_id) if message_id is not None else None,
        "attempt_count": attempt_count,
        "status": status,
        "status_code": status_code,
        "next_attempt_at": next_attempt_at.isoformat() if next_attempt_at else None,
        "reason": reason,
    }
    _logger.log(
        level,
        event,
        extra={
            "delivery_fields": {
                key: value for key, value in fields.items() if value is not None
            }
        },
    )
