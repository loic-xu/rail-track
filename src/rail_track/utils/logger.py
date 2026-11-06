import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any


class JsonFormatter(logging.Formatter):
    """Formats log records into serialized JSON payloads for centralized collectors."""

    def format(self, record: logging.LogRecord) -> str:
        """Serialize a LogRecord into a structured JSON string.

        Args:
            record: The logging event record containing execution metadata.

        Returns:
            A UTF-8 encoded JSON string representation of the log event.
        """
        log_payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "line": record.lineno,
        }

        if record.exc_info:
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
            log_payload["exception"] = record.exc_text

        return json.dumps(log_payload, ensure_ascii=False)


def get_logger(name: str) -> logging.Logger:
    """Factory function providing a standardized JSON logger instance.

    Guarantees idempotency by attaching handlers only when uninitialized,
    and isolates the pipeline logger from the root namespace to prevent duplicate emits.

    Args:
        name: Name of the logger, typically `__name__` of the calling module.

    Returns:
        A pre-configured logging.Logger instance routing structured events to stdout.
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False

    return logger
