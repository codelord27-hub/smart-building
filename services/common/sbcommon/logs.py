"""Structured JSON logging.

Every line is a single JSON object on stdout with a `severity` field, which
Cloud Logging (GKE logging agent) parses automatically, so logs from all
services can be filtered by `jsonPayload.service` in one place.
"""
import json
import logging
import sys
import time

from .config import POD_NAME, SERVICE_NAME

_RESERVED = set(vars(logging.LogRecord("", 0, "", 0, "", (), None)).keys()) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "severity": record.levelname,
            "message": record.getMessage(),
            "service": SERVICE_NAME,
            "pod": POD_NAME,
            "logger": record.name,
            "time": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def setup_logging(level: str = "INFO") -> logging.Logger:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
    # uvicorn access logs are replaced by our own request log + metrics
    logging.getLogger("uvicorn.access").disabled = True
    logging.getLogger("httpx").setLevel(logging.WARNING)      # no log line per outgoing request
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    for name in ("uvicorn", "uvicorn.error"):
        lg = logging.getLogger(name)
        lg.handlers[:] = []
        lg.propagate = True
    return logging.getLogger(SERVICE_NAME)
