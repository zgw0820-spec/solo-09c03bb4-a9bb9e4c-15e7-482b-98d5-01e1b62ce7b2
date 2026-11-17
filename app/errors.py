"""Typed application errors mapped to HTTP responses in main.py."""
from __future__ import annotations


class QueryServiceError(Exception):
    """Base class for errors that carry a stable machine-readable code."""

    code = "query_service_error"
    http_status = 500

    def __init__(self, message: str = "", extra: dict | None = None) -> None:
        super().__init__(message)
        self.extra = extra or {}


class InvalidSqlError(QueryServiceError):
    code = "invalid_sql"
    http_status = 400


class InvalidDeadlineError(QueryServiceError):
    code = "invalid_deadline"
    http_status = 422


class PoolAcquireTimeout(QueryServiceError):
    """Raised when no connection became available within the total deadline."""

    code = "acquire_timeout"
    http_status = 503

    def __init__(self, message: str = "", extra: dict | None = None) -> None:
        super().__init__(message, {"executed": False, **(extra or {})})


class PoolClosed(QueryServiceError):
    """Raised when shutdown started before a request could borrow a backend.

    This is a definite "not executed" receipt: the request never sent SQL.
    """

    code = "pool_closed"
    http_status = 503

    def __init__(self, message: str = "", extra: dict | None = None) -> None:
        super().__init__(message, {"executed": False, **(extra or {})})


class ConnectionLost(QueryServiceError):
    """The backend died after the statement was sent, and shutdown began
    before a retry was possible.  No connection is rebuilt."""

    code = "connection_lost"
    http_status = 503

    def __init__(self, message: str = "", extra: dict | None = None) -> None:
        super().__init__(
            message, {"executed": True, "retried": False, **(extra or {})}
        )


class QueryTimeout(QueryServiceError):
    """Raised when the statement did not finish before the total deadline."""

    code = "query_timeout"
    http_status = 504
