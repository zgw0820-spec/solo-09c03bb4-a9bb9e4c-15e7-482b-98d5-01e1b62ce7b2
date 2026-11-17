"""Request execution: deadline accounting, statement cancellation, recovery.

The crucial timeout protocol is::

    1. run the statement as a *shielded* task so a late result can never
       overwrite the timeout receipt;
    2. on deadline, call libpq ``PQcancel`` to ask the server to abort the
       statement;
    3. await the shielded task (the cancellation error) so no coroutine is
       left running against this connection;
    4. prove the backend is IDLE (ROLLBACK + transaction_status check);
    5. only then return the connection to the pool, otherwise discard it
       and build a replacement.

Cancellation and shutdown
-------------------------

Every borrow is wrapped in a :class:`_Lease` that hands the connection and
its borrower slot back **exactly once**, on every exit path: success, SQL
error, timeout, connection loss, request cancellation, and cancellation
arriving in the middle of timeout recovery.  ``execute`` additionally has a
last-resort ``finally`` that discards a lease that is still outstanding, and
stops the statement task, so a cancelled request can never keep a backend
checked out -- otherwise ``close()`` would never see the pool drain.

Transactions are committed *inside* the cursor block on purpose: psycopg's
``async with conn.cursor()`` rolls back any transaction still open when the
block exits, so committing afterwards would silently leave the backend in
an aborted/in-transaction state.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from typing import Any

import psycopg

from .config import Settings
from .errors import ConnectionLost, PoolClosed, QueryTimeout
from .pool import QueryConnectionPool

logger = logging.getLogger("executor")

# Bound on how long recovery waits for a cancelled statement to stop.
_STATEMENT_STOP_TIMEOUT = 5.0


class _Lease:
    """One borrowed connection plus its borrower slot.

    ``release`` / ``discard`` perform the single pool hand-back; both are
    idempotent so recovery code, error handlers and the final safety net can
    call them without coordinating.
    """

    def __init__(self, pool: QueryConnectionPool, conn: psycopg.AsyncConnection):
        self._pool = pool
        self.conn = conn
        self.outcome: str | None = None  # None | "released" | "discarded"

    @property
    def returned(self) -> bool:
        return self.outcome is not None

    async def release(self) -> None:
        if self.returned:
            return
        self.outcome = "released"
        await self._pool.release_healthy(self.conn)

    async def discard(self) -> None:
        if self.returned:
            return
        self.outcome = "discarded"
        await self._pool.discard_and_replace(self.conn)

    async def confirm_and_return(self) -> None:
        """Return only if the backend proves IDLE, otherwise discard."""
        if self.returned:
            return
        if await self._pool.confirm_idle_or_discard(self.conn):
            await self.release()
        else:
            await self.discard()


class QueryExecutor:
    def __init__(self, pool: QueryConnectionPool, settings: Settings) -> None:
        self._pool = pool
        self._settings = settings

    async def execute(self, sql: str, deadline: float) -> dict[str, Any]:
        """Execute one validated read-only statement against the pool.

        A dead-on-arrival backend (e.g. terminated out-of-band while idle)
        is discarded and rebuilt, then the read-only statement is retried
        once within the same total deadline.
        """
        last_conn_error: psycopg.Error | None = None
        for attempt in (1, 2):
            wait_started = time.monotonic()
            # Propagate PoolAcquireTimeout / PoolClosed as 503 receipts.
            # When this raises, nothing was borrowed and no SQL was sent.
            conn = await self._pool.acquire(deadline)
            lease = _Lease(self._pool, conn)
            waited_ms = round((time.monotonic() - wait_started) * 1000, 3)
            acquired_at = time.monotonic()

            async def _run() -> list[tuple[Any, ...]]:
                # Shutdown may have started between borrowing and sending:
                # refuse before BEGIN so no SQL reaches the backend.
                if self._pool.is_closed():
                    raise PoolClosed("服务正在关闭，查询未发送")
                # Server-side backstop, deliberately *after* the request
                # deadline: application cancellation normally wins;
                # statement_timeout only intervenes if PQcancel is delayed.
                backstop_ms = max(
                    1,
                    int(
                        (deadline - time.monotonic()) * 1000
                        + self._settings.statement_timeout_grace_ms
                    ),
                )
                async with conn.cursor() as cur:
                    await cur.execute("BEGIN READ ONLY")
                    await cur.execute(
                        "SELECT set_config('statement_timeout', %s, true)",
                        (str(backstop_ms),),
                    )
                    await cur.execute(sql)
                    rows = await cur.fetchall()
                    # Commit before the cursor block exits, or psycopg's
                    # cursor teardown would implicitly ROLLBACK this txn.
                    await cur.execute("COMMIT")
                    return rows

            task = asyncio.ensure_future(_run())
            try:
                rows = await asyncio.wait_for(
                    asyncio.shield(task),
                    timeout=max(0.0, deadline - time.monotonic()),
                )
            except (asyncio.TimeoutError, TimeoutError):
                # Client deadline: cancel, join, prove IDLE, then release.
                raise await self._recover_timeout(
                    lease, task, waited_ms, acquired_at
                )
            except psycopg.errors.QueryCanceled as exc:
                # The server-side statement_timeout backstop fired first.
                # Same recovery protocol, same 504 receipt.
                logger.warning("statement_timeout backstop fired: %s", exc)
                raise await self._recover_timeout(
                    lease, task, waited_ms, acquired_at, backstop=True
                )
            except PoolClosed:
                # Shutdown began before BEGIN; the backend is untouched.
                await _shielded(lease.confirm_and_return())
                raise
            except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
                # Connection-level failure (backend terminated/restarted):
                # discard it, rebuild via the pool, retry once if budget
                # remains.  Retry is safe because the statement is read-only
                # and never committed anything on a dead connection.
                logger.warning("connection-level error, discarding: %s", exc)
                # Shutdown may start between the failure and the retry.  Never
                # send SQL on a fresh connection during shutdown, and never
                # rebuild one: report the failure instead.
                shutdown = self._pool.is_closed()
                await _shielded(lease.discard())
                last_conn_error = exc
                if shutdown or self._pool.is_closed():
                    raise ConnectionLost(
                        "后端连接在语句执行中失效，服务正在关闭，未重试"
                    ) from exc
                if deadline - time.monotonic() <= 0:
                    raise
                continue
            except psycopg.Error:
                # Statement-level error: roll back, verify IDLE, then reuse.
                await _shielded(lease.confirm_and_return())
                raise
            except BaseException:
                # Includes the request's own cancellation.  Cancel and hand
                # the backend back, shielded from further cancellation.
                await _shielded(self._release_after_abort(lease, task))
                raise
            finally:
                await _stop_task(task)

            elapsed_ms = round((time.monotonic() - acquired_at) * 1000, 3)
            await _shielded(lease.release())
            return {
                "status": "ok",
                "executed": True,
                "wait_ms": waited_ms,
                "execution_ms": elapsed_ms,
                "row_count": len(rows),
                "rows": [self._jsonable(row) for row in rows],
            }

        # Defensive: loop should either return or raise earlier.
        raise last_conn_error  # pragma: no cover

    # ------------------------------------------------------------------ #
    async def _recover_timeout(
        self,
        lease: _Lease,
        task: asyncio.Task,
        waited_ms: float,
        acquired_at: float,
        backstop: bool = False,
    ) -> QueryTimeout:
        """Timeout recovery that completes even if the request is cancelled.

        The whole recovery is shielded: a cancellation arriving mid-recovery
        is delivered to the request afterwards, but the backend is still
        either provably IDLE and returned, or discarded -- never leaked.
        """
        logger.warning(
            "deadline reached (%s), cancelling statement on backend",
            "server backstop" if backstop else "client deadline",
        )
        try:
            cancel_ok, late_rows = await _shielded(
                self._cancel_and_join(lease.conn, task)
            )
        except BaseException:
            # Could not establish that the statement stopped: drop the
            # backend rather than risk handing out a busy connection.
            await _shielded(lease.discard())
            raise
        await _shielded(lease.confirm_and_return())

        elapsed_ms = round((time.monotonic() - acquired_at) * 1000, 3)
        discarded = lease.outcome == "discarded"
        return QueryTimeout(
            "语句执行超过总截止时间，已请求取消"
            + ("；无法确认后端空闲，连接已弃置" if discarded else ""),
            extra={
                "status": "timeout",
                "executed": True,
                "cancelled": cancel_ok,
                "connection_discarded": discarded,
                "wait_ms": waited_ms,
                "execution_ms": elapsed_ms,
                "late_result_ignored": late_rows is not None,
            },
        )

    async def _cancel_and_join(
        self, conn: psycopg.AsyncConnection, task: asyncio.Task
    ) -> tuple[bool, list[tuple] | None]:
        # 1) libpq cancel request.  PQcancel opens a separate connection to
        #    the server and is a blocking socket call; run it off the loop.
        cancel_ok = False
        try:
            await asyncio.to_thread(conn.cancel)
            cancel_ok = True
        except Exception:
            logger.exception("PQcancel failed; connection will be discarded")

        # 2) Wait for the shielded statement task to actually finish so no
        #    coroutine keeps touching this connection.
        late_rows: list[tuple] | None = None
        try:
            late_rows = await asyncio.wait_for(
                task, timeout=_STATEMENT_STOP_TIMEOUT
            )
            # The statement finished before the cancel took effect.  Its
            # result must NOT replace the timeout receipt; audit-log only.
            logger.warning(
                "statement completed before cancel took effect; late result "
                "with %d rows ignored, timeout receipt stands",
                len(late_rows),
            )
        except asyncio.TimeoutError:
            logger.error("statement task did not stop after cancel")
        except psycopg.errors.QueryCanceled:
            logger.info("server confirmed cancellation (QueryCanceled)")
        except psycopg.Error as exc:
            logger.info("statement ended after cancel: %s", exc.sqlstate)
        return cancel_ok, late_rows

    async def _release_after_abort(self, lease: _Lease, task: asyncio.Task) -> None:
        """Give the backend back after the *request* was cancelled.

        ``close()`` waits for every borrowed backend, so a cancelled request
        must never leak its slot.  The statement is cancelled through libpq
        (bounded wait) and the connection is only reused if it proves IDLE;
        otherwise it is discarded.  During shutdown both paths close it.
        """
        if not task.done():
            with contextlib.suppress(Exception):
                await asyncio.to_thread(lease.conn.cancel)
            with contextlib.suppress(BaseException):
                await asyncio.wait_for(task, timeout=_STATEMENT_STOP_TIMEOUT)
        await lease.confirm_and_return()

    @staticmethod
    def _jsonable(row: tuple) -> list[Any]:
        out: list[Any] = []
        for value in row:
            # Common non-JSON types from fault logs.
            if hasattr(value, "isoformat"):
                out.append(value.isoformat())
            elif isinstance(value, (bytes, memoryview)):
                out.append(bytes(value).hex())
            else:
                out.append(value)
        return out


async def _shielded(coro) -> Any:  # noqa: ANN001 - generic awaitable
    """Await ``coro`` to completion even while the caller is being cancelled.

    Further cancellations are recorded by asyncio and surface on the
    caller's next real await; the cleanup itself always finishes, which is
    what keeps a shutdown drain from hanging on a half-returned lease.
    """
    inner = asyncio.ensure_future(coro)
    while not inner.done():
        try:
            await asyncio.shield(inner)
        except asyncio.CancelledError:
            continue
    return inner.result()


async def _stop_task(task: asyncio.Task) -> None:
    """Make sure no statement coroutine outlives its request."""
    if task.done():
        # Retrieve the exception, if any, to avoid "never retrieved" noise.
        with contextlib.suppress(BaseException):
            task.exception()
        return
    task.cancel()
    with contextlib.suppress(BaseException):
        await asyncio.wait_for(task, timeout=_STATEMENT_STOP_TIMEOUT)
