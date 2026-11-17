"""A fixed-size pool of **two** read-only PostgreSQL connections.

Design goals, mapped to the operational requirements:

* ``POOL_SIZE`` (default 2) physical connections at most.  A bounded
  semaphore serialises requests so a third request *waits* instead of
  opening another backend.
* Waiting for a free connection consumes the same deadline budget as
  execution (:meth:`QueryConnectionPool.acquire` takes an absolute
  monotonic deadline).  If the deadline passes while queued, no statement
  is ever sent on any connection.
* After a timeout the handler must prove the backend is idle again
  (``transaction_status == IDLE`` after a successful ``ROLLBACK``) before
  the connection is returned to the pool.  If that cannot be confirmed the
  connection is closed and discarded, and a replacement is built so the
  pool keeps its capacity of two.
* Every connection is opened in a read-only, read-committed session.

Shutdown protocol
-----------------

``close()`` is a graceful drain, not a snapshot close:

1. It stops accepting new borrowers immediately (``_closed`` + a wake-up
   event).  Requests still queued on the semaphore are woken at once and
   receive :class:`~app.errors.PoolClosed`; they never reach a backend and
   never send SQL.
2. Queries that already borrowed a connection are *not* interrupted: they
   keep their original total deadline and finish normally.
3. Connections returned (healthy or discarded) while closed are closed and
   removed; no replacement is ever built after shutdown starts.
4. ``close()`` returns only once every physical backend is closed.  It is
   idempotent and safe to call concurrently from several shutdown hooks.
5. After close begins, :meth:`acquire` can never hand out a connection.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass

from psycopg import AsyncConnection
from psycopg.pq import TransactionStatus

from .config import Settings
from .errors import PoolAcquireTimeout, PoolClosed

logger = logging.getLogger("querypool")


@dataclass
class PoolStatus:
    size: int
    idle: int
    in_use: int
    total_created: int
    total_discarded: int
    closed: bool


class QueryConnectionPool:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        # One slot per *borrower*.  Acquiring a slot guarantees the right to
        # use one physical backend, even if that backend must be (re)built.
        self._slots = asyncio.BoundedSemaphore(settings.pool_size)
        self._free: asyncio.Queue[AsyncConnection] = asyncio.Queue()
        # Every physical backend currently tracked (idle + in use).  Used to
        # know when shutdown has drained *all* connections.
        self._all: set[AsyncConnection] = set()
        self._closed = False
        # Set at the start of shutdown: wakes every semaphore waiter.
        self._closed_event = asyncio.Event()
        # Set once shutdown has started and ``_all`` is empty.
        self._drained = asyncio.Event()
        # Serialises connection creation against close() so a backend can
        # never be created after (or race with) the drain.
        self._lock = asyncio.Lock()
        self._total_created = 0
        self._total_discarded = 0

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    async def open(self) -> None:
        """Eagerly establish the full pool so failures surface at startup."""
        async with self._lock:
            if self._closed:
                raise RuntimeError("连接池已关闭，不能重新打开")
            for _ in range(self._settings.pool_size):
                conn = await self._make_connection()
                self._free.put_nowait(conn)
        logger.info(
            "pool opened: size=%d created=%d",
            self._settings.pool_size,
            self._total_created,
        )

    async def close(self) -> None:
        """Stop lending, drain in-flight borrowers, then close everything.

        Idempotent and concurrent-safe: every caller waits until all
        backends are closed before returning.  Called after
        :meth:`start_draining` it only has to drain what is left.
        """
        # Stop intake before the first await: a request that already holds a
        # borrower slot but has not yet taken a connection must not be handed
        # one between here and the lock acquisition.
        if not self._closed:
            self._closed = True
            self._closed_event.set()

        async with self._lock:
            # Wake every queued borrower: they get a definite "not executed"
            # receipt instead of waiting for a connection that never comes.
            self._closed_event.set()
            while True:
                try:
                    conn = self._free.get_nowait()
                except asyncio.QueueEmpty:
                    break
                await self._retire_locked(conn)
            self._signal_drained_locked()
            if self._all:
                logger.info(
                    "pool closing: idle backends closed, waiting for %d in-use",
                    len(self._all),
                )

        # Waiting happens outside the lock: in-flight handlers need the lock
        # to return or discard their connection.
        await self._drained.wait()
        logger.info("pool closed: all backends closed")

    def start_draining(self) -> None:
        """Stop lending immediately; safe to call from a signal handler.

        New and queued requests are refused at once without touching a
        backend, while queries already executing finish under their own
        deadline.  ``close()`` performs the actual draining and closing.

        Only a plain flag is written here; the wake-up of queued borrowers
        is handed to the event loop, so a signal arriving mid-loop cannot
        interleave with asyncio internals.  Safe to call repeatedly.
        """
        if self._closed:
            return
        self._closed = True
        logger.info("pool draining: no new connections will be lent")
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            self._closed_event.set()
        else:
            loop.call_soon_threadsafe(self._closed_event.set)

    async def status(self) -> PoolStatus:
        idle = self._free.qsize()
        return PoolStatus(
            size=self._settings.pool_size,
            idle=idle,
            in_use=max(0, len(self._all) - idle),
            total_created=self._total_created,
            total_discarded=self._total_discarded,
            closed=self._closed,
        )

    def is_closed(self) -> bool:
        """True once shutdown has begun; no new statement should be sent."""
        return self._closed

    # ------------------------------------------------------------------ #
    # Connection factory
    # ------------------------------------------------------------------ #
    async def _make_connection(self) -> AsyncConnection:
        """Open a fresh connection pinned to a read-only session."""
        # Start in autocommit so the session SETs below do not leave an
        # implicit transaction open (psycopg opens one on the first command
        # of a non-autocommit connection).  Switch back afterwards so every
        # request manages explicit BEGIN/COMMIT itself.
        conn = await AsyncConnection.connect(
            self._settings.database_dsn,
            connect_timeout=self._settings.connect_timeout,
            autocommit=True,
        )
        try:
            # Session-level safety net: even an accidental COMMIT cannot write.
            await conn.execute("SET default_transaction_read_only = on")
            await conn.execute(
                "SET default_transaction_isolation = 'read committed'"
            )
            # Async connections expose a setter coroutine, not a property.
            await conn.set_autocommit(False)
        except BaseException:
            # Never track a half-initialised connection.
            await self._safe_close(conn)
            raise
        assert conn.info.transaction_status == TransactionStatus.IDLE
        self._all.add(conn)
        self._total_created += 1
        logger.info(
            "connection created (backends=%d total_created=%d)",
            len(self._all),
            self._total_created,
        )
        return conn

    async def _safe_close(self, conn: AsyncConnection) -> None:
        with contextlib.suppress(Exception):
            await conn.close()

    @staticmethod
    def _is_idle(conn: AsyncConnection) -> bool:
        """True only when the backend has no open/in-progress transaction."""
        return (not conn.closed) and (
            conn.info.transaction_status == TransactionStatus.IDLE
        )

    def _signal_drained_locked(self) -> None:
        if self._closed and not self._all:
            self._drained.set()

    async def _retire_locked(self, conn: AsyncConnection) -> None:
        """Close an orderly-returned connection; never counts as a fault."""
        await self._safe_close(conn)
        self._all.discard(conn)
        self._signal_drained_locked()

    async def _discard_locked(self, conn: AsyncConnection) -> None:
        """Close an unusable connection and account for the fault."""
        await self._safe_close(conn)
        self._all.discard(conn)
        self._total_discarded += 1
        self._signal_drained_locked()

    # ------------------------------------------------------------------ #
    # Acquire / release
    # ------------------------------------------------------------------ #
    async def acquire(self, deadline: float) -> AsyncConnection:
        """Reserve one connection, waiting at most until ``deadline``.

        Time spent here counts against the request's total deadline.  On
        timeout the semaphore slot is released and *nothing has been sent
        to the database* for this request.  If shutdown starts while this
        request is queued, it is woken immediately and raises
        :class:`PoolClosed` without touching a backend.
        """
        if self._closed:
            raise PoolClosed("服务正在关闭，未借出连接，语句未执行")

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise PoolAcquireTimeout("总截止时间已在排队前耗尽，语句未执行")

        await self._acquire_slot(remaining)
        try:
            return await self._take_idle_or_build()
        except BaseException:
            # Any failure to obtain a physical backend frees the borrower
            # slot so the pool cannot deadlock after a failed rebuild.
            self._slots.release()
            raise

    async def _acquire_slot(self, remaining: float) -> None:
        """Wait for a borrower slot, a shutdown signal, or the deadline.

        Exactly one of three outcomes: slot held (ownership passed to the
        caller), :class:`PoolClosed`, or :class:`PoolAcquireTimeout`.  On
        every non-success exit -- including the caller being cancelled while
        waiting -- no slot is left held.
        """
        slot_task = asyncio.ensure_future(self._slots.acquire())
        closed_task = asyncio.ensure_future(self._closed_event.wait())
        handed_over = False
        try:
            await asyncio.wait(
                {slot_task, closed_task},
                timeout=remaining,
                return_when=asyncio.FIRST_COMPLETED,
            )
            got_slot = (
                slot_task.done()
                and not slot_task.cancelled()
                and slot_task.exception() is None
            )

            # Shutdown wins over a simultaneously expiring deadline: the
            # caller must receive a definite "not executed" receipt, promptly.
            if self._closed:
                raise PoolClosed("服务正在关闭，排队请求已取消，语句未执行")

            if not got_slot:
                raise PoolAcquireTimeout(
                    "等待连接超过总截止时间，排队期间已过期，语句未执行"
                )

            handed_over = True
        finally:
            if not handed_over and (
                slot_task.done()
                and not slot_task.cancelled()
                and slot_task.exception() is None
            ):
                # The semaphore granted us a slot while we are unwinding:
                # give it back so the pool cannot shrink permanently.
                self._slots.release()
            for task in (slot_task, closed_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(slot_task, closed_task, return_exceptions=True)

    async def _take_idle_or_build(self) -> AsyncConnection:
        # Holding the lock keeps close() from draining between our
        # "is shutdown starting?" check and the hand-off of a backend.
        async with self._lock:
            if self._closed:
                raise PoolClosed("服务正在关闭，未借出连接，语句未执行")

            if self._free.qsize() > 0:
                conn = self._free.get_nowait()
            else:
                # Pool shrank after an earlier discarded connection: rebuild
                # lazily within the caller's remaining deadline budget.
                logger.info("no idle backend; lazily rebuilding within pool")
                conn = await self._make_connection()

            if self._closed:
                # Shutdown began while we were building/fetching: close the
                # backend instead of lending it, so no SQL is ever sent.
                await self._retire_locked(conn)
                raise PoolClosed("服务正在关闭，未借出连接，语句未执行")

            if not self._is_idle(conn):
                # An idle backend turned stale; replace it before handing out.
                logger.warning(
                    "idle connection not healthy (closed=%s tx=%s), replacing",
                    conn.closed,
                    conn.info.transaction_status,
                )
                await self._discard_locked(conn)
                if self._closed:
                    raise PoolClosed("服务正在关闭，未借出连接，语句未执行")
                conn = await self._make_connection()
            return conn

    async def release_healthy(self, conn: AsyncConnection) -> None:
        """Return a connection that has been confirmed IDLE to the pool.

        During shutdown the connection is closed instead of being pooled;
        no replacement is built.
        """
        async with self._lock:
            try:
                if self._closed:
                    await self._retire_locked(conn)
                elif self._is_idle(conn):
                    self._free.put_nowait(conn)
                else:
                    # Never put an unverified connection back; rebuild instead.
                    await self._discard_and_rebuild_locked(conn)
            finally:
                self._slots.release()

    async def discard_and_replace(self, conn: AsyncConnection) -> None:
        """Drop an unverifiable connection and build its replacement.

        The replacement is built *before* the slot is released so the pool
        usually offers the full capacity of two healthy backends afterwards.
        If the replacement cannot be built the slot is released anyway and
        the pool temporarily shrinks; the next acquire lazily restores it.
        During shutdown the connection is simply closed -- a replacement is
        never built once ``close()`` has begun.
        """
        async with self._lock:
            try:
                await self._discard_and_rebuild_locked(conn)
            finally:
                self._slots.release()

    async def _discard_and_rebuild_locked(self, conn: AsyncConnection) -> None:
        logger.warning(
            "discarding connection (closed=%s tx=%s)%s",
            conn.closed,
            conn.info.transaction_status,
            " during shutdown; not rebuilding" if self._closed else " and rebuilding",
        )
        await self._discard_locked(conn)
        if self._closed:
            return
        try:
            fresh = await self._make_connection()
            self._free.put_nowait(fresh)
        except Exception:
            logger.exception("failed to rebuild discarded connection; "
                             "pool will lazily rebuild on next acquire")

    # ------------------------------------------------------------------ #
    # Timeout recovery primitives used by the request handler
    # ------------------------------------------------------------------ #
    async def confirm_idle_or_discard(self, conn: AsyncConnection) -> bool:
        """Best-effort proof that no statement is running anymore.

        Returns ``True`` only when the backend reports the IDLE transaction
        state.  Any failure means the connection must be discarded.
        """
        try:
            if conn.closed:
                return False
            status = conn.info.transaction_status
            if status != TransactionStatus.IDLE:
                # A cancelled statement leaves an aborted transaction;
                # ROLLBACK both ends it and proves the backend responds.
                await conn.execute("ROLLBACK")
                status = conn.info.transaction_status
            return (not conn.closed) and status == TransactionStatus.IDLE
        except Exception:
            logger.exception("could not confirm connection idle")
            return False
