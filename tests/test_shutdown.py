"""Shutdown / query interleaving tests for the two-connection pool.

These drive the pool and executor directly against an ephemeral PostgreSQL,
so they exercise the exact production shutdown path without the HTTP layer:

* A request queued behind two busy backends receives a *shutdown receipt*
  as soon as ``close()`` starts, promptly, and **no SQL is sent for it**.
* Queries that already borrowed a backend keep their original total
  deadline and finish normally; ``close()`` waits for them.
* Returned connections (healthy return, timeout discard, connection-level
  discard) are closed during shutdown and are **never replaced**.
* ``close()`` returns only after every backend is closed, can be called
  repeatedly/concurrently, and afterwards no connection can be borrowed.
* Normal semantics survive: capacity of two, read-only SQL, timeouts.

Run:  python tests/test_shutdown.py
"""
from __future__ import annotations

import asyncio
import contextlib
import pathlib
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import psycopg  # noqa: E402
import pgserver  # noqa: E402

from app.config import Settings  # noqa: E402
from app.errors import (  # noqa: E402
    ConnectionLost,
    PoolAcquireTimeout,
    PoolClosed,
    QueryTimeout,
)
from app.executor import QueryExecutor  # noqa: E402
from app.pool import QueryConnectionPool  # noqa: E402

PASS = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


async def backend_pids(admin_dsn: str) -> list[int]:
    """Backends of the read-only role, queried synchronously off-loop."""

    def _query() -> list[int]:
        with psycopg.connect(admin_dsn, autocommit=True) as c, c.cursor() as cur:
            cur.execute(
                "SELECT pid FROM pg_stat_activity WHERE usename = 'fault_ro'"
            )
            return [row[0] for row in cur.fetchall()]

    return await asyncio.to_thread(_query)


async def main() -> int:
    data_dir = tempfile.mkdtemp(prefix="shutdown-")
    pg = pgserver.get_server(data_dir, cleanup_mode="stop")
    admin_dsn = pg.get_uri()
    ddl = (ROOT / "db" / "01_init.sql").read_text(encoding="utf-8")
    with psycopg.connect(admin_dsn, autocommit=True) as c, c.cursor() as cur:
        cur.execute("CREATE DATABASE faultdb")
    from urllib.parse import urlparse, urlunparse

    p = urlparse(admin_dsn)
    fault_admin = urlunparse(p._replace(path="/faultdb"))
    with psycopg.connect(fault_admin, autocommit=True) as c, c.cursor() as cur:
        cur.execute(ddl)
        cur.execute("ALTER ROLE fault_ro PASSWORD NULL")
    ro = urlunparse(
        urlparse(fault_admin)._replace(
            netloc=urlparse(fault_admin).netloc.replace("postgres", "fault_ro", 1)
        )
    )
    settings = Settings(database_dsn=ro, pool_size=2)

    # ------------------------------------------------------------------ #
    print("=== A. 关闭时排队请求立即收到关闭回执且不发送 SQL ===")
    pool = QueryConnectionPool(settings)
    await pool.open()
    executor = QueryExecutor(pool, settings)

    started = asyncio.Event()
    sent: list[str] = []

    async def occupied(marker: str) -> dict:
        started.set()
        return await executor.execute(
            f"SELECT '{marker}', pg_sleep(1.2)", time.monotonic() + 8
        )

    busy = [asyncio.ensure_future(occupied("BUSY1")),
            asyncio.ensure_future(occupied("BUSY2"))]
    await asyncio.sleep(0.4)
    pids_before = await backend_pids(admin_dsn)
    check("两条后端被占满", len(pids_before) == 2, pids_before)

    # Third request queues behind both.  Its SQL would be observable via a
    # marker string; it must never reach any backend.
    queued_started = time.monotonic()
    queued = asyncio.ensure_future(
        executor.execute("SELECT 'QUEUED-MUST-NOT-RUN'", time.monotonic() + 8)
    )
    await asyncio.sleep(0.3)
    check("排队请求尚未完成", not queued.done())

    t_close = time.monotonic()
    close_task = asyncio.ensure_future(pool.close())
    try:
        await asyncio.wait_for(asyncio.shield(queued), timeout=2.0)
        queued_err: Exception | None = None
    except Exception as exc:  # noqa: BLE001 - receipt under test
        queued_err = exc
    queued_elapsed = time.monotonic() - queued_started
    check("排队请求在关闭开始后 1s 内返回", queued_elapsed < 1.0,
          f"{queued_elapsed:.2f}s")
    check("排队请求收到 PoolClosed(503)", isinstance(queued_err, PoolClosed),
          repr(queued_err))
    if isinstance(queued_err, PoolClosed):
        check("排队回执明确未执行", queued_err.extra.get("executed") is False,
              queued_err.extra)

    # The two already-running queries must still finish within their budget.
    # While they are still in flight, close() must not have returned yet.
    check("在途查询结束前 close() 仍在等待", not close_task.done())
    results = await asyncio.gather(*busy, return_exceptions=True)
    busy_ok = [r for r in results if isinstance(r, dict)]
    check("已执行查询照常完成", len(busy_ok) == 2, results)
    check("已执行查询拿到自身结果",
          {r["rows"][0][0] for r in busy_ok} == {"BUSY1", "BUSY2"}, results)

    # close() must still be waiting for the query that was in flight at the
    # moment it started; measure that it ends only after they finished.
    await asyncio.wait_for(close_task, timeout=5.0)
    close_elapsed = time.monotonic() - t_close
    check("close() 等到在途查询结束才返回 (>0.3s)", close_elapsed >= 0.3,
          f"{close_elapsed:.2f}s")

    st = await pool.status()
    check("关闭状态可见", st.closed is True, st)
    check("关闭返回时无空闲连接入池", st.idle == 0, st)
    check("关闭返回时占用为 0", st.in_use == 0, st)
    pids_after = await backend_pids(admin_dsn)
    check("关闭返回后数据库侧无 fault_ro 后端", pids_after == [], pids_after)
    check("关闭后无补建 (total_created 未增长)",
          st.total_created == 2, st)

    # No new borrows after close, and close is idempotent.
    post_close = None
    try:
        await pool.acquire(time.monotonic() + 1)
    except PoolClosed as exc:
        post_close = exc
    check("关闭后 acquire 立即拒绝", isinstance(post_close, PoolClosed),
          repr(post_close))
    await asyncio.gather(pool.close(), pool.close())
    check("重复/并发 close 安全", True)

    # A queued acquire at close time is the pure-pool variant of case A.
    pool2 = QueryConnectionPool(settings)
    await pool2.open()
    await pool2.close()
    late = None
    try:
        await pool2.acquire(time.monotonic() + 1)
    except PoolClosed as exc:
        late = exc
    check("新池关闭后不可借出", isinstance(late, PoolClosed), repr(late))

    # ------------------------------------------------------------------ #
    print("=== B. 关闭期间弃置不补建（执行超时路径）===")
    pool3 = QueryConnectionPool(settings)
    await pool3.open()
    ex3 = QueryExecutor(pool3, settings)
    timed_out = asyncio.ensure_future(
        ex3.execute("SELECT pg_sleep(2)", time.monotonic() + 0.3)
    )
    await asyncio.sleep(0.15)
    close3 = asyncio.ensure_future(pool3.close())
    try:
        await asyncio.wait_for(asyncio.shield(timed_out), timeout=8.0)
        receipt = None
    except QueryTimeout as exc:
        receipt = exc
    check("在途查询按原总期限收尾(504)", receipt is not None)
    await asyncio.wait_for(close3, timeout=8.0)
    st3 = await pool3.status()
    check("超时弃置后未补建", st3.total_created == 2, st3)
    check("超时弃置后占用与空闲均为 0", st3.in_use == 0 and st3.idle == 0, st3)
    pids3 = await backend_pids(admin_dsn)
    check("关闭后无残留后端", pids3 == [], pids3)

    # ------------------------------------------------------------------ #
    print("=== C. 关闭期间连接级故障弃置不补建 ===")
    pool4 = QueryConnectionPool(settings)
    await pool4.open()
    ex4 = QueryExecutor(pool4, settings)

    query4 = asyncio.ensure_future(
        ex4.execute("SELECT 'VICTIM', pg_sleep(2)", time.monotonic() + 10)
    )
    await asyncio.sleep(0.3)
    # Start shutdown first: it starts draining immediately and waits for the
    # in-flight query.  Then kill its backend out of band, so the failure is
    # definitively handled *during* shutdown (no timing race).
    close4 = asyncio.ensure_future(pool4.close())
    deadline = time.monotonic() + 2
    while not pool4.is_closed() and time.monotonic() < deadline:
        await asyncio.sleep(0.01)
    check("关闭已开始且查询仍在途", pool4.is_closed() and not query4.done())

    targets = await backend_pids(admin_dsn)
    check("找到后端用于带外终止", len(targets) >= 1, targets)
    def _kill() -> int:
        killed = 0
        with psycopg.connect(admin_dsn, autocommit=True) as c, c.cursor() as cur:
            for pid in targets:
                cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
                killed += int(bool(cur.fetchone()[0]))
        return killed

    killed = await asyncio.to_thread(_kill)
    check("管理端成功终止后端", killed >= 1, killed)

    result4: Exception | None = None
    try:
        await asyncio.wait_for(asyncio.shield(query4), timeout=10.0)
    except Exception as exc:  # noqa: BLE001 - receipt under test
        result4 = exc
    check("关闭中连接失效返回 connection_lost(503)",
          isinstance(result4, ConnectionLost), repr(result4))
    await asyncio.wait_for(close4, timeout=10.0)
    st4 = await pool4.status()
    check("故障弃置已计数", st4.total_discarded >= 1, st4)
    check("关闭中弃置未补建 (total_created 保持 2)", st4.total_created == 2, st4)
    check("关闭后占用与空闲均为 0", st4.in_use == 0 and st4.idle == 0, st4)
    check("关闭后无残留后端", await backend_pids(admin_dsn) == [])

    # ------------------------------------------------------------------ #
    print("=== D. 请求在超时恢复途中被取消也不泄漏连接 ===")
    pool6 = QueryConnectionPool(settings)
    await pool6.open()
    ex6 = QueryExecutor(pool6, settings)
    victim = asyncio.ensure_future(
        ex6.execute("SELECT pg_sleep(3)", time.monotonic() + 0.3)
    )
    await asyncio.sleep(0.5)  # deadline passed; recovery is under way
    victim.cancel()
    with contextlib.suppress(BaseException):
        await victim
    check("被取消的请求已结束", victim.done())
    # The lease must now be back: both slots usable and close() must not hang.
    ok6 = await asyncio.wait_for(
        ex6.execute("SELECT 6", time.monotonic() + 5), timeout=10
    )
    check("取消后仍可借出连接（槽位未泄漏）", ok6["rows"] == [[6]], ok6)
    await asyncio.wait_for(pool6.close(), timeout=10)
    st6 = await pool6.status()
    check("取消后 close() 未悬挂且后端全关",
          st6.in_use == 0 and st6.idle == 0 and await backend_pids(admin_dsn) == [],
          st6)

    # ------------------------------------------------------------------ #
    print("=== E. 正常语义保留 ===")
    pool5 = QueryConnectionPool(settings)
    await pool5.open()
    ex5 = QueryExecutor(pool5, settings)
    st5 = await pool5.status()
    check("容量仍为 2", st5.size == 2 and st5.idle == 2, st5)
    ok = await ex5.execute("SELECT count(*) FROM fault_logs",
                           time.monotonic() + 5)
    check("普通只读查询成功", ok["rows"] == [[7]], ok)
    with psycopg.connect(ro, autocommit=True) as c, c.cursor() as cur:
        cur.execute("SHOW default_transaction_read_only")
        check("会话默认只读", cur.fetchone()[0] == "on")
    try:
        await ex5.execute("SELECT pg_sleep(2)", time.monotonic() + 0.2)
        check("执行超时返回 504", False, "no timeout")
    except QueryTimeout as exc:
        check("执行超时返回 504", exc.extra.get("status") == "timeout", exc.extra)
    # Capacity: occupy both, third must queue and expire, not open a third.
    busy5 = [
        asyncio.ensure_future(
            ex5.execute("SELECT pg_sleep(0.8)", time.monotonic() + 6)
        )
        for _ in range(2)
    ]
    await asyncio.sleep(0.2)
    try:
        await ex5.execute("SELECT 1", time.monotonic() + 0.2)
        check("第三请求排队过期(503)而非新建连接", False, "did not expire")
    except PoolAcquireTimeout:
        check("第三请求排队过期(503)而非新建连接", True)
    await asyncio.gather(*busy5)
    check("并发期间后端数不超过 2", len(await backend_pids(admin_dsn)) <= 2)
    await pool5.close()

    pg.cleanup()
    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
