#!/usr/bin/env python3
"""HTTP-level shutdown drill: real uvicorn process, real SIGTERM.

Boots the app against an ephemeral PostgreSQL (like tests/local_run.py) on a
dedicated port, then:

  * occupies both pooled backends with slow queries;
  * queues a third request behind them;
  * sends SIGTERM to uvicorn;
  * asserts the queued request gets a prompt ``pool_closed`` (503) receipt
    without sending SQL, while in-flight queries finish with their own
    results;
  * asserts the process exits cleanly and the database has no fault_ro
    backends left.

Run:  python tests/test_shutdown_http.py
"""
from __future__ import annotations

import json
import os
import pathlib
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pgserver  # noqa: E402
import psycopg  # noqa: E402

PORT = int(os.environ.get("SHUTDOWN_TEST_PORT", "8099"))
BASE = f"http://127.0.0.1:{PORT}"
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


def post(payload: dict, timeout: float = 30.0):
    req = urllib.request.Request(
        BASE + "/query",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())
    except (urllib.error.URLError, ConnectionError) as exc:
        return -1, {"error": {"code": "connection_failed", "message": str(exc)}}


def get(path: str):
    try:
        with urllib.request.urlopen(BASE + path, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode())
    except (urllib.error.URLError, ConnectionError) as exc:
        return -1, {"error": {"code": "connection_failed", "message": str(exc)}}


def backend_count(admin_dsn: str) -> int:
    with psycopg.connect(admin_dsn, autocommit=True) as c, c.cursor() as cur:
        cur.execute("SELECT count(*) FROM pg_stat_activity "
                    "WHERE usename = 'fault_ro'")
        return cur.fetchone()[0]


def main() -> int:
    data_dir = tempfile.mkdtemp(prefix="shutdown-http-")
    pg = pgserver.get_server(data_dir, cleanup_mode="stop")
    admin_dsn = pg.get_uri()
    with psycopg.connect(admin_dsn, autocommit=True) as c, c.cursor() as cur:
        cur.execute("CREATE DATABASE faultdb")
    from urllib.parse import urlparse, urlunparse

    fault_admin = urlunparse(urlparse(admin_dsn)._replace(path="/faultdb"))
    with psycopg.connect(fault_admin, autocommit=True) as c, c.cursor() as cur:
        cur.execute((ROOT / "db" / "01_init.sql").read_text(encoding="utf-8"))
        cur.execute("ALTER ROLE fault_ro PASSWORD NULL")
    ro = urlunparse(
        urlparse(fault_admin)._replace(
            netloc=urlparse(fault_admin).netloc.replace("postgres", "fault_ro", 1)
        )
    )

    env = dict(os.environ)
    env.update({
        "DATABASE_DSN": ro,
        "POOL_SIZE": "2",
        "PYTHONPATH": str(ROOT) + os.pathsep + env.get("PYTHONPATH", ""),
    })
    log_path = pathlib.Path(data_dir) / "uvicorn.log"
    log_file = log_path.open("w+", encoding="utf-8")
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", str(PORT),
         "--timeout-graceful-shutdown", "20"],
        cwd=str(ROOT), env=env, stdout=log_file, stderr=subprocess.STDOUT,
    )
    try:
        deadline = time.monotonic() + 30
        up = False
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                break
            status, _ = get("/health")
            if status == 200:
                up = True
                break
            time.sleep(0.2)
        check("HTTP 服务启动并健康", up)
        if not up:
            log_file.flush()
            print(log_path.read_text(encoding="utf-8"))
            return 1

        results: dict[str, tuple] = {}

        def slow(marker: str) -> None:
            results[marker] = post(
                {"sql": f"SELECT '{marker}', pg_sleep(2)", "timeout_ms": 12000},
                timeout=25,
            )

        t1 = threading.Thread(target=slow, args=("INFLIGHT1",))
        t2 = threading.Thread(target=slow, args=("INFLIGHT2",))
        t1.start()
        t2.start()
        time.sleep(0.6)

        queued_result: dict[str, tuple] = {}

        def queued() -> None:
            t0 = time.monotonic()
            status, body = post(
                {"sql": "SELECT 'QUEUED-MUST-NOT-RUN'", "timeout_ms": 12000},
                timeout=25,
            )
            queued_result["value"] = (status, body, time.monotonic() - t0)

        tq = threading.Thread(target=queued)
        tq.start()
        time.sleep(0.4)
        check("排队请求尚未返回", "value" not in queued_result)

        t_signal = time.monotonic()
        proc.send_signal(signal.SIGTERM)

        tq.join(timeout=20)
        queued_latency = time.monotonic() - t_signal
        check("排队请求在 SIGTERM 后及时拿到回执 (<3s)",
              "value" in queued_result and queued_latency < 3.0,
              f"latency={queued_latency:.2f}s")
        if "value" in queued_result:
            q_status, q_body, q_elapsed = queued_result["value"]
            check("排队回执 503", q_status == 503, q_body)
            check("排队回执错误码 pool_closed",
                  q_body.get("error", {}).get("code") == "pool_closed", q_body)
            check("排队回执明确未执行", q_body.get("executed") is False, q_body)
            check("排队请求未等待到自己的 SQL 执行完",
                  q_elapsed < 2.0, f"{q_elapsed:.2f}s")

        t1.join(timeout=20)
        t2.join(timeout=20)
        for marker in ("INFLIGHT1", "INFLIGHT2"):
            status, body = results.get(marker, (-1, {}))
            check(f"在途查询 {marker} 成功", status == 200
                  and body.get("status") == "ok", body)
            check(f"在途查询 {marker} 拿到自身结果",
                  status == 200 and body.get("rows", [[None]])[0][0] == marker,
                  body)

        exit_code = proc.wait(timeout=30)
        # uvicorn re-raises the captured signal after a graceful shutdown, so
        # the process legitimately reports SIGTERM (-15 / 143) even when the
        # whole drain completed cleanly.  "0" is accepted for other servers.
        check("uvicorn 关闭后正常退出 (0 或 SIGTERM)",
              exit_code in (0, -signal.SIGTERM, 128 + signal.SIGTERM),
              f"code={exit_code}")
        check("进程退出后无 fault_ro 残留后端",
              backend_count(admin_dsn) == 0,
              backend_count(admin_dsn))

        log_file.flush()
        log = log_path.read_text(encoding="utf-8")
        check("日志确认优雅关闭流程走完",
              "Finished server process" in log and "pool closed" in log,
              log[-2000:])
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
        log_file.close()
        pg.cleanup()

    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
