# 机房设备故障日志只读查询 API

值班分析员共用的**只读 SQL 入口**：固定两条 PostgreSQL 连接，提交单条
`SELECT` 与**总截止时间**，排队等待也计入预算；超时立即取消数据库语句，
确认事务结束才复用连接，无法确认则弃置连接并补建；迟到结果不会改写超时
回执。后续查询始终拿到属于自己的结果。

停服时先停止接单：排队请求立即收到 `pool_closed` 回执且不发送 SQL，已执行
的查询按原总期限收尾，关闭开始后归还或弃置都不再补建；`close()` 返回时
所有后端均已关闭，重复关闭安全，随后无法再借出连接。

## 技术栈

- FastAPI（Swagger UI：`/docs`，ReDoc：`/redoc`）
- psycopg 3（异步连接 + libpq `PQcancel`）
- PostgreSQL 16
- Docker Compose 编排，预置 7 条故障日志与只读角色 `fault_ro`

## 快速开始（Docker Compose）

```bash
docker compose up --build
# API:    http://localhost:8000
# Swagger: http://localhost:8000/docs
```

自测（另开终端，容器内或本机均可）：

```bash
docker compose exec api python -m tests.selftest
```

## 接口

### `POST /query`

```json
{
  "sql": "SELECT id, device_id, severity FROM fault_logs LIMIT 5",
  "timeout_ms": 2000
}
```

`timeout_ms` 是**包含排队时间**的总预算（单调时钟），从请求进入即计时。

| 场景 | HTTP | 回执要点 |
| --- | --- | --- |
| 成功 | 200 | `status=ok`，`rows` 为本次查询自身结果，附 `wait_ms`/`execution_ms` |
| 执行超时 | 504 | `error.code=query_timeout`，`status=timeout`，`cancelled`、`connection_discarded`、`late_result_ignored` |
| 排队期间预算耗尽 | 503 | `error.code=acquire_timeout`，语句**从未发送**到数据库 |
| 服务关闭时仍在排队 | 503 | `error.code=pool_closed`，`executed=false`，立即返回，语句**从未发送** |
| 语句执行中后端失效且服务正在关闭 | 503 | `error.code=connection_lost`，`executed=true`，不重试、不补建 |
| SQL 不合法 | 400 | `error.code=invalid_sql` |

### `GET /health`

返回连接池容量、空闲/占用数、累计创建与弃置数。

## 关键设计

### 1. 容量恒为 2，排队计入总预算

`QueryConnectionPool` 用容量为 2 的信号量限制**物理后端数**，第三个请求
排队等待而非新开连接。`acquire(deadline)` 接收绝对单调时钟截止时间，
等待与执行共用同一预算；排队期间过期则直接返回 503，不发送任何语句。

### 2. 单条只读 SELECT 的三层防护

1. `app/validation.py` 仅允许单条 `SELECT`/`WITH`/`TABLE`/`VALUES`，拒绝
   多语句（额外分号）、写关键字、`SELECT ... INTO` 等。
2. 每个请求在显式 `BEGIN READ ONLY` 事务中执行，会话默认
   `default_transaction_read_only=on`，并设置局部 `statement_timeout` 兜底。
3. 数据库角色 `fault_ro` 仅有 `SELECT` 权限，且角色默认只读；直连也无法
   写入（`InsufficientPrivilege`）。

### 3. 超时处理协议（`app/executor.py`）

1. 语句作为 **shielded task** 运行，超时回执一旦确定就不会被迟到结果覆盖。
2. 截止时间到，调用 libpq `PQcancel`（经独立取消连接）请求服务器中止语句。
3. `await` 该 shielded 任务直至真正结束（收到 `QueryCanceled` 或拿到迟到
   结果），杜绝协程残留。
4. 执行 `ROLLBACK` 并核对 `transaction_status == IDLE`，**确认事务结束**才
   把连接放回池；无法确认则关闭弃置并补建，保持容量为 2。
5. 迟到完成的结果只做审计日志，回执固定为 504 且不携带 `rows`。

### 4. 连接级故障的弃置补建与无感重试

后端被带外终止（如 `pg_terminate_backend`）时，连接级错误触发弃置并由池
补建；只读语句在同一总预算内安全重试一次，客户端无感知。若补建暂时失败，
池临时缩水并在下一次获取时懒建补回，不会死锁。

### 5. 关闭流程（`QueryConnectionPool.close`）

停服时可能仍有查询占用连接、其他请求在排队。关闭按下面的顺序进行：

1. **停止接单**：收到 `SIGTERM` 即由信号处理器调用 `start_draining()`
   （只置标志并用 `call_soon_threadsafe` 唤醒等待者，不触碰 asyncio 内部）。
   这一步是必要的：ASGI 服务器会先等所有请求任务结束、最后才执行 lifespan
   关闭；若只在 `close()` 里停单，排队请求会在这段窗口里拿到连接并发出 SQL。
2. **排队请求立即收尾**：等待信号量的请求被唤醒，收到 `503 pool_closed`
   （`executed=false`），**不发送任何 SQL**。
3. **在途查询按原总期限收尾**：已借出的查询不被打断，继续用原有 deadline
   执行；超时仍走原有的取消 → 确认 IDLE → 复用/弃置协议。
4. **归还或弃置均不补建**：关闭开始后，健康归还和无法确认的弃置都只是
   关闭并移出跟踪集合，不会再调用 `_make_connection`。
5. **关闭返回即全部关闭**：`close()` 等到跟踪集合清空才返回，此时所有
   后端（含数据库中 `pg_stat_activity` 的后端）都已关闭。重复或并发调用
   `close()` 安全，均会等到排空完成；关闭后 `acquire()` 立即拒绝。

`/health` 的 `pool.closed` 可观测关闭状态；`stop_grace_period` 与
`--timeout-graceful-shutdown` 为排空留出足够时间。

## 无 Docker 的本地运行与自测

本机若无容器运行时，可用 `pgserver`（PyPI 上的免 root PostgreSQL 16）：

```bash
pip install -r requirements.txt pgserver
python tests/local_run.py        # 拉起 PG16 + uvicorn
python -m tests.selftest         # 端到端自测（46 项）
python tests/test_protocol.py    # 超时/迟到结果/排队协议直测（11 项）
python tests/test_shutdown.py    # 关闭与查询交错：排队回执/排空/不补建/取消（38 项）
python tests/test_shutdown_http.py  # 真实 uvicorn + SIGTERM 关闭演练（14 项）
```

## 目录结构

```
app/            FastAPI 应用（config/pool/executor/validation/schemas/main）
db/01_init.sql  建表、预置日志、只读角色（Compose 首次初始化执行）
tests/          selftest.py（HTTP 端到端）、test_protocol.py（协议直测）、
                test_shutdown.py / test_shutdown_http.py（关闭流程）、
                local_run.py（免 Docker 启动器）
Dockerfile / docker-compose.yml / requirements.txt
```
