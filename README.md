# A股股票监视器

这是一个带 Web 管理台的 A 股监控项目，支持：
- 交易日的9:20-11:30、13:00-15:00启动监控
- 自定义监控个股和指数，例如 `600519`、`000001.SZ`、`1A0001`
- 每个标的配置多条触发规则
- 支持连续 `N` 次满足后才触发的规则
- 每个标的绑定独立企业微信机器人 `webhook`
- Web 页面查看最新价格、涨跌幅、规则状态和告警记录
- 支持多个账号注册登录
- 通过注册码注册账号，忘记密码时也可用注册码重置指定账号的密码
- 监控命中后先进入持久化队列，再由独立 Webhook Worker 发送

## 运行架构

项目使用“单镜像，多容器”的三段式结构：

- `web`
  提供 FastAPI API、Web 页面、SSE 事件流、账号注册登录。
- `monitor`
  按 `轮询周期 / 监控任务数` 的间隔均匀执行监控任务。
- `webhook`
  从 SQLite 任务队列中拉取待发送消息，失败自动重试。

为了让三个进程共享状态，下面这些数据都会持久化到 SQLite：

- 用户账号和密码哈希
- 注册码哈希
- 轮询周期
- 行情快照缓存
- 规则连续命中计数
- 告警记录
- Webhook 发送任务
- SSE 事件流

## 项目结构

```text
.
├─ backend
│  ├─ app
│  │  ├─ static
│  │  │  ├─ app.js
│  │  │  ├─ index.html
│  │  │  └─ styles.css
│  │  ├─ auth.py
│  │  ├─ config.py
│  │  ├─ database.py
│  │  ├─ main.py
│  │  ├─ monitor_worker.py
│  │  ├─ monitoring.py
│  │  ├─ notifier.py
│  │  ├─ quote_provider.py
│  │  ├─ repository.py
│  │  ├─ schemas.py
│  │  ├─ utils.py
│  │  ├─ webhook_dispatcher.py
│  │  └─ webhook_worker.py
│  ├─ data
│  └─ requirements.txt
├─ Dockerfile
├─ docker-compose.yml
└─ README.md
```

## 核心设计

- `FastAPI` 提供 API 和页面
- `SQLite` 保存配置、事件、行情缓存和发送任务
- `users` 表支持多账号注册登录
- `rules` 表持久化连续命中要求和当前命中计数
- `MonitorService` 只负责抓行情、判规则、写入告警队列
- `WebhookDispatcher` 独立发送企业微信消息，并做失败重试
- 页面通过 `SSE` 读取数据库事件流，自动刷新最新缓存
- 数据库启用了 `WAL`，更适合多进程并发读写

## 已支持的规则字段

- `last_price` 最新价
- `change_pct` 涨跌幅
- `open_price` 开盘价
- `high_price` 最高价
- `low_price` 最低价
- `volume` 成交量
- `turnover` 成交额

操作符支持：

- `gte` 大于等于
- `lte` 小于等于
- `eq` 等于
- `neq` 不等于

额外规则条件：

- `consecutive_hits_required`
  连续满足多少次后才触发，默认 `1`

## 本地启动

### 1. 安装依赖

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r backend\requirements.txt
```

说明：

- 当前项目只使用 [requirements.txt](/J:/codex/monitor/backend/requirements.txt)
- 不再使用 `requirements-optional.txt`
- `akshare` / `pandas` 不是当前运行所需依赖

### 2. 配置环境变量

```powershell
$env:POLL_INTERVAL_SECONDS="30"
$env:REGISTRATION_CODE="change-this-registration-code"
$env:LOGIN_USERNAME=""
$env:LOGIN_PASSWORD=""
```

说明：

- `REGISTRATION_CODE` 用于注册账号和重置密码
- `LOGIN_USERNAME` / `LOGIN_PASSWORD` 可选，仅用于首次启动时预置一个初始账号
- 如果不预置账号，首次进入页面后可直接使用注册码注册多个账号

### 3. 启动 Web 服务

```powershell
uvicorn app.main:app --reload --app-dir backend
```

### 4. 启动 Monitor Worker

```powershell
python -m backend.app.monitor_worker
```

### 5. 启动 Webhook Worker

```powershell
python -m backend.app.webhook_worker
```

打开：

- Web 管理台: <http://127.0.0.1:8000>
- API 文档: <http://127.0.0.1:8000/docs>

## Docker Compose 部署

先准备环境变量：

```powershell
Copy-Item .env.example .env
```

至少建议修改：

- `REGISTRATION_CODE`
- `APP_PORT`
- `POLL_INTERVAL_SECONDS`

如果你希望首启直接有初始账号，再额外填写：

- `LOGIN_USERNAME`
- `LOGIN_PASSWORD`

先构建单镜像：

```powershell
docker build -t a-share-monitor:latest .
```

说明：

- `docker-compose.yml` 中的 `web`、`monitor`、`webhook` 三个服务都会复用这个同一个镜像
- 这是“单镜像，多容器”的运行方式
- 运行时环境变量只需要重点配置 `web` 服务；`monitor` 和 `webhook` 通过共享 SQLite 卷读取持久化配置

启动：

```powershell
docker compose up -d
```

查看日志：

```powershell
docker compose logs -f web
docker compose logs -f monitor
docker compose logs -f webhook
```

停止：

```powershell
docker compose down
```

说明：

- 镜像入口是 [Dockerfile](/J:/codex/monitor/Dockerfile)
- 编排文件是 [docker-compose.yml](/J:/codex/monitor/docker-compose.yml)
- 数据默认保存在共享卷 `monitor_data` 或你自定义的宿主机目录
- 容器内 SQLite 路径固定为 `/app/backend/data/monitor.db`

## 当前生产形态特点

- `web`、`monitor`、`webhook` 已拆成独立进程，互不阻塞
- 告警发送改成持久化任务队列，进程重启后任务不会丢
- Webhook 发送失败会自动重试，不会因为单次网络抖动直接吞掉消息
- SSE 事件不再依赖单进程内存队列，跨进程也能推送页面刷新
- 多账号可同时注册和登录
- 连续命中计数会持久化，Worker 重启后不会丢失规则状态

## 后续可扩展方向

- 将 `SQLite` 替换为 `PostgreSQL`
- 引入 Redis 作为事件总线和任务队列
- 增加账号角色和权限控制
- 增加节流、聚合和恢复通知
- 增加更复杂的触发条件，例如时间窗口、日内次数限制、恢复通知
