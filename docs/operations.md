# 运维与契约细则

README 保持一页速览，这里放不适合塞进去的中等深度内容：部署方式、信任边界、API 契约变更、Docker 细节、目录约定。

## 部署

- **API 服务**：`python -m board.api` 默认用 waitress 启动（线程数 `TB_WSGI_THREADS`，默认 8）；waitress 缺失时回退 werkzeug 并打 WARNING 提示非生产级。选 waitress 的原因：gunicorn 不支持 Windows（本项目主要在 Windows 上演示），而 waitress 是纯 Python 的单进程多线程 WSGI 服务器，正好和项目里 threading.Lock 限流这类进程内状态合拍——换成多进程部署会把限流计数拆散，那就得把限流挪到反向代理层，演示系统不做这一步。
- **进程守护（裸机）**：`python watchdog.py` 守护 api + worker W1/W2。子进程非 0 退出按 3s→6s→12s→30s 指数退避重拉（防配置性故障引发重启风暴）；rc=0 视为主动退出，不再拉起。watchdog 只管进程活着，卡住的任务由 worker 主循环里的 reaper 按租约回收，两者职责不重叠。想让 watchdog 脱离 shell 进程树，可用 WMI（Win32_Process.Create）启动。
- **对外暴露前提**：见下方信任边界。

### 两种部署形态怎么选

裸机 watchdog 和 docker compose 并存、随时可切换，但共享宿主机 127.0.0.1:5000，不能同时跑——切换前先停掉另一边（`docker compose down` 或停裸机进程）。

- 容器形态：重启交给 compose 的 restart 策略（api/worker/postgres 为 unless-stopped，seed 一次性 on-failure）。编排层本来就会"退出即重拉"，watchdog 在容器里是重复建设，所以不进容器。
- 裸机形态：没有编排层，watchdog 就是守护本体，保留退避和 rc=0 语义。
- 两种形态共同的不变量：任务级容错永远靠租约 + reaper，和"谁来重启进程"无关。正确性始终在 PostgreSQL 的锁和约束里，部署形态只决定进程由谁拉起，不引入新的协调组件。

## 信任边界

- 默认只监听 127.0.0.1：不出本机即视为可信，未设 `API_TOKEN` 时 `/api` 全放行（首个请求时打一次 WARNING 提醒这一点）。
- 设置 `API_TOKEN` 后，`/api` 所有请求必须带 `Authorization: Bearer <token>`，用 `hmac.compare_digest` 常量时间比对，不匹配一律 401。认证在限流之前，无效 token 不消耗限流配额。
- 看板静态页和 `/healthz` 探活不做认证。
- 容器形态的例外：容器内 API 绑 0.0.0.0（端口映射的硬前提），"不出本机"改由 compose 只映射 `127.0.0.1:5000` 保证；compose 内网可以无 token 直达 api，放开对外映射前必须先设 `API_TOKEN` 并加 HTTPS 反代。
- 对外部署的底线：`API_TOKEN` + HTTPS 反向代理（Bearer 是明文，没有 TLS 不能裸露）。不内置多用户/权限体系。

## API 契约变更（相对早期版本）

1. `GET /api/tasks` 默认只返回前 1000 条（显式 limit 超过 2000 会被夹到 2000，不报错）。有没有后续页看响应头 `X-Has-More`；要全量就带 `after_id` 循环翻页直到该头为 false。
2. `claim_epoch` 读端改为字符串输出：PG 的 bigint 可能超出 JS Number 的 2^53 安全整数范围，字符串透传无损。写端 `expected_epoch` 同时接受 int 和纯十进制数字串（≤19 位，对齐 bigint 值域）；消费方拿到字符串原样回传即可，不必自己转数字。

## Docker 细节与数据生命周期

```bash
docker compose up --build          # postgres healthy → seed 播种 → api healthy → worker 消费
# 看板 http://127.0.0.1:5000（端口只映射到 127.0.0.1）
```

- 多 worker 并发演示：`docker compose up -d --scale worker=5`，再用
  `docker compose exec api python scripts/seed_demo_bulk.py --count 40` 注入任务。
  观察点：看板 done 数量增长、claimed_by 出现多个不同 worker id（容器 HOSTNAME 派生，scale 时天然唯一）；`docker compose logs worker | grep claimed` 可截取并发认领证据。
- 容器内全量测试（含慢速攻击用例和覆盖率，conftest 自动建 taskboard_test 隔离库）：`docker compose --profile test run --rm testrunner`
- 数据生命周期：seed 默认非破坏——首次 up 播种 3 个基线任务，之后重启保留已有数据（含注入的批量任务）；`docker compose down` 保留命名 volume pgdata，`down -v` 连库一起清。
- 配置纪律：环境变量统一由 compose 注入（x-app-env 锚点），不挂载宿主机 .env；容器 PG 不映射端口、只在 compose 内网可达，和宿主机 PostgreSQL 互不干扰。容器演示默认不设 API_TOKEN（无认证、仅本机映射），需要时在 api 服务的 environment 里加。
- Dev Container：`.devcontainer/` 复用同一 compose 栈（主 compose + extend 覆盖层），VS Code "Reopen in Container" 即得 postgres/seed/api/worker 全套环境和源码挂载。

## 目录约定

演示与取证脚本都在 `scripts/`（attack_claim / reaper_demo / seed_demo_bulk / e2e_prep）；`tests/` 只放 pytest 用例和 conftest，两边不混。
