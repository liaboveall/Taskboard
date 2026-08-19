# 运维与契约细则（自 README 下沉）

README 保持一页速览，本页承载五块中等深度内容：部署形态取舍、信任边界细则、API 契约变更声明、Docker 详细命令与数据生命周期、目录约定。

## 部署

- **生产启动（waitress）**：`.venv\Scripts\python.exe -m board.api` 自动以 `waitress.serve` 提供（线程数 `TB_WSGI_THREADS` 可覆盖，默认 8）；waitress 缺失时回退 werkzeug `app.run`（debug 关闭）并打 WARNING 提示非生产级。选型理由：gunicorn 不支持 Windows（本项目演示/部署环境以 Windows 为主）；waitress 是纯 Python 单进程多线程 WSGI 服务器，与项目内 threading.Lock 限流、模块级状态的进程内语义一致——多进程部署会分裂限流计数等进程内状态，需下沉到反向代理层解决，故不内置多进程方案。
- **守护拉起链路（watchdog）**：`.venv\Scripts\python.exe watchdog.py` 守护 api + worker W1/W2：子进程退出即按策略重拉——非 0 退出按 3s→6s→12s→30s 指数退避（防配置性故障触发重启风暴），rc=0 优雅退出置停止态不再拉起；职责边界只管进程存活，被收割任务的回收由 worker 主循环的租约 reaper 负责。建议用 WMI（Win32_Process.Create）启动 watchdog 使其脱离 shell 进程树。
- **对外暴露前提**：见下方「信任边界细则」（设 `API_TOKEN` + HTTPS 反代）。

### 部署形态取舍（裸机 watchdog vs 容器 compose）
两种形态**并存可切换，边界清晰**；但注意互斥点：两形态共享宿主机
127.0.0.1:5000，**不可同时运行**——切换时先 `docker compose down` 再起裸机，
或先停裸机进程再 `docker compose up`。
- **容器形态**：生命周期管理交给 compose restart 策略（api/worker/postgres `unless-stopped`，seed 一次性 `restart: on-failure`）——容器编排层天然具备"退出即重拉"，watchdog 的指数退避在这里是重复建设，故 **watchdog 不进容器**。
- **裸机形态**：无编排层时 watchdog 就是进程守护本体，保留原样（含退避与 rc=0 停止态语义）。
- 两形态共用的不变量：任务级容错永远靠租约 + reaper（worker 主循环），与进程守护方式无关——守护只保进程活着，被收割任务的回收不依赖它。这也呼应项目零中间件哲学：正确性下沉到 PostgreSQL 行锁与唯一约束，部署形态只决定"谁来重启进程"，不引入任何新的协调组件。

## 信任边界细则
- 默认仅监听 127.0.0.1：未出本机即视为可信，未设 `API_TOKEN` 时 `/api` 全放行（首个 /api 请求时打一次性 WARNING 提示该口径）。
- **容器形态例外**：容器内进程经 `API_HOST=0.0.0.0` 绑全网卡（宿主机端口映射能触达的硬前提），"不出本机"改由 compose 仅映射 `127.0.0.1:5000` 保证；且 compose 内网内可无 token 直达 api，放开对外映射前必须先设 `API_TOKEN` + HTTPS 反代。
- 设置 `API_TOKEN` 环境变量后，`/api` 全部请求必须携带 `Authorization: Bearer <token>`，`hmac.compare_digest` 常量时间比对，不匹配一律 401（error_code=unauthorized）；认证门位于限流之前，无效 token 不消耗限流配额。
- 看板静态页与 `/healthz` 探活不受认证拦截。
- 对外部署必须：设置 `API_TOKEN` + HTTPS 反向代理（token 为明文 Bearer，无 TLS 不得裸露）；本项目不内置多用户/权限体系。

## API 契约变更声明（相对早期版本的对外行为变更）
1. **GET /api/tasks 默认截断**：缺省行为由全量快照改为默认截断前 1000 条（缺省 limit=1000；显式 limit 超 2000 时夹紧到 2000，不报错）。是否还有后续页由响应头 `X-Has-More: true` 传达；需要全量数据的消费方应携带 `after_id` 循环翻页直至该头为 false。
2. **claim_epoch 读端字符串化**：GET 输出的 `claim_epoch` 由数字变为字符串（PG bigint 可超 JS Number 的 2^53 安全整数边界，字符串透传无损）。写端 POST report 的 `expected_epoch` 维持双接受：int 与纯十进制数字串（≤19 位，对齐 bigint 值域）；迁移方式：读端拿到字符串后原样透传即可，无需自行转数字。

## Docker 详细命令与数据生命周期
```bash
docker compose up --build          # postgres healthy → seed 播种 → api healthy → worker 消费
# 看板：http://127.0.0.1:5000（端口只映射到 127.0.0.1）
```
- **多 worker 并发演示**：`docker compose up -d --scale worker=5` 后注入批量任务
  `docker compose exec api python scripts/seed_demo_bulk.py --count 40`；
  观察点：看板 done 数量增长、`claimed_by` 分布出现 ≥3 个不同 worker id（容器
  HOSTNAME 派生，--scale 时天然唯一）；`docker compose logs worker | grep claimed`
  可截取多进程并发认领证据。
- **容器内全量测试**（含 slow 攻击用例与覆盖率，conftest 自动建 taskboard_test 隔离库）：
  `docker compose --profile test run --rm testrunner`
- **收尾**：`docker compose down`（默认保留命名 volume pgdata；`down -v` 连库一起清）。
- **数据生命周期**：seed 缺省模式非破坏——首次 up 播种 3 任务基线，后续 up
  保留已有数据（含注入的批量任务），重建容器不清库；仅 `down -v` 清库。
- 容器配置纪律：环境变量统一由 compose 注入（x-app-env 锚点），不挂载宿主机 .env；
  容器 PG 只在 compose 内网暴露（不映射端口），与宿主机 PostgreSQL 互不干扰。
  容器演示默认不设 API_TOKEN（无认证，仅本机映射）；需开启时在 api 服务的
  environment 注入 `API_TOKEN` 即可（口径见上方「信任边界细则」）。
- **Dev Container**：`.devcontainer/` 复用同一 compose 栈（主 compose + extend 覆盖层），
  VS Code "Reopen in Container" 即得 postgres/seed/api/worker 全套环境与源码挂载。

## 目录约定
- 演示/取证脚本归位 `scripts/`（attack_claim / reaper_demo / seed_demo_bulk / e2e_prep），`tests/` 只留 pytest 用例与 conftest，两者职责不混居。
