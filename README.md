# 任务调度看板（kGroup 实习生笔试题·题目一）

Python（Flask + psycopg 3）+ PostgreSQL：三层参数合并、并发认领、幂等 step 日志、单文件状态看板。并发正确性靠 PostgreSQL 行锁与唯一约束，无外部中间件。

**语言选择**：Python，团队最熟。并发正确性全部下沉到数据库行锁与唯一约束，应用层不持有分布式状态；本规模（≤10 worker、≤5 TPS）用不上 MQ/Redis。

## 快速开始（Python 3.12，需本机 PostgreSQL）
```bash
python -m venv .venv && .venv\Scripts\python.exe -m pip install -r requirements.txt
createdb taskboard && copy .env.example .env      # 在 .env 填入 DATABASE_URL
.venv\Scripts\python.exe -m board.seed            # 空库播种；--reset 破坏性重建
.venv\Scripts\python.exe -m board.worker --id W1  # 第二终端起 --id W2
.venv\Scripts\python.exe -m board.api             # 打开 http://localhost:5000
```

## Docker 一键运行（零本机依赖，仅需 Docker Desktop）
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
- 容器配置纪律：环境变量统一由 compose 注入（x-app-env 锚点），不挂载宿主机 .env；
  容器 PG 只在 compose 内网暴露（不映射端口），与宿主机 PostgreSQL 互不干扰。
  容器演示默认不设 API_TOKEN（无认证，仅本机映射）；需开启时在 api 服务的
  environment 注入 `API_TOKEN` 即可（口径见「信任边界」）。
- **Dev Container**：`.devcontainer/` 复用同一 compose 栈（主 compose + extend 覆盖层），
  VS Code "Reopen in Container" 即得 postgres/seed/api/worker 全套环境与源码挂载。

## 架构简述
- 参数合并：L1 base / L2 group / L3 step 三层粘性折叠，L3 `""` 为"不覆盖"哨兵。
- 并发认领：单条原子 UPDATE + FOR UPDATE SKIP LOCKED。
- 幂等：step_logs 主键 (task_id, step_index) + ON CONFLICT DO NOTHING，first-report-wins。
- 任务状态唯一写者是 worker；手动上报只写 step_logs 并带状态门。
- 租约回收：超期任务由 reclaim_expired 回收重跑，状态流转与日志写入带 owner/epoch 围栏。

## 参数"当前生效值"的演变（test_params.py 逐 Step 断言证明）
起点 = base ⊕ L2；之后逐 Step 粘性推进，L3 `""` 保留当前值、不回跳 base。例：base={a:1,b:2,c:3}，L2={a:10,d:4}：

| Step | L3 override | 生效快照 |
|---|---|---|
| 1 | {b:20, e:""} | {a:10,b:20,c:3,d:4}（e 未定义过，保持不存在） |
| 2 | {a:"", c:""} | 同上（哨兵跳过） |
| 3 | {a:100} | {a:100,b:20,c:3,d:4} |
| 4 | {} | 同上 |

## 真实并发如何保证
`scripts/attack_claim.py`：spawn 起 10 个独立进程，每个进程在子进程内新建独立数据库连接（非线程/协程级伪并发），Barrier 对齐起跑。判定 = 队列回传 id 去重 + DB 计数核对 + 多 worker 参与度核对。10 进程 × 10 轮 × 100 任务，**duplicate_claims=0**。

## 自己发现的边界情况
- L3 `""` 保留当前粘性值，不回跳 base/L2；L2 `""` 是字面值。
- `""` 作用于从未定义的 key → key 保持不存在。
- 假值（0/False/None）不是哨兵，仅精确 `""` 触发跳过。
- 嵌套 dict override 整体替换、非深合并（刻意取舍，有测试锁定）。
- step_index 可不连续，按真实序号上报。

## 测试
- 本地默认：`pytest`（pyproject addopts 含 `-m "not slow"`，自动排除分钟级的多进程攻击用例）；无 PostgreSQL 环境时 DB 用例自动 skip、纯逻辑用例照跑，终端会打印醒目的"假绿警告"。
- CI 全量：GitHub Actions（.github/workflows/ci.yml）起 postgres:14 service + `TB_STRICT_DB=1`（隔离库供给失败大声 fail），`pytest -m ""` 覆盖默认 marker 排除、全量含 slow 攻击用例（`tests/test_attack_claim.py` subprocess 调 `scripts/attack_claim.py`）；覆盖率 `--cov=board --cov-report=term-missing` 首跑只出报告不卡门禁（不设 fail-under，待基线实测后回填）。
- 演示/取证脚本已归位 `scripts/`（e2e_prep / seed_demo_bulk / reaper_demo / attack_claim），`tests/` 只留 pytest 用例。
- 看板 E2E 验证：并发 5 次上报全去重、竞速失败如实渲染。
- 更多验证细节见 COLLAB.md。

## API 契约变更声明（相对早期版本的对外行为变更）
1. **GET /api/tasks 默认截断**：缺省行为由全量快照改为默认截断前 1000 条（缺省 limit=1000；显式 limit 超 2000 时夹紧到 2000，不报错）。是否还有后续页由响应头 `X-Has-More: true` 传达；需要全量数据的消费方应携带 `after_id` 循环翻页直至该头为 false。
2. **claim_epoch 读端字符串化**：GET 输出的 `claim_epoch` 由数字变为字符串（PG bigint 可超 JS Number 的 2^53 安全整数边界，字符串透传无损）。写端 POST report 的 `expected_epoch` 维持双接受：int 与纯十进制数字串（≤19 位，对齐 bigint 值域）；迁移方式：读端拿到字符串后原样透传即可，无需自行转数字。

## 部署
- **生产启动（waitress）**：`.venv\Scripts\python.exe -m board.api` 自动以 `waitress.serve` 提供（线程数 `TB_WSGI_THREADS` 可覆盖，默认 8）；waitress 缺失时回退 werkzeug `app.run`（debug 关闭）并打 WARNING 提示非生产级。选型理由：gunicorn 不支持 Windows（本项目演示/部署环境以 Windows 为主）；waitress 是纯 Python 单进程多线程 WSGI 服务器，与项目内 threading.Lock 限流、模块级状态的进程内语义一致——多进程部署会分裂限流计数等进程内状态，需下沉到反向代理层解决，故不内置多进程方案。
- **守护拉起链路（watchdog）**：`.venv\Scripts\python.exe watchdog.py` 守护 api + worker W1/W2：子进程退出即按策略重拉——非 0 退出按 3s→6s→12s→30s 指数退避（防配置性故障触发重启风暴），rc=0 优雅退出置停止态不再拉起；职责边界只管进程存活，被收割任务的回收由 worker 主循环的租约 reaper 负责。建议用 WMI（Win32_Process.Create）启动 watchdog 使其脱离 shell 进程树。
- **对外暴露前提**：见下方「信任边界」（设 `API_TOKEN` + HTTPS 反代）。

### 部署形态取舍（裸机 watchdog vs 容器 compose）
两种形态**并存不互斥，边界清晰**：
- **容器形态**：生命周期管理交给 compose restart 策略（api/worker/postgres `unless-stopped`，seed 一次性 `restart: "no"`）——容器编排层天然具备“退出即重拉”，watchdog 的指数退避在这里是重复建设，故 **watchdog 不进容器**。
- **裸机形态**：无编排层时 watchdog 就是进程守护本体，保留原样（含退避与 rc=0 停止态语义）。
- 两形态共用的不变量：任务级容错永远靠租约 + reaper（worker 主循环），与进程守护方式无关——守护只保进程活着，被收割任务的回收不依赖它。这也呼应项目零中间件哲学：正确性下沉到 PostgreSQL 行锁与唯一约束，部署形态只决定“谁来重启进程”，不引入任何新的协调组件。

## 信任边界
- 默认仅监听 127.0.0.1：未出本机即视为可信，未设 `API_TOKEN` 时 `/api` 全放行（首个 /api 请求时打一次性 WARNING 提示该口径）。
- 设置 `API_TOKEN` 环境变量后，`/api` 全部请求必须携带 `Authorization: Bearer <token>`，`hmac.compare_digest` 常量时间比对，不匹配一律 401（error_code=unauthorized）；认证门位于限流之前，无效 token 不消耗限流配额。
- 看板静态页与 `/healthz` 探活不受认证拦截。
- 对外部署必须：设置 `API_TOKEN` + HTTPS 反向代理（token 为明文 Bearer，无 TLS 不得裸露）；本项目不内置多用户/权限体系。

## 砍掉清单
连接池、回收后断点续跑、嵌套深合并、WebSocket、迁移框架——均为与本规模不匹配或刻意取舍，逐项理由见 COLLAB.md。（鉴权已以 opt-in `API_TOKEN` 形式补齐，见上方「信任边界」。）
