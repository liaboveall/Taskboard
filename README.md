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

## 部署
- **生产启动（waitress）**：`.venv\Scripts\python.exe -m board.api` 自动以 `waitress.serve` 提供（线程数 `TB_WSGI_THREADS` 可覆盖，默认 8）；waitress 缺失时回退 werkzeug `app.run`（debug 关闭）并打 WARNING 提示非生产级。选型理由：gunicorn 不支持 Windows（本项目演示/部署环境以 Windows 为主）；waitress 是纯 Python 单进程多线程 WSGI 服务器，与项目内 threading.Lock 限流、模块级状态的进程内语义一致——多进程部署会分裂限流计数等进程内状态，需下沉到反向代理层解决，故不内置多进程方案。
- **守护拉起链路（watchdog）**：`.venv\Scripts\python.exe watchdog.py` 守护 api + worker W1/W2：子进程退出即按策略重拉——非 0 退出按 3s→6s→12s→30s 指数退避（防配置性故障触发重启风暴），rc=0 优雅退出置停止态不再拉起；职责边界只管进程存活，被收割任务的回收由 worker 主循环的租约 reaper 负责。建议用 WMI（Win32_Process.Create）启动 watchdog 使其脱离 shell 进程树。
- **对外暴露前提**：见下方「信任边界」（设 `API_TOKEN` + HTTPS 反代）。

## 信任边界
- 默认仅监听 127.0.0.1：未出本机即视为可信，未设 `API_TOKEN` 时 `/api` 全放行（首个 /api 请求时打一次性 WARNING 提示该口径）。
- 设置 `API_TOKEN` 环境变量后，`/api` 全部请求必须携带 `Authorization: Bearer <token>`，`hmac.compare_digest` 常量时间比对，不匹配一律 401（error_code=unauthorized）；认证门位于限流之前，无效 token 不消耗限流配额。
- 看板静态页与 `/healthz` 探活不受认证拦截。
- 对外部署必须：设置 `API_TOKEN` + HTTPS 反向代理（token 为明文 Bearer，无 TLS 不得裸露）；本项目不内置多用户/权限体系。

## 砍掉清单
连接池、回收后断点续跑、嵌套深合并、WebSocket、迁移框架——均为与本规模不匹配或刻意取舍，逐项理由见 COLLAB.md。（鉴权已以 opt-in `API_TOKEN` 形式补齐，见上方「信任边界」。）
