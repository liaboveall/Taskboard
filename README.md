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
- pytest 102 用例全绿（params 25 / api 34 / recovery 21 / 其余 22）。
- 多进程并发攻击测试 PASS（duplicate_claims=0）。
- 看板 E2E 验证：并发 5 次上报全去重、竞速失败如实渲染。
- 更多验证细节见 COLLAB.md。

## 信任边界
- 默认仅监听 127.0.0.1：未出本机即视为可信，未设 `API_TOKEN` 时 `/api` 全放行（首个 /api 请求时打一次性 WARNING 提示该口径）。
- 设置 `API_TOKEN` 环境变量后，`/api` 全部请求必须携带 `Authorization: Bearer <token>`，`hmac.compare_digest` 常量时间比对，不匹配一律 401（error_code=unauthorized）；认证门位于限流之前，无效 token 不消耗限流配额。
- 看板静态页与 `/healthz` 探活不受认证拦截。
- 对外部署必须：设置 `API_TOKEN` + HTTPS 反向代理（token 为明文 Bearer，无 TLS 不得裸露）；本项目不内置多用户/权限体系。

## 砍掉清单
连接池、回收后断点续跑、嵌套深合并、WebSocket、迁移框架——均为与本规模不匹配或刻意取舍，逐项理由见 COLLAB.md。（鉴权已以 opt-in `API_TOKEN` 形式补齐，见上方「信任边界」。）
