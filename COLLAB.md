# COLLAB.md

## AI 工具使用情况
全程使用 AI 编码助手辅助：方案设计、代码实现、测试编写与问题排查均在 AI 辅助下完成，分多个 session 迭代（含评审修复与证据补齐，时间线见 git log）。作者负责审阅每一处实现并验证结论。

## 验证方式（不依赖 AI 口头保证）
1. `pytest tests -v`：**102 个用例全绿**（test_params 25 / test_api 34 / test_recovery 21 / test_db 7 / test_blindspots 5 / test_idempotent_log 5 / test_worker_fail 4 / test_stepidx 1；无数据库环境下 DB 用例自动 skip）。
2. `tests/attack_claim.py`：multiprocessing spawn 真实多进程攻击，10 进程 × 10 轮 × 100 任务，判定 = 队列回传 id 去重 + DB 侧计数核对 + 参与度核对（distinct claimed_by ≥ 2），duplicate_claims=0（攻击日志已归档）。
3. 看板 E2E（新口径）：并发 5 次上报由前端 5 个并行 POST 承担，服务端每 POST 只执行 1 次真实上报、单 POST 响应 received=1；看板面板为 5 POST 聚合（received=5 / inserted=0 / duplicates_ignored=5，worker 先报、5 POST 全被去重）。
4. 双 worker 并行运行，批量认领交错分摊。

## 第三轮：系统性修复
### 修复清单
| 级别 | 问题 | 修复 |
|---|---|---|
| 高危 H1 | worker 崩溃后任务永远卡在 claimed/running，无回收 | claimed_at 兼作租约；worker 每步推进刷新租约（心跳零额外往返）；主循环空闲执行 `claim.reclaim_expired` 回收超租约任务（LEASE_SECONDS 环境变量，默认 60）；回收后整体重跑，已报 step 由幂等主键挡下。不做断点续跑（刻意取舍） |
| 高危 H2 | API 手动上报端点兼做任务状态流转，形成双写者 | 状态机单写者：任务状态唯一写者是 worker；手动上报端点只写 step_logs，加状态门（仅 claimed/running 可报，否则 409） |
| 高危 H3 | 前端对非 2xx 响应仍渲染"幂等验证通过" | 非 2xx 渲染红色错误面板，如实展示失败 |
| 中等 M1 | 被夺权的僵尸 worker 可能继续写入（双执行） | transition 带 claimed_by fencing；report_step 带 owner 围栏写入，僵尸 worker 连日志都写不进 |
| 中等 | seed 默认破坏性、watchdog 固定间隔重启、演示口径漂移 | seed 默认非破坏（仅空库播种），`--reset` 显式破坏性重建（`--force` 为免确认别名）；watchdog 指数退避重启 + WATCHDOG_PYTHON 可配（职责边界=进程存活，任务回收归 reaper）；口径收敛见下 |
| 轻微 | schema 重复执行报错、失败不可观测 | IF NOT EXISTS 非破坏幂等化；step_logs 新增 error_message 列、tasks 新增 finished_at；step_index/current_step CHECK>=1；部分索引 idx_tasks_pending / idx_tasks_lease |

### 验证方式
1. **pytest 102 用例全绿**（本机有 PostgreSQL，无 skip）。
2. **攻击测试**：攻击测试末行 `result=PASS, duplicate_claims=0`；test_stepidx.py 非连续 step_index 1/3/7 口径已入 pytest。
3. **reaper 实杀演示**：LEASE_SECONDS=15 下 W1 执行任务 2 时被强杀，W2 打印 `reclaimed expired tasks: [2]` 后重新认领并跑到 done。
4. **浏览器 E2E 正反例**：正例：5 POST 聚合 received=5/inserted=0/duplicates_ignored=5；反例：竞速失败 5/5 POST 返回 409，红色错误面板如实渲染（H3 修复验证）。

### 新测试清单
- test_recovery（21 用例）：reaper 回收、新鲜租约不回收、claimed_by fencing、owner 围栏写入、无 steps 任务快速 done。
- test_api（34 用例）：404 任务不存在、409 状态门（done/pending 不可报）、手动上报不改任务状态、并发上报恰好插入 1 行、400 参数校验。
- test_params 25（含嵌套结构整体替换行为锁定）；test_idempotent_log 5（含 error_message 落库）；test_db 7 / test_blindspots 5 / test_worker_fail 4 / test_stepidx 1。
- tests/seed_demo_bulk.py：批量播种演示任务工具（--count，纯追加），用于复现双 worker 批量运行与 reaper 演示证据。

### 遗留取舍（答辩预案）
- **first-report-wins 代价**：冲突即 DO NOTHING、无任何 UPDATE 路径，幂等正确性是结构性的；代价是 step 日志可能与任务终态分歧（真实瞬时失败 → 重试成功会永远记录为失败）。对策：失败必落 failed 且大声可观测（error_message 列 + stdout 告警），不再静默吞掉。备选 `ON CONFLICT DO UPDATE ... WHERE NOT success`（单调提升）语义更贴近业务，但正确性退化为依赖 WHERE 条件正确，演示系统不选。
- **回收后断点续跑**：刻意不做——回收任务整体重跑，已报 step 由幂等主键挡下；实现续跑需持久化 step 级执行游标，复杂度与本规模不匹配。
- **psycopg_pool 连接池**：≤5 TPS 规模每请求建连可接受；替代方案 threading.local 复用+信号量封顶未选，因池自带超时语义而代码量相当。
- **嵌套参数深度合并**：整体替换是刻意取舍，有测试锁定。

## 第四轮：评审意见修复
合并三路代码评审（完整性/正确性/影响面）发现的中低危问题，共 10 项代码修复，均为最小切口：
1. **旧库列补齐**：schema.sql 末尾追加 `ADD COLUMN IF NOT EXISTS`（finished_at / error_message），头注释说明 CHECK 与部分索引不回填旧表、完整升级用 `board.seed --reset`。
2. **围栏 FOR UPDATE**：logs.py owner 非 None 的围栏 INSERT，SELECT 数据源加 `FOR UPDATE`，使围栏读与 reaper 回收串行化，消除 READ COMMITTED 下快照先于 reaper 提交的僵尸写入窗口。
3. **API 行锁关门**：report 端点状态读改 `SELECT ... FOR UPDATE` 并去掉提前 commit，行锁保持到 report_step 内部 commit，与 reaper 互斥。
4. **errorhandler 收敛**：未捕获异常对外固定文案 `internal server error`，详情 `app.logger.exception` 只进服务端日志。
5. **快进返回值**：worker 无 steps 快进 done 时检查 transition 返回值，失权时如实报 `ownership lost before fast-forward`。
6. **告警措辞**：_mark_failed 失败日志写不进时的打印改中性归因 `failure log not written (duplicate or fenced out)`。
7. **reaper 定期化**：主循环 time.monotonic 节流（REAP_INTERVAL_SECONDS=5），不再只在队列空时回收。
8. **release 接线**：claimed→running 重试全败时先 `claim.release` 归还 pending 再跳过，比等租约到期更及时。
9. **攻击日志 --out**：attack_claim.py 输出路径 argparse 参数化，默认不变。
10. **注释修正**：finished_at 注释改为“预留终态打点，供排查/审计查询，当前看板未展示”（schema.sql 与 claim.py）。
另同步：README/COLLAB 用例拆分数字按实测修正（test_params 25 / test_api 6）；.gitignore 补 `evidence/_*`；回归 pytest 41 全绿。
