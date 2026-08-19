# COLLAB —— 分工、AI 使用与验证

## 分工与声明

本为为单人独立完成，特此说明。全程使用 AI 编码助手：方案设计、代码实现、测试编写、问题排查都有参与，分多个 session 迭代（时间线见 git log）。所有产出经我审阅和验证。与 AI 出现分歧时的裁决规则只有一条：谁说了都不算，以测试和攻击脚本的实测结果为准——下面是纠错案例。

## AI 纠错案例（AI 给错 → 怎么发现 → 改成了什么）

| AI 给的方案 | 怎么发现有问题 | 纠正 |
|---|---|---|
| 攻击脚本用 `GROUP BY id HAVING count(*)>1` 查重复认领 | 审阅时推演即知：tasks.id 是主键，库里结构上不可能有重复 id 行，这个查询恒返回空——是个永远通过、什么都没验证的死检查 | 改判据：重复认领只会表现为多个进程回传同一个 id，故以队列回传 id 去重 + DB 计数双向核对为准（scripts/attack_claim.py 注释有记录） |
| 幂等上报用常见写法 `ON CONFLICT DO NOTHING RETURNING (xmax = 0)` 区分首写/重复 | 对照 PostgreSQL 语义：xmax 技巧是给 DO UPDATE 用的，DO NOTHING 冲突时根本不返回行，表达式永远求值不到，纯死代码 | 改为直接 `RETURNING 1`，fetchone() 为 None 即重复上报（board/logs.py 注释有记录） |
| test_seed 假设 seed 可以直接叠在 fixture 连接的事务上跑 | 真库复现：fixture 的未提交事务持有 tasks 表 ACCESS SHARE 锁，seed 的 DDL 要 ACCESS EXCLUSIVE，撞 lock_timeout 确定性报 LockNotAvailable | 测试侧先 commit 释放锁再跑 seed，不动生产代码（commit c397357） |
| compose 的 seed 服务用 --force 每次 up 清库重建 | 评审发现演示数据每次重启归零，和"默认非破坏"的项目纪律矛盾 | 改为默认只在空库播种，清库要显式 --reset（commit 45654f0） |

四条的共同点：没有一条是 AI 自我反思发现的，全靠推演、真库实测、评审这些外部手段兜住。

## 验证方式（不依赖 AI 口头保证）

1. `pytest tests -v` 真库实测：默认集 122 个用例全绿（test_api 40 / test_params 30 / test_recovery 23 / test_db 7 / test_blindspots 5 / test_idempotent_log 5 / test_worker_fail 5 / test_seed 3 / test_watchdog 3 / test_stepidx 1）。默认 addopts 排除慢速攻击用例（`-m "not slow"`），`pytest -m ""` 全量 123，CI 按全量跑；无数据库时 DB 用例自动 skip。重点边界都有对应用例：reaper 回收、代际围栏、死信上限、状态翻转白名单（test_recovery）；409 状态门、并发上报恰插一行、限流 429、分页/ETag、token 认证、epoch 字符串化（test_api）。
2. `scripts/attack_claim.py`：multiprocessing spawn 10 进程 × 10 轮 × 100 任务，判定 = 队列回传 id 去重 + DB 计数核对 + claimed_by 分布核对（≥2 个 worker 真的抢到了活）。看日志尾部两行：`duplicate_claims=0`、`result=PASS`。全量跑另含两轮深度验证：claim×reaper 组合轮（回收重认领后 epoch 严格递增、旧代围栏写入全部 0 行）、report_step 洪泛轮 ×3（每 (task, step) 恰好一行、首报归属正确）。CI 里由 tests/test_attack_claim.py subprocess 复用同一脚本。
3. reaper 实杀演示：LEASE_SECONDS=15 下强杀正在执行任务 2 的 W1，W2 打出 `reclaimed expired tasks: [2]` 后接手重跑到 done。
4. 看板 E2E 正反例：正例并发 5 POST 全部去重（received=5 / inserted=0 / duplicates_ignored=5，worker 先报的场景）；反例竞速失败时 5/5 返回 409，前端红色错误面板如实渲染，不谎报通过。

## 修复轨迹

自查加三路代码评审（完整性 / 正确性 / 影响面），共两轮系统性修复。按影响程度排：

| 级别 | 问题 | 修复 |
|---|---|---|
| 高 | worker 崩溃后任务永远卡在 claimed/running | claimed_at 兼作租约，心跳线程续租；reaper 定期回收超期任务，达重试上限进 failed 死信；回收后整体重跑，已报 step 由幂等主键挡下 |
| 高 | API 手动上报端点兼改任务状态，形成双写者 | 收敛为单写者：任务状态只有 worker 能改，上报端点只写 step_logs，非 claimed/running 一律 409 |
| 高 | 前端对非 2xx 响应仍渲染"幂等验证通过" | 非 2xx 渲染红色错误面板，如实展示失败 |
| 中 | 被夺权的僵尸 worker 可能继续写入（双执行） | transition / report_step 带 (claimed_by, claim_epoch) 双围栏，僵尸连日志都写不进 |
| 中 | seed 默认破坏性、watchdog 固定间隔重启 | seed 默认非破坏（--reset 显式清库）；watchdog 指数退避 3s→30s，rc=0 视为主动退出不再拉起 |
| 低 | schema 重复执行报错、失败原因不可见 | IF NOT EXISTS 幂等化；step_logs 加 error_message、tasks 加 finished_at；补 CHECK 约束与部分索引 |

评审轮的十项中低危小修不逐条展开，代表性的几条：围栏读加 `FOR UPDATE`，消除 READ COMMITTED 下快照先于 reaper 提交的僵尸写窗口；report 端点状态读改 `FOR UPDATE` 且行锁保持到提交，与回收互斥；未捕获异常对外只回固定文案，细节进服务端日志；claimed→running 翻转失败先 release 归还 pending 再跳过，不等租约到期；reaper 从"队列空才回收"改为按 5s 间隔定期回收。

## 遗留取舍与已知边界

- **first-report-wins 的代价**：冲突即 DO NOTHING、没有任何 UPDATE 路径，幂等靠主键结构保证；代价是真实瞬时失败后重试成功，日志永远记为失败。对策：失败必落库且可观测（error_message 列 + 告警日志），不静默。备选 `ON CONFLICT DO UPDATE ... WHERE NOT success`（只允许失败→成功的单调提升）语义更贴业务，但正确性就退化为"WHERE 条件要写对"，演示系统不换。
- **回收后断点续跑**：不做。回收任务整体重跑，已报 step 由幂等主键自动跳过；真续跑要持久化 step 级执行游标，这个规模不值。
- **嵌套参数深合并**：整体替换是刻意选择，有测试锁定行为，改语义先改测试。
