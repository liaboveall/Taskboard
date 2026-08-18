# Taskboard

基于 Flask + PostgreSQL 的任务调度看板。

## 技术栈
- Python 3.12
- Flask
- psycopg 3 + PostgreSQL 17

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

## API 端点
- `GET /api/tasks`：任务列表（ETag/304 + keyset 分页）
- `POST /api/tasks/<id>/report`：步骤结果上报（幂等，409 步号不匹配）
- `GET /healthz`：健康检查

## 前端看板
`static/index.html` 单文件看板，轮询 API 刷新任务状态。ETag/304 无变更不传 body，keyset 分页处理大数据集。

## 自己发现的边界情况
- L3 `""` 保留当前粘性值，不回跳 base/L2；L2 `""` 是字面值。
- `""` 作用于从未定义的 key → key 保持不存在。
- 假值（0/False/None）不是哨兵，仅精确 `""` 触发跳过。
- 嵌套 dict override 整体替换、非深合并（刻意取舍，有测试锁定）。
- step_index 可不连续，按真实序号上报。
