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
```

## 参数"当前生效值"的演变（test_params.py 逐 Step 断言证明）
起点 = base ⊕ L2；之后逐 Step 粘性推进，L3 `""` 保留当前值、不回跳 base。例：base={a:1,b:2,c:3}，L2={a:10,d:4}：

| Step | L3 override | 生效快照 |
|---|---|---|
| 1 | {b:20, e:""} | {a:10,b:20,c:3,d:4}（e 未定义过，保持不存在） |
| 2 | {a:"", c:""} | 同上（哨兵跳过） |
| 3 | {a:100} | {a:100,b:20,c:3,d:4} |
| 4 | {} | 同上 |

## 自己发现的边界情况
- L3 `""` 保留当前粘性值，不回跳 base/L2；L2 `""` 是字面值。
- `""` 作用于从未定义的 key → key 保持不存在。
- 假值（0/False/None）不是哨兵，仅精确 `""` 触发跳过。
- 嵌套 dict override 整体替换、非深合并（刻意取舍，有测试锁定）。
- step_index 可不连续，按真实序号上报。
