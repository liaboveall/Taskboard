# tests/seed_demo_bulk.py —— 批量演示数据播种（独立脚本，非 pytest 用例）
# 用法: python tests/seed_demo_bulk.py [--count N]   （默认 N=80）
#
# 仅用于演示批量运行（复现 w1/w2 批量执行证据）。本脚本是纯追加 INSERT，
# 绝不 TRUNCATE/清空任何表：可直接在已有数据的库上运行，重复执行会持续
# 追加任务；库重建由 `python -m board.seed --reset` 负责。
#
# 任务形态与 board.seed 的"任务1"一致：每任务 3 个 step，含 L3 粘性覆盖参数
# （Step2 a=200，Step3 a="" 粘住 200）。用 generate_series 批量插入，单事务提交。
#
# 注意：组/任务/步骤拆为同事务内三条语句插入。PostgreSQL 的 INSERT ... RETURNING
# 只能引用目标表自身的列，不能引用源 CTE/表别名（旧版单条 SQL 末尾
# `RETURNING t.group_id` 即因此报 UndefinedTable）。
import argparse
import json
import sys
from pathlib import Path

# 允许 `python tests/seed_demo_bulk.py` 直接运行（找到项目根下的 board 包）
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from board import db  # noqa: E402

# 与 board.seed demo-group 一致的 L2 覆盖（参与粘性链演示）
GROUP_OVERRIDE = {"region": "cn-north", "note": "", "retry": 3}
# 与 board.seed 任务1 一致的 base_params
TASK_BASE = {"a": 1, "timeout": 30}

# 3 个 step 的 L3 覆盖，按 step_index 1..3 展开（任务1 同款粘性链）
# 1) 建组（纯追加，不动已有数据）
SQL_GROUP = """
INSERT INTO task_groups (name, override_params)
VALUES (%(name)s, %(override)s::jsonb)
RETURNING id
"""

# 2) generate_series 批量插入 N 个同构任务
SQL_TASKS = """
INSERT INTO tasks (group_id, base_params)
SELECT %(group_id)s, %(base)s::jsonb
FROM generate_series(1, %(count)s)
"""

# 3) 每任务展开 3 个 step，L3 覆盖依次为 {} / {a:200} / {a:""}
SQL_STEPS = """
INSERT INTO steps (task_id, step_index, override_params)
SELECT t.id, s.idx,
       CASE s.idx
           WHEN 1 THEN '{}'
           WHEN 2 THEN '{"a": 200}'
           ELSE '{"a": ""}'
       END::jsonb
FROM tasks t
CROSS JOIN generate_series(1, 3) AS s(idx)
WHERE t.group_id = %(group_id)s
"""


def main():
    parser = argparse.ArgumentParser(
        description="批量播种同构演示任务（独立脚本，非 pytest 用例）")
    parser.add_argument("--count", type=int, default=80, help="播种任务数（默认 80）")
    args = parser.parse_args()
    if args.count <= 0:
        parser.error("--count 必须为正整数")

    print("注意：本脚本为纯追加播种，可在已有数据的库上直接运行。")
    conn = db.connect()
    try:
        # 单事务提交：组 + N 任务 + 3N 步骤要么全部落库，要么全部回滚
        with conn.cursor() as cur:
            cur.execute(SQL_GROUP, {
                "name": f"bulk-demo-{args.count}",
                "override": json.dumps(GROUP_OVERRIDE, allow_nan=False),
            })
            group_id = cur.fetchone()[0]
            cur.execute(SQL_TASKS, {
                "group_id": group_id,
                "base": json.dumps(TASK_BASE, allow_nan=False),
                "count": args.count,
            })
            cur.execute(SQL_STEPS, {"group_id": group_id})
        conn.commit()
        print(f"seeded: group id={group_id}, {args.count} tasks x 3 steps, all pending "
              f"(与 board.seed 任务1 同构：L3 粘性覆盖 Step2 a=200 / Step3 a=\"\")")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
