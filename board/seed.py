# board/seed.py —— 演示数据播种
# 用法: python -m board.seed [--reset] [--force]
#
# 默认行为（非破坏）：执行 schema.sql（全部 CREATE IF NOT EXISTS）+
#   仅当 tasks 表为空时播种；tasks 非空则打印 "tasks not empty, seeding skipped"。
# --reset（破坏性）：DROP 四表（按 step_logs→steps→tasks→task_groups 顺序）
#   → 执行 schema.sql → 播种；需交互确认（输入 yes/y 才继续）。
# --force：--reset 的向后兼容别名，触发同样的破坏性重建
#   （历史上 --force 的语义是跳过交互确认，此便利性保留）。
# 注意：schema.sql 已无 DROP 语句，DROP 逻辑由本文件实现。
# schema 与播种合并在同一事务内单次 commit，消除空库中间态。
#
# 播种内容：
#   1 个 group（override 含一个普通 key + 一个 "" 字面值 key）
#   3 个任务 × 3 steps，覆盖粘性链、"" 回退、新 key 引入等刁钻组合，全部 pending。
import argparse
import json
import sys

from board import db

# 当前 schema 版本（与 schema.sql 末尾的登记值一致）：
# seed 启动时断言 schema_meta 版本，防止漏迁移的旧库静默运行。
EXPECTED_SCHEMA_VERSION = 3

# 破坏性重建的 DROP 顺序：先子表后父表，规避外键依赖
DROP_ORDER = ["step_logs", "steps", "tasks", "task_groups"]


class SchemaVersionError(RuntimeError):
    """schema_meta 版本与 EXPECTED_SCHEMA_VERSION 不匹配（漏迁移的旧库）。"""


def apply_schema(cur):
    """执行 schema.sql（全部 IF NOT EXISTS + 幂等 DO 块，可重复执行；不 commit，由调用方统一提交）。"""
    schema = (db.ROOT / "schema.sql").read_text(encoding="utf-8")
    cur.execute(schema)


def assert_schema_version(cur):
    """apply_schema 后断言 schema_meta 当前版本 == EXPECTED_SCHEMA_VERSION。

    不匹配（表缺失/版本过低/过高）抛 SchemaVersionError：
    漏迁移的旧库绝不允许静默运行（审计列/外键/函数式索引缺失会让
    写路径与查询口径错位）。"""
    cur.execute("SELECT max(version) FROM schema_meta")
    row = cur.fetchone()
    version = row[0] if row else None
    if version != EXPECTED_SCHEMA_VERSION:
        raise SchemaVersionError(
            f"schema 版本不匹配：库中 schema_meta 版本={version!r}，"
            f"期望 {EXPECTED_SCHEMA_VERSION}。请重跑 schema.sql 完成迁移。"
        )


def seed(cur):
    """插入演示数据（不 commit，由调用方统一提交）。"""
    # 1 个组：普通 key "region" 覆盖；"note" 的 "" 是 L2 字面值
    cur.execute(
        "INSERT INTO task_groups (name, override_params) VALUES (%s, %s) RETURNING id",
        ("demo-group", json.dumps({"region": "cn-north", "note": "", "retry": 3}, allow_nan=False)),
    )
    group_id = cur.fetchone()[0]

    # 任务1：粘性链 + 关键链
    #   base a=1; L2 a=20; Step2 a=200; Step3 a="" -> Step3 生效 200
    cur.execute(
        "INSERT INTO tasks (group_id, base_params) VALUES (%s, %s) RETURNING id",
        (group_id, json.dumps({"a": 1, "timeout": 30}, allow_nan=False)),
    )
    t1 = cur.fetchone()[0]
    for idx, ov in enumerate([{}, {"a": 200}, {"a": ""}], start=1):
        cur.execute(
            "INSERT INTO steps (task_id, step_index, override_params) VALUES (%s,%s,%s)",
            (t1, idx, json.dumps(ov, allow_nan=False)),
        )

    # 任务2：L3 "" 作用于从未定义的 key（保持不存在）+ L3 引入新 key
    cur.execute(
        "INSERT INTO tasks (group_id, base_params) VALUES (%s, %s) RETURNING id",
        (group_id, json.dumps({"batch_size": 64}, allow_nan=False)),
    )
    t2 = cur.fetchone()[0]
    for idx, ov in enumerate([{"ghost": ""}, {"mode": "fast"}, {}], start=1):
        cur.execute(
            "INSERT INTO steps (task_id, step_index, override_params) VALUES (%s,%s,%s)",
            (t2, idx, json.dumps(ov, allow_nan=False)),
        )

    # 任务3：L2 "" 字面值 + Step1 即带 override + 后续 "" 不回跳
    cur.execute(
        "INSERT INTO tasks (group_id, base_params) VALUES (%s, %s) RETURNING id",
        (group_id, json.dumps({"note": "base-note", "a": 5}, allow_nan=False)),
    )
    t3 = cur.fetchone()[0]
    for idx, ov in enumerate([{"a": 50}, {"note": "", "a": ""}, {"retry": 9}], start=1):
        cur.execute(
            "INSERT INTO steps (task_id, step_index, override_params) VALUES (%s,%s,%s)",
            (t3, idx, json.dumps(ov, allow_nan=False)),
        )

    print(f"seeded: 1 group (id={group_id}), 3 tasks (ids={t1},{t2},{t3}) x 3 steps, all pending")


def drop_all(cur):
    """破坏性重建：按子表→父表顺序 DROP 四表（schema.sql 已无 DROP 语句）。"""
    for table in DROP_ORDER:
        cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")


def main():
    parser = argparse.ArgumentParser(
        description="taskboard seed（默认非破坏：仅空库时播种；--reset 破坏性重建）")
    parser.add_argument("--reset", action="store_true",
                        help="破坏性重建：DROP 四表 -> schema.sql -> 播种（需交互确认）")
    parser.add_argument("--force", action="store_true",
                        help="--reset 的向后兼容别名：行为等同 --reset，且跳过交互确认")
    args = parser.parse_args()

    reset = args.reset or args.force

    # 破坏性防护：--reset 需显式确认；--force 作为历史别名保留跳过确认的便利
    if reset and not args.force:
        answer = input("--reset 会 DROP 并重建全部四张表，清空现有数据。继续? [yes/N] ")
        if answer.strip().lower() not in ("yes", "y"):
            print("aborted")
            return

    conn = db.connect()
    try:
        # 单事务：schema + 播种一次性 commit，消除空库中间态
        with conn.cursor() as cur:
            if reset:
                drop_all(cur)
            apply_schema(cur)
            # 版本断言：apply_schema 之后立即校验，不匹配则终止（不 commit）
            assert_schema_version(cur)
            if reset:
                seed(cur)
            else:
                cur.execute("SELECT 1 FROM tasks LIMIT 1")
                if cur.fetchone():
                    print("tasks not empty, seeding skipped")
                else:
                    seed(cur)
        conn.commit()
    except SchemaVersionError as exc:
        conn.rollback()
        print("=" * 60, file=sys.stderr)
        print("FATAL: schema 版本断言失败，拒绝在未迁移的库上运行", file=sys.stderr)
        print(str(exc), file=sys.stderr)
        print("=" * 60, file=sys.stderr)
        sys.exit(1)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
