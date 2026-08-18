# tests/test_stepidx.py —— 非连续 step_index 回归用例（自 tests/_smoke_stepidx.py 移植）
#
# 覆盖场景：steps 编号非连续（2/5/7）时，worker 必须按【真实步号】上报
# （step_logs 恰为这些步号各一行），且末步完成后 current_step clamp 在
# 真实末步 7（不出现越界显示）。
#
# 与原脚本的差异：
#   - 模拟执行耗时降为近零：monkeypatch 掉 worker.time.sleep 与
#     worker.random.uniform，不真实 sleep 数秒；
#   - 数据生命周期交给 fixture teardown（yield 后按外键顺序级联清理），
#     异常路径也保证不留 status='claimed' 的脏任务；
#   - 命中 conftest 注入的隔离库 taskboard_test，演示库零写入。
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from board import db, worker


def _db_available():
    """DB 不在（无 DATABASE_URL/.env 或服务未启动）时返回 False，
    让本模块 skip 而非 error；PG 可达时 conftest 已把 DATABASE_URL
    注入为 taskboard_test，本函数自然为 True、用例真实执行。"""
    try:
        c = db.connect()
    except Exception:
        return False
    c.close()
    return True


pytestmark = pytest.mark.skipif(
    not _db_available(), reason="PostgreSQL not reachable (set DATABASE_URL or .env)"
)

# 刻意非连续编号：覆盖"步号不从 1 连续"的口径
STEP_INDICES = (2, 5, 7)
WORKER_ID = "W-stepidx"


@pytest.fixture()
def conn():
    c = db.connect()
    yield c
    c.close()


@pytest.fixture()
def noncontig_task(conn):
    """创建带非连续 step 2/5/7 的临时任务并置为 claimed（模拟已认领）。
    teardown 按外键顺序级联清理（step_logs → steps → tasks）：
    无论用例成败都不留 status='claimed' 的脏任务。"""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO tasks (base_params) VALUES ('{\"a\":1}'::jsonb) RETURNING id"
        )
        tid = cur.fetchone()[0]
        for si in STEP_INDICES:
            cur.execute(
                "INSERT INTO steps (task_id, step_index, override_params) VALUES (%s,%s,%s)",
                (tid, si, json.dumps({"a": si * 100}, allow_nan=False)),
            )
        cur.execute(
            "UPDATE tasks SET status='claimed', claimed_by=%s WHERE id=%s",
            (WORKER_ID, tid),
        )
    conn.commit()

    yield tid

    try:
        conn.rollback()  # 回滚用例中可能残留的中止事务
    except Exception:
        pass
    with conn.cursor() as cur:
        cur.execute("DELETE FROM step_logs WHERE task_id=%s", (tid,))
        cur.execute("DELETE FROM steps WHERE task_id=%s", (tid,))
        cur.execute("DELETE FROM tasks WHERE id=%s", (tid,))
    conn.commit()


def test_noncontiguous_steps_reported_at_real_indices_and_clamped(
    conn, noncontig_task, monkeypatch
):
    tid = noncontig_task

    # 模拟执行耗时降为近零：sleep 空实现、uniform 恒返回 0（不得真实 sleep）
    monkeypatch.setattr(worker.time, "sleep", lambda *a, **k: None)
    monkeypatch.setattr(worker.random, "uniform", lambda a, b: 0.0)

    assert worker.run_task(conn, tid, WORKER_ID) is True

    with conn.cursor() as cur:
        cur.execute(
            "SELECT step_index, success, worker_id FROM step_logs "
            "WHERE task_id=%s ORDER BY step_index",
            (tid,),
        )
        logs = cur.fetchall()
        cur.execute("SELECT status, current_step FROM tasks WHERE id=%s", (tid,))
        status, current_step = cur.fetchone()

    # 日志落在真实编号 2/5/7 各恰一行，且归属本 worker
    assert logs == [(si, True, WORKER_ID) for si in STEP_INDICES], f"unexpected logs: {logs}"
    # 状态 done；current_step clamp 在真实末步 7（不再出现越界步号）
    assert status == "done", f"unexpected status: {status}"
    assert current_step == STEP_INDICES[-1], f"current_step not clamped: {current_step}"
