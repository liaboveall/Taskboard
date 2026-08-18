# tests/test_idempotent_log.py —— step_logs 幂等性测试（对真实 PostgreSQL）
import sys
import threading
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from board import db, logs


def _db_available():
    """DB 不在（无 DATABASE_URL/.env 或服务未启动）时返回 False，
    让本模块 skip 而非 error，保证套件在无库环境可移植。"""
    try:
        c = db.connect()
    except Exception:
        return False
    c.close()
    return True


pytestmark = pytest.mark.skipif(
    not _db_available(), reason="PostgreSQL not reachable (set DATABASE_URL or .env)"
)


@pytest.fixture()
def task_id():
    """在事务内创建一条测试任务，测试结束后级联清理。"""
    conn = db.connect()
    with conn.cursor() as cur:
        cur.execute("INSERT INTO tasks (base_params) VALUES ('{}'::jsonb) RETURNING id")
        tid = cur.fetchone()[0]
        cur.execute("INSERT INTO steps (task_id, step_index) VALUES (%s, 1)", (tid,))
    conn.commit()
    yield tid
    # 清理：先删日志再删任务（外键顺序）
    with conn.cursor() as cur:
        cur.execute("DELETE FROM step_logs WHERE task_id = %s", (tid,))
        cur.execute("DELETE FROM steps WHERE task_id = %s", (tid,))
        cur.execute("DELETE FROM tasks WHERE id = %s", (tid,))
    conn.commit()
    conn.close()


def count_logs(conn, task_id, step_index=1):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*), bool_and(success) FROM step_logs WHERE task_id=%s AND step_index=%s",
            (task_id, step_index),
        )
        return cur.fetchone()


def test_repeat_report_same_connection(task_id):
    """同一连接重复上报 3 次 → 恰好 1 行。"""
    conn = db.connect()
    try:
        first = logs.report_step(conn, task_id, 1, True, "W-test")
        second = logs.report_step(conn, task_id, 1, True, "W-test")
        third = logs.report_step(conn, task_id, 1, True, "W-test")
        assert first is True
        assert second is False
        assert third is False
        n, all_success = count_logs(conn, task_id)
        assert n == 1 and all_success
    finally:
        conn.close()


def test_first_success_wins_against_failures(task_id):
    """先报 success=True，再用多连接/多线程报 success=False ×4
    → 仍恰好 1 行且 success=True（first-report-wins 结构性保证）。"""
    conn = db.connect()
    try:
        assert logs.report_step(conn, task_id, 1, True, "W-first") is True
    finally:
        conn.close()

    errors = []

    def report_fail(i):
        try:
            c = db.connect()  # 每个线程独立连接
            try:
                assert logs.report_step(c, task_id, 1, False, f"W-fail-{i}") is False
            finally:
                c.close()
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=report_fail, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors

    conn = db.connect()
    try:
        n, all_success = count_logs(conn, task_id)
        assert n == 1 and all_success
        with conn.cursor() as cur:
            cur.execute("SELECT worker_id FROM step_logs WHERE task_id=%s", (task_id,))
            assert cur.fetchone()[0] == "W-first"  # 第一条记录的写入者未被覆盖
    finally:
        conn.close()


def test_fail_first_wins_against_later_success(task_id):
    """fail-first → success-later（评审修复 5）：first-report-wins 的双向证明。

    先报 success=False 首次写入成功（True）；再报 success=True 被主键
    挡下（False），且库中该行仍为 success=False——先到的失败记录同样
    结构性地不可被后续成功覆盖。
    """
    conn = db.connect()
    try:
        assert logs.report_step(conn, task_id, 1, False, "W-fail-first") is True
        assert logs.report_step(conn, task_id, 1, True, "W-success-later") is False
        n, all_success = count_logs(conn, task_id)
        assert n == 1
        assert all_success is False  # 库里现存行仍是首次写入的失败记录
        with conn.cursor() as cur:
            cur.execute("SELECT worker_id FROM step_logs WHERE task_id=%s", (task_id,))
            assert cur.fetchone()[0] == "W-fail-first"
    finally:
        conn.close()


def test_error_message_column(task_id):
    """失败上报携带 error_message 入库；不传时该列为 NULL。"""
    conn = db.connect()
    try:
        # 补插 step 2（fixture 只建了 step 1），用于对照“不传 error_message”的场景
        with conn.cursor() as cur:
            cur.execute("INSERT INTO steps (task_id, step_index) VALUES (%s, 2)", (task_id,))
        conn.commit()

        assert logs.report_step(conn, task_id, 1, False, "W1", error_message="boom") is True
        assert logs.report_step(conn, task_id, 2, True, "W1") is True

        with conn.cursor() as cur:
            cur.execute(
                "SELECT error_message FROM step_logs WHERE task_id=%s AND step_index=1",
                (task_id,),
            )
            assert cur.fetchone()[0] == "boom"
            cur.execute(
                "SELECT error_message FROM step_logs WHERE task_id=%s AND step_index=2",
                (task_id,),
            )
            assert cur.fetchone()[0] is None  # 未传 error_message → NULL
    finally:
        conn.close()


def test_two_connections_near_simultaneous(task_id):
    """两个独立连接近乎同时上报同一 step → 恰好 1 行。"""
    c1, c2 = db.connect(), db.connect()
    barrier = threading.Barrier(2)
    results = []

    def report(conn, wid):
        barrier.wait(timeout=10)  # 对齐起跑时间，制造并发碰撞
        results.append(logs.report_step(conn, task_id, 1, True, wid))

    t1 = threading.Thread(target=report, args=(c1, "W-a"))
    t2 = threading.Thread(target=report, args=(c2, "W-b"))
    t1.start(); t2.start()
    t1.join(); t2.join()
    c1.close(); c2.close()

    # 恰好一个插入成功
    assert sorted(results) == [False, True]

    conn = db.connect()
    try:
        n, all_success = count_logs(conn, task_id)
        assert n == 1 and all_success
    finally:
        conn.close()
