# tests/test_worker_fail.py —— worker 失败链 / 截断边界 / 围栏中止 / 心跳注入回归测试（对真实 PostgreSQL）
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from board import db, worker


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
def conn():
    c = db.connect()
    yield c
    c.close()


@pytest.fixture()
def make_task(conn):
    """工厂 fixture：按需创建带明显标记的临时任务（可定制 status/
    claimed_by/claimed_at/steps），测试结束后按外键顺序级联清理
    （step_logs → steps → tasks），不留演示脏数据。"""
    created = []

    def _make(status="pending", claimed_by=None, claimed_at_sql=None, steps=()):
        # claimed_at_sql 仅为测试内部常量片段（如 "now() - interval '120 seconds'"）
        frag = claimed_at_sql if claimed_at_sql else "NULL"
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO tasks (base_params, status, claimed_by, claimed_at) "
                f"VALUES ('{{}}'::jsonb, %s, %s, {frag}) RETURNING id",
                (status, claimed_by),
            )
            tid = cur.fetchone()[0]
            for idx in steps:
                cur.execute(
                    "INSERT INTO steps (task_id, step_index) VALUES (%s, %s)",
                    (tid, idx),
                )
        conn.commit()
        created.append(tid)
        return tid

    yield _make
    # 清理：先回滚可能残留的中止事务，再按外键顺序删
    try:
        conn.rollback()
    except Exception:
        pass
    for tid in reversed(created):
        with conn.cursor() as cur:
            cur.execute("DELETE FROM step_logs WHERE task_id = %s", (tid,))
            cur.execute("DELETE FROM steps WHERE task_id = %s", (tid,))
            cur.execute("DELETE FROM tasks WHERE id = %s", (tid,))
    conn.commit()


def _task_state(conn, tid):
    """回读 (status, claimed_by, claimed_at)。"""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status, claimed_by, claimed_at FROM tasks WHERE id = %s", (tid,)
        )
        return cur.fetchone()


def _epoch(conn, tid):
    """回读 claim_epoch（单调认领代数）。"""
    with conn.cursor() as cur:
        cur.execute("SELECT claim_epoch FROM tasks WHERE id = %s", (tid,))
        return cur.fetchone()[0]


def _claimed_at(conn, tid):
    """回读 claimed_at（租约/心跳刷新观察点）。"""
    with conn.cursor() as cur:
        cur.execute("SELECT claimed_at FROM tasks WHERE id = %s", (tid,))
        return cur.fetchone()[0]


def _log_count(conn, tid):
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM step_logs WHERE task_id = %s", (tid,))
        return cur.fetchone()[0]


# ---------- ① 失败链：step 抛异常 → failed + 失败日志行 ----------

def test_step_exception_marks_failed_with_failure_log(conn, make_task, monkeypatch):
    """首个 step 的执行 sleep 抛 RuntimeError → run_task 返回 False，
    任务翻 failed，step_logs 恰 1 行 success=False 且带 error_message。"""
    tid = make_task(status="claimed", claimed_by="W1", claimed_at_sql="now()", steps=(1, 2))

    def boom(*_a, **_k):
        raise RuntimeError("boom")

    # 模拟执行耗时的 sleep 改成抛错点；monkeypatch 随用例自动还原
    monkeypatch.setattr(worker.time, "sleep", boom)

    assert worker.run_task(conn, tid, "W1") is False

    status, claimed_by, _ = _task_state(conn, tid)
    assert status == "failed"
    assert claimed_by == "W1"  # 终态保留归属，供排查
    with conn.cursor() as cur:
        cur.execute(
            "SELECT success, worker_id, error_message FROM step_logs WHERE task_id = %s",
            (tid,),
        )
        rows = cur.fetchall()
    assert len(rows) == 1
    success, worker_id, error_message = rows[0]
    assert success is False
    assert worker_id == "W1"
    assert "boom" in error_message


# ---------- ② error_message 截断边界：600 字符入库恰 500 ----------

def test_error_message_truncated_to_500(conn, make_task, monkeypatch):
    """异常 str 长 600 字符，入库 error_message 必须被截到恰 500。"""
    tid = make_task(status="claimed", claimed_by="W1", claimed_at_sql="now()", steps=(1,))

    long_msg = "x" * 600

    def boom(*_a, **_k):
        raise RuntimeError(long_msg)

    monkeypatch.setattr(worker.time, "sleep", boom)

    assert worker.run_task(conn, tid, "W1") is False

    status, _, _ = _task_state(conn, tid)
    assert status == "failed"
    with conn.cursor() as cur:
        cur.execute(
            "SELECT error_message FROM step_logs WHERE task_id = %s", (tid,)
        )
        row = cur.fetchone()
    assert row is not None
    assert len(row[0]) == 500


# ---------- ③ 围栏中止：错误代际令牌 → 零副作用跳过 ----------

def test_wrong_epoch_aborts_with_zero_side_effects(conn, make_task):
    """以错误 claim_epoch（E+99）调 run_task：首次 claimed→running 翻转
    的双围栏即不匹配 → 走 release+skip 路径；release 带同一错误代际
    令牌同样不匹配 → 状态保持 claimed、归属与代际纹丝不动、零日志。
    该路径在任何写动作之前就返回，无需 monkeypatch sleep。"""
    tid = make_task(status="claimed", claimed_by="W1", claimed_at_sql="now()", steps=(1,))
    epoch = _epoch(conn, tid)

    assert worker.run_task(conn, tid, "W1", claim_epoch=epoch + 99) is False

    status, claimed_by, _ = _task_state(conn, tid)
    assert status == "claimed"          # 未被篡改为 failed/done/pending
    assert claimed_by == "W1"
    assert _epoch(conn, tid) == epoch   # release 未命中，epoch 不变
    assert _log_count(conn, tid) == 0   # 未执行任何 step，零日志


# ---------- ④ 心跳注入：interval=0.05 观察续租与 fenced 失权 ----------

def test_heartbeat_renews_lease_and_fenced_on_owner_change(conn, make_task):
    """直接用 Heartbeat 类（interval 参数注入，不经过 run_task）：
    前半段——持有者心跳把 claimed_at 刷新（与原值比较，≥1 次续租）；
    后半段——claimed_by 易主后围栏 UPDATE rowcount=0，fenced Event
    在 1s 内置位（确定性失权信号），stop() 正常收线。"""
    tid = make_task(
        status="running", claimed_by="W1",
        claimed_at_sql="now() - interval '30 seconds'", steps=(1,),
    )
    epoch = _epoch(conn, tid)
    old_claimed_at = _claimed_at(conn, tid)
    assert old_claimed_at is not None

    hb = worker.Heartbeat(tid, "W1", claim_epoch=epoch, interval=0.05)
    hb.start()
    try:
        # 防 flaky 口径：不用固定 sleep 等心跳，改 deadline 轮询等待
        # claimed_at 被续租刷新（条件达成即 break）；轮询间隔 0.05s，
        # 超时上限为原硬 sleep 0.3s 的 2 倍。
        deadline = time.monotonic() + 0.6
        while time.monotonic() < deadline:
            if _claimed_at(conn, tid) > old_claimed_at:
                break
            time.sleep(0.05)
        new_claimed_at = _claimed_at(conn, tid)
        # claimed_at 被刷新过（≥1 次续租）：新值严格大于回填的旧值
        assert new_claimed_at is not None
        assert new_claimed_at > old_claimed_at
        assert not hb.fenced.is_set()  # 持有者心跳绝不误报失权
    finally:
        hb.stop()

    # 易主：claimed_by 改为他人，旧心跳围栏 (claimed_by, epoch) 不再匹配
    with conn.cursor() as cur:
        cur.execute("UPDATE tasks SET claimed_by='W-other' WHERE id = %s", (tid,))
    conn.commit()

    hb2 = worker.Heartbeat(tid, "W1", claim_epoch=epoch, interval=0.05)
    hb2.start()
    try:
        # rowcount==0 → 确定性失权，fenced 必须及时置位（墙钟断言给足 1s 余量）
        assert hb2.fenced.wait(timeout=1.0) is True
    finally:
        hb2.stop()  # stop() 幂等收线：线程 join + 独立连接关闭
