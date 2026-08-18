# tests/test_blindspots.py —— T7 盲区补测：心跳韧性二分 / 建连重试三件套 / reaper×心跳×上报时序
#
# 覆盖三处此前无专项回归的并发盲区（对真实 PostgreSQL，走 conftest 隔离库）：
#   ① Heartbeat 失权判定的严格二分：
#      - 连接/建连异常（OperationalError）≠ 失权：重建连接后继续续租，
#        fenced 【不】置位；
#      - UPDATE rowcount==0（围栏不匹配，SQL 回填制造失权）= 确定性失权：
#        fenced 置位。
#      用 interval 参数注入可控短周期，不真实等待长租约。
#   ② db.connect 建连重试三件套：前 N 次 OperationalError 后恢复 → 有限
#      退避重试后成功（断言尝试次数与退避序列）；重试耗尽 → 原样抛错。
#   ③ reaper×心跳×上报时序：任务 claimed 后 SQL 回填 claimed_at 制造过期
#      （全程不 sleep），reclaim_expired 回收瞬间前后：
#      - 回收前原持有者带围栏上报仍可写（仍合法持有）；
#      - 回收后原持有者（owner+epoch 围栏）上报返回 False 且 step_logs 无脏行，
#        心跳首拍即 fenced；
#      - 新持有者重认领（epoch 严格递增）后正常写入，旧代迟到上报 0 写入。
import sys
import time
from pathlib import Path

import psycopg
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from board import claim, db, logs, worker


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
    """工厂 fixture：按需创建临时任务，测试结束按外键顺序级联清理
    （step_logs → steps → tasks），与 test_recovery.py 同风格。"""
    created = []

    def _make(status="pending", claimed_by=None, claimed_at_sql=None, steps=()):
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


@pytest.fixture()
def park_pending(conn):
    """清场工厂：把存量 pending 行临时挪进 done（打标记），保证 FIFO
    认领必然命中目标任务；teardown 原样恢复。与 test_recovery.py 同风格。"""
    MARK = "T-parked-by-blindspots"

    def _park(exclude_id=None):
        sql = "UPDATE tasks SET status='done', claimed_by=%s WHERE status='pending'"
        args = [MARK]
        if exclude_id is not None:
            sql += " AND id <> %s"
            args.append(exclude_id)
        with conn.cursor() as cur:
            cur.execute(sql, tuple(args))
        conn.commit()

    yield _park
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE tasks SET status='pending', claimed_by=NULL, claimed_at=NULL "
            "WHERE claimed_by = %s",
            (MARK,),
        )
    conn.commit()


def _epoch(conn, tid):
    with conn.cursor() as cur:
        cur.execute("SELECT claim_epoch FROM tasks WHERE id = %s", (tid,))
        return cur.fetchone()[0]


def _claimed_at(conn, tid):
    with conn.cursor() as cur:
        cur.execute("SELECT claimed_at FROM tasks WHERE id = %s", (tid,))
        return cur.fetchone()[0]


def _wait_until(check, timeout=3.0, interval=0.02):
    """轮询等待 check() 为真（短间隔），超时返回 False。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check():
            return True
        time.sleep(interval)
    return False


# ---------- ①-a Heartbeat：连接损坏/建连失败 ≠ 失权，重建后继续续租 ----------

def test_heartbeat_rebuilds_connection_and_keeps_renewing(conn, make_task, monkeypatch):
    """心跳独立连接损坏后：db.connect 连续抛 OperationalError 若干轮，
    心跳仅重建重试、fenced 绝不置位；连接恢复后续租继续（claimed_at 前进）。"""
    tid = make_task(status="running", claimed_by="W-hb", claimed_at_sql="now()", steps=(1,))
    epoch = _epoch(conn, tid)

    hb = worker.Heartbeat(tid, "W-hb", claim_epoch=epoch, interval=0.05)
    hb.start()  # 首次建连走真实路径，拿到可用连接
    try:
        claimed_at_0 = _claimed_at(conn, tid)

        # monkeypatch db.connect：前 3 次抛 OperationalError，之后恢复
        state = {"fails": 0}

        def flaky_connect(*args, **kwargs):
            if state["fails"] < 3:
                state["fails"] += 1
                raise psycopg.OperationalError("simulated connection loss")
            return psycopg.connect(db.database_url())

        monkeypatch.setattr(db, "connect", flaky_connect)

        # 模拟心跳连接损坏：丢弃旧连接 → 下一拍进入 _reconnect 走 flaky 路径
        old = hb._conn
        hb._conn = None
        old.close()

        # 等待续租成功：claimed_at 前进（重建连接后的 UPDATE 生效）
        assert _wait_until(lambda: _claimed_at(conn, tid) > claimed_at_0), \
            "heartbeat did not resume renewing after reconnect"
        assert state["fails"] == 3            # 确实历经 3 次建连失败才重建成功
        assert not hb.fenced.is_set()         # 连接损坏≠失权：fenced 绝不置位
    finally:
        hb.stop()


# ---------- ①-b Heartbeat：UPDATE rowcount==0 = 确定性失权，fenced 置位 ----------

def test_heartbeat_fenced_on_rowcount_zero(conn, make_task):
    """SQL 回填制造失权（易主 + 代际 +1）：心跳围栏 UPDATE 匹配 0 行，
    fenced 事件必须置位（确定性失权信号，与连接异常严格二分）。"""
    tid = make_task(status="running", claimed_by="W-hb", claimed_at_sql="now()", steps=(1,))
    epoch = _epoch(conn, tid)

    hb = worker.Heartbeat(tid, "W-hb", claim_epoch=epoch, interval=0.05)
    hb.start()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE tasks SET claimed_by='W-other', claim_epoch=claim_epoch+1 "
                "WHERE id=%s",
                (tid,),
            )
        conn.commit()
        assert hb.fenced.wait(timeout=3.0), "fenced event not set after ownership loss"
    finally:
        hb.stop()


# ---------- ② db.connect 建连重试三件套 ----------

def test_db_connect_retries_then_succeeds(monkeypatch):
    """psycopg.connect 前 2 次抛 OperationalError、第 3 次成功：
    db.connect 有限退避重试后拿到连接；尝试次数与退避序列符合常量约定。"""
    calls = {"n": 0}
    sleeps = []
    real_connect = psycopg.connect  # 先拿到真实函数引用，再打补丁，避免自递归

    def flaky(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] <= 2:
            raise psycopg.OperationalError("simulated outage")
        return real_connect(db.database_url())

    monkeypatch.setattr(db.psycopg, "connect", flaky)
    monkeypatch.setattr(db.time, "sleep", lambda s: sleeps.append(s))

    c = db.connect()
    try:
        assert calls["n"] == 3                        # 首次 + 2 次重试后成功
        assert sleeps == list(db._RETRY_BACKOFF)      # 退避序列 (0.2, 0.5)
    finally:
        c.close()


def test_db_connect_raises_after_retries_exhausted(monkeypatch):
    """psycopg.connect 持续抛 OperationalError：重试耗尽后原样抛出
    OperationalError，总尝试次数 == _MAX_RETRIES + 1。"""
    calls = {"n": 0}

    def always_fail(*args, **kwargs):
        calls["n"] += 1
        raise psycopg.OperationalError("persistent outage")

    monkeypatch.setattr(db.psycopg, "connect", always_fail)
    monkeypatch.setattr(db.time, "sleep", lambda s: None)

    with pytest.raises(psycopg.OperationalError):
        db.connect()
    assert calls["n"] == db._MAX_RETRIES + 1          # 共 3 次尝试，不多不少


# ---------- ③ reaper×心跳×上报时序 ----------

def test_reaper_heartbeat_report_timing(conn, make_task, park_pending):
    """claimed → SQL 回填过期 → reclaim 回收瞬间前后的心跳/围栏行为全谱：
    回收前持有者围栏上报合法可写；回收后旧主上报 False 且零脏行、
    心跳首拍 fenced；新主重认领（epoch 递增）正常写入，旧代迟到写 0 行。
    全程 SQL 回填制造时间条件，不 sleep。"""
    tid = make_task(status="claimed", claimed_by="W1", claimed_at_sql="now()", steps=(1, 2))
    epoch_old = _epoch(conn, tid)

    # 回收前一刻：W1 仍是合法持有者，带双围栏上报可写（step 1）
    assert logs.report_step(conn, tid, 1, True, "W1",
                            owner="W1", owner_epoch=epoch_old) is True

    # SQL 回填 claimed_at 制造过期（防 flaky 铁律：不 sleep 等过期）
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE tasks SET claimed_at = now() - interval '120 seconds' WHERE id=%s",
            (tid,),
        )
    conn.commit()

    recovered = claim.reclaim_expired(conn, 60)
    assert tid in recovered

    # 回收后一刻：旧持有者对未完成 step 的围栏上报返回 False，零脏行
    assert logs.report_step(conn, tid, 2, True, "W1",
                            owner="W1", owner_epoch=epoch_old) is False
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM step_logs WHERE task_id=%s AND step_index=2", (tid,)
        )
        assert cur.fetchone()[0] == 0
        cur.execute(
            "SELECT worker_id, step_index FROM step_logs WHERE task_id=%s "
            "ORDER BY step_index", (tid,)
        )
        assert cur.fetchall() == [("W1", 1)]  # 仅存 step1 的合法首报

    # 心跳此刻启动：首拍围栏 UPDATE 匹配 0 行 → fenced 置位（确定性失权）
    hb = worker.Heartbeat(tid, "W1", claim_epoch=epoch_old, interval=0.05)
    hb.start()
    try:
        assert hb.fenced.wait(timeout=3.0), "heartbeat not fenced after reclaim"
    finally:
        hb.stop()

    # 新持有者重认领：epoch 严格递增，围栏写入正常
    park_pending(tid)
    claimed = claim.claim_next(conn, "W2")
    assert claimed is not None and claimed[0] == tid
    epoch_new = claimed[1]
    assert epoch_new > epoch_old
    assert logs.report_step(conn, tid, 2, True, "W2",
                            owner="W2", owner_epoch=epoch_new) is True

    # 旧代迟到上报：代际围栏 + first-report-wins 双重拦死，行数不变
    assert logs.report_step(conn, tid, 2, True, "W1",
                            owner="W1", owner_epoch=epoch_old) is False
    with conn.cursor() as cur:
        cur.execute(
            "SELECT worker_id, success FROM step_logs "
            "WHERE task_id=%s AND step_index=2", (tid,)
        )
        assert cur.fetchall() == [("W2", True)]
