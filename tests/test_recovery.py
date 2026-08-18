# tests/test_recovery.py —— reaper 租约回收 / fencing / 围栏写入回归测试（对真实 PostgreSQL）
import sys
from pathlib import Path

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


@pytest.fixture()
def park_pending(conn):
    """清场工厂：把演示队列里存量 pending 行临时挪进 done（claimed_by 打标记），
    保证 FIFO 认领（ORDER BY id）必然命中目标任务；teardown 按标记原样恢复
    pending，不伤演示数据、不留脏数据。exclude_id 用于豁免目标任务本身。"""
    MARK = "T-parked-by-test"

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
    # 恢复：标记行全部退回 pending（存量 pending 行本就无认领信息）
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE tasks SET status='pending', claimed_by=NULL, claimed_at=NULL "
            "WHERE claimed_by = %s",
            (MARK,),
        )
    conn.commit()


def _task_state(conn, tid):
    """回读 (status, claimed_by, claimed_at)。"""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status, claimed_by, claimed_at FROM tasks WHERE id = %s", (tid,)
        )
        return cur.fetchone()


def _log_count(conn, tid):
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM step_logs WHERE task_id = %s", (tid,))
        return cur.fetchone()[0]


def _epoch(conn, tid):
    """回读 claim_epoch（单调认领代数）。"""
    with conn.cursor() as cur:
        cur.execute("SELECT claim_epoch FROM tasks WHERE id = %s", (tid,))
        return cur.fetchone()[0]


def _finished_at(conn, tid):
    """回读 finished_at（终态打点，非终态应为 NULL）。"""
    with conn.cursor() as cur:
        cur.execute("SELECT finished_at FROM tasks WHERE id = %s", (tid,))
        return cur.fetchone()[0]


# ---------- ① reaper 回收：过期 claimed/running → pending 并清空认领信息 ----------

def test_reaper_reclaims_expired_claimed_and_running(conn, make_task):
    tid_claimed = make_task(
        status="claimed", claimed_by="W-dead",
        claimed_at_sql="now() - interval '120 seconds'",
    )
    tid_running = make_task(
        status="running", claimed_by="W-dead",
        claimed_at_sql="now() - interval '120 seconds'",
    )

    recovered = claim.reclaim_expired(conn, 60)
    assert tid_claimed in recovered
    assert tid_running in recovered

    for tid in (tid_claimed, tid_running):
        status, claimed_by, claimed_at = _task_state(conn, tid)
        assert status == "pending"
        assert claimed_by is None
        assert claimed_at is None


# ---------- ② 新鲜租约不回收 ----------

def test_fresh_lease_not_reclaimed(conn, make_task):
    tid = make_task(status="claimed", claimed_by="W-alive", claimed_at_sql="now()")

    recovered = claim.reclaim_expired(conn, 60)
    assert tid not in recovered

    status, claimed_by, claimed_at = _task_state(conn, tid)
    assert status == "claimed"
    assert claimed_by == "W-alive"
    assert claimed_at is not None


# ---------- ③ fencing：transition 的 claimed_by 围栏 ----------

def test_transition_fencing_blocks_wrong_owner(conn, make_task):
    tid = make_task(status="claimed", claimed_by="W1", claimed_at_sql="now()")

    # 非持有者 W2 的 CAS 翻转影响行数为 0 → False，状态纹丝不动
    assert claim.transition(conn, tid, "claimed", "running", claimed_by="W2") is False
    status, claimed_by, _ = _task_state(conn, tid)
    assert status == "claimed"
    assert claimed_by == "W1"

    # 真正的持有者 W1 正常推进
    assert claim.transition(conn, tid, "claimed", "running", claimed_by="W1") is True
    status, claimed_by, _ = _task_state(conn, tid)
    assert status == "running"
    assert claimed_by == "W1"


# ---------- ④ 围栏写入：report_step 的 owner 围栏 ----------

def test_report_step_owner_fence(conn, make_task):
    # running 且由 W1 持有；准备两个 step，分别验证围栏拦/放与 owner=None 直通
    tid = make_task(status="running", claimed_by="W1", claimed_at_sql="now()", steps=(1, 2))

    # 僵尸 worker W2（owner=W2 与 tasks.claimed_by=W1 不匹配）写不进日志
    assert logs.report_step(conn, tid, 1, True, "W2", owner="W2") is False
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM step_logs WHERE task_id=%s AND step_index=1", (tid,)
        )
        assert cur.fetchone()[0] == 0

    # 真正的持有者 W1 首次写入成功
    assert logs.report_step(conn, tid, 1, True, "W1", owner="W1") is True
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM step_logs WHERE task_id=%s AND step_index=1", (tid,)
        )
        assert cur.fetchone()[0] == 1

    # owner=None（API 手动通道语义）不受围栏限制；
    # 用 step 2 避开 step 1 已存在日志的 first-report-wins 干扰
    assert logs.report_step(conn, tid, 2, True, "W2", owner=None) is True
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM step_logs WHERE task_id=%s AND step_index=2", (tid,)
        )
        assert cur.fetchone()[0] == 1


# ---------- ⑤ 无 steps 任务：直接 running→done，不写任何日志 ----------

def test_no_steps_task_fast_forwards_to_done(conn, make_task):
    # 无 steps 的空任务，置 claimed 并由 T-test 持有（run_task 的翻转带围栏）
    tid = make_task(status="claimed", claimed_by="T-test", claimed_at_sql="now()")

    # 无 steps 路径无 sleep，可直接同步调用
    assert worker.run_task(conn, tid, "T-test") is True

    status, _, _ = _task_state(conn, tid)
    assert status == "done"
    assert _log_count(conn, tid) == 0  # 不伪造幽灵 step，step_logs 零行


# ---------- ⑥ 回收×幂等组合实证：first-report-wins 跨越回收重跑 ----------

def test_reclaim_rerun_keeps_first_report_and_advances(conn, make_task, park_pending, monkeypatch):
    """核心组合实证：W1 报完 step1 后租约过期被回收，W2 重认领接管——
    step1 日志仍恰 1 行且属 W1（first-report-wins 不被重跑覆盖），
    step2 由 W2 补上；同时锁定探针消歧：step1 重报返回 False 但新主人
    仍是合法持有者 → 继续执行而非误判失权中止。"""
    # W1 持有、新鲜租约；报 step1 成功（带双围栏）
    tid = make_task(status="claimed", claimed_by="W1", claimed_at_sql="now()", steps=(1, 2))
    epoch_old = _epoch(conn, tid)
    assert logs.report_step(conn, tid, 1, True, "W1", owner="W1", owner_epoch=epoch_old) is True

    # SQL 回填 claimed_at 制造过期（防 flaky 铁律：不 sleep 等过期）
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE tasks SET claimed_at = now() - interval '120 seconds' WHERE id = %s",
            (tid,),
        )
    conn.commit()
    recovered = claim.reclaim_expired(conn, 60)
    assert tid in recovered
    status, claimed_by, claimed_at = _task_state(conn, tid)
    assert (status, claimed_by, claimed_at) == ("pending", None, None)
    assert _epoch(conn, tid) == epoch_old  # 回收不清零 epoch：围栏代际连续

    # 清场（豁免目标任务）后队列应只剩它，保证 FIFO 认领命中
    park_pending(tid)
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM tasks WHERE status='pending'")
        assert cur.fetchone()[0] == 1

    # W2 重认领：新代 epoch 严格大于旧代
    claimed = claim.claim_next(conn, "W2")
    assert claimed is not None
    tid2, epoch_new = claimed
    assert tid2 == tid
    assert epoch_new > epoch_old

    # monkeypatch 掉 worker 全部 sleep 加速执行
    monkeypatch.setattr(worker.time, "sleep", lambda *a, **k: None)
    assert worker.run_task(conn, tid, "W2", claim_epoch=epoch_new) is True

    # 终态 done；step1 恰 1 行属 W1，step2 恰 1 行属 W2
    status, claimed_by, _ = _task_state(conn, tid)
    assert status == "done"
    with conn.cursor() as cur:
        cur.execute(
            "SELECT worker_id, success FROM step_logs WHERE task_id=%s AND step_index=1",
            (tid,),
        )
        assert cur.fetchall() == [("W1", True)]
        cur.execute(
            "SELECT worker_id, success FROM step_logs WHERE task_id=%s AND step_index=2",
            (tid,),
        )
        assert cur.fetchall() == [("W2", True)]


# ---------- ⑦ epoch 围栏专项：旧代令牌全拦、新代放行 ----------

def test_epoch_fencing_blocks_stale_generation(conn, make_task):
    """模拟夺权：同一任务易主且代际 +1 后，旧 (claimed_by, epoch) 的
    transition/report_step 全部 False 且状态纹丝不动；新代全部 True。"""
    tid = make_task(status="running", claimed_by="W1", claimed_at_sql="now()", steps=(1,))
    epoch_old = _epoch(conn, tid)

    # 手工模拟夺权：新主人 W2、代际 +1，status 保持 running
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE tasks SET claimed_by='W2', claim_epoch=claim_epoch+1 WHERE id=%s",
            (tid,),
        )
    conn.commit()
    epoch_new = _epoch(conn, tid)
    assert epoch_new == epoch_old + 1

    # 旧代全部拦死：翻转 False、日志写不进 False
    assert claim.transition(conn, tid, "running", "done",
                            claimed_by="W1", claim_epoch=epoch_old) is False
    assert logs.report_step(conn, tid, 1, True, "W1",
                            owner="W1", owner_epoch=epoch_old) is False
    status, claimed_by, _ = _task_state(conn, tid)
    assert (status, claimed_by) == ("running", "W2")  # 旧代写操作零副作用
    assert _log_count(conn, tid) == 0

    # 新代放行：日志首写成功、终态翻转成功
    assert logs.report_step(conn, tid, 1, True, "W2",
                            owner="W2", owner_epoch=epoch_new) is True
    assert claim.transition(conn, tid, "running", "done",
                            claimed_by="W2", claim_epoch=epoch_new) is True
    status, claimed_by, _ = _task_state(conn, tid)
    assert status == "done"
    with conn.cursor() as cur:
        cur.execute("SELECT worker_id FROM step_logs WHERE task_id=%s", (tid,))
        assert cur.fetchall() == [("W2",)]


# ---------- ⑧ claimed_at IS NULL 脏数据回收 ----------

def test_reclaim_null_claimed_at_dirty_row(conn, make_task):
    """claimed 却 claimed_at=NULL 的脏数据行也属于回收目标
    （WHERE 里 claimed_at IS NULL 分支），不能让它永远卡死队列。"""
    tid = make_task(status="claimed", claimed_by="W-dirty", claimed_at_sql="NULL")

    recovered = claim.reclaim_expired(conn, 60)
    assert tid in recovered

    status, claimed_by, claimed_at = _task_state(conn, tid)
    assert (status, claimed_by, claimed_at) == ("pending", None, None)


# ---------- ⑨ release 正反例：只有当前持有者能释放 ----------

def test_release_only_owner_succeeds(conn, make_task):
    # 正例：持有者 release 成功，三字段全部复位
    tid = make_task(status="claimed", claimed_by="W1", claimed_at_sql="now()")
    assert claim.release(conn, tid, "W1") is True
    status, claimed_by, claimed_at = _task_state(conn, tid)
    assert (status, claimed_by, claimed_at) == ("pending", None, None)

    # 反例：非持有者 release 返回 False，状态与归属纹丝不动
    tid2 = make_task(status="claimed", claimed_by="W1", claimed_at_sql="now()")
    assert claim.release(conn, tid2, "W2") is False
    status, claimed_by, _ = _task_state(conn, tid2)
    assert (status, claimed_by) == ("claimed", "W1")


# ---------- ⑩ finished_at：仅终态打点，running 不打 ----------

def test_finished_at_stamped_only_on_terminal(conn, make_task):
    # claimed→running：非终态翻转，finished_at 保持 NULL
    tid = make_task(status="claimed", claimed_by="W1", claimed_at_sql="now()")
    assert claim.transition(conn, tid, "claimed", "running", claimed_by="W1") is True
    assert _finished_at(conn, tid) is None

    # running→done：终态打点
    assert claim.transition(conn, tid, "running", "done", claimed_by="W1") is True
    assert _finished_at(conn, tid) is not None

    # claimed→failed：另一条终态路径同样打点
    tid2 = make_task(status="claimed", claimed_by="W1", claimed_at_sql="now()")
    assert claim.transition(conn, tid2, "claimed", "failed", claimed_by="W1") is True
    assert _finished_at(conn, tid2) is not None


# ---------- ⑪ 中-10 transition 状态机白名单：非法组合参数层直接 ValueError ----------

@pytest.mark.parametrize("from_status,to_status", [
    ("pending", "done"),       # 评审点名三例
    ("done", "running"),
    ("failed", "claimed"),
    ("pending", "running"),    # 认领必须走 claim_next，不许直翻
    ("pending", "claimed"),
    ("claimed", "done"),       # 未经 running 不得直达终态
    ("running", "claimed"),    # 回退路径不存在（回收是 reaper 专属）
    ("running", "pending"),
    ("done", "pending"),
    ("failed", "running"),
    ("done", "failed"),        # 终态之间亦不可互转
])
def test_transition_whitelist_rejects_illegal_pairs(from_status, to_status):
    """非法 (from, to) 组合在参数校验层直接抛 ValueError，早于任何 SQL
    （conn 不会被触碰）；消息格式钉死现有异常文案，供调用方稳定匹配。"""
    pattern = (rf"illegal transition: from_status={from_status!r} -> "
               rf"to_status={to_status!r} not in allowed state machine")
    with pytest.raises(ValueError, match=pattern):
        claim.transition(None, 1, from_status, to_status)
