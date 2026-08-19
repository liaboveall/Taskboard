# tests/test_api.py —— report 端点回归测试（404/409/状态门/状态不变/并发/400）
#                    + list_tasks 组装契约 / 500 兜底 / HTTPException JSON 化 /
#                    conflict_note / expected_owner 持有者校验
#                    + T5 契约收紧：序号门/持有者强制/error_code/413/限流/
#                    healthz/ETag 304/claim_epoch/连接池隔离复位
#
# DB 用例沿用既有 skipif 无库跳过模式；每个 DB 用例用带明显标记的
# 独立临时任务并在 teardown 清理（step_logs → steps → tasks）。
import contextlib
import json
import sys
import threading
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from board import api as api_mod
from board import db
from board.api import app


def _db_available():
    """DB 不在（无 DATABASE_URL/.env 或服务未启动）时返回 False，
    让 DB 用例 skip 而非 error，保证套件在无库环境可移植。"""
    try:
        c = db.connect()
    except Exception:
        return False
    c.close()
    return True


needs_db = pytest.mark.skipif(
    not _db_available(), reason="PostgreSQL not reachable (set DATABASE_URL or .env)"
)


@pytest.fixture()
def client():
    return app.test_client()


@pytest.fixture(autouse=True)
def _clean_rate_limit():
    """限流是模块级内存状态：test_client 全部共享 127.0.0.1 这个 IP，
    不清账会让存量用例被跨用例累积的计数误伤 429；
    并发用例自建 test_client 也能被覆盖（autouse 对每个用例生效）。"""
    api_mod._reset_rate_limit()
    yield
    api_mod._reset_rate_limit()


@pytest.fixture()
def conn():
    c = db.connect()
    yield c
    c.close()


@pytest.fixture()
def make_task(conn):
    """工厂 fixture：创建临时任务（可定制 status/claimed_by/steps 编号/
    current_step），测试结束后按外键顺序级联清理，不留演示脏数据。"""
    created = []

    def _make(status="pending", claimed_by=None, steps=(1,), current_step=None):
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO tasks (base_params, status, claimed_by) "
                "VALUES ('{}'::jsonb, %s, %s) RETURNING id",
                (status, claimed_by),
            )
            tid = cur.fetchone()[0]
            for idx in steps:
                cur.execute("INSERT INTO steps (task_id, step_index) VALUES (%s, %s)",
                            (tid, idx))
            if current_step is not None:
                cur.execute("UPDATE tasks SET current_step = %s WHERE id = %s",
                            (current_step, tid))
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


# ---------- ⑤ 400：success 非布尔（纯参数校验，不碰 DB） ----------

def test_report_400_non_boolean_success(client):
    resp = client.post("/api/tasks/1/steps/1/report", json={"success": "notbool"})
    assert resp.status_code == 400
    assert "error" in resp.get_json()


# ---------- ① 404：任务不存在 / 步骤不存在 ----------

@needs_db
def test_report_404_unknown_task_and_unknown_step(client, make_task):
    tid = make_task(status="running", claimed_by="W-api")

    resp = client.post("/api/tasks/999999999/steps/1/report", json={"success": True})
    assert resp.status_code == 404
    assert "error" in resp.get_json()

    # 任务存在但 step_index 不在 steps 表（steps 表是唯一事实源）
    resp = client.post(f"/api/tasks/{tid}/steps/999/report", json={"success": True})
    assert resp.status_code == 404
    assert "error" in resp.get_json()


# ---------- ② 409 状态门：pending / done 任务拒绝上报 ----------

@needs_db
def test_report_409_for_pending_and_done(client, make_task):
    tid_pending = make_task(status="pending")
    tid_done = make_task(status="done")

    resp = client.post(f"/api/tasks/{tid_pending}/steps/1/report", json={"success": True})
    assert resp.status_code == 409
    assert "error" in resp.get_json()

    resp = client.post(f"/api/tasks/{tid_done}/steps/1/report", json={"success": True})
    assert resp.status_code == 409
    assert "error" in resp.get_json()


# ---------- ③ 状态不变（钉死 H2 修复）：API 只写 step_logs，绝不流转状态 ----------

@needs_db
@pytest.mark.parametrize("status", ["claimed", "running"])
def test_report_does_not_change_task_status(client, conn, make_task, status):
    tid = make_task(status=status, claimed_by="W-api")

    resp = client.post(
        f"/api/tasks/{tid}/steps/1/report",
        json={"success": True, "expected_owner": "W-api"},
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["received"] == 1
    assert body["inserted"] == 1

    # tasks.status 仍为原状态：API 手动通道不再做任何状态流转
    with conn.cursor() as cur:
        cur.execute("SELECT status FROM tasks WHERE id = %s", (tid,))
        assert cur.fetchone()[0] == status


# ---------- ④ 并发：5 线程同时上报同一 (task, step) → inserted 合计恰为 1 ----------

@needs_db
def test_concurrent_reports_insert_exactly_once(make_task):
    tid = make_task(status="running", claimed_by="W-api")
    inserted_sum, errors = [], []
    barrier = threading.Barrier(5)

    def hit():
        try:
            c = app.test_client()  # 每线程各自独立的 test_client
            barrier.wait(timeout=10)
            resp = c.post(
                f"/api/tasks/{tid}/steps/1/report",
                json={"success": True, "expected_owner": "W-api"},
            )
            assert resp.status_code == 200
            body = resp.get_json()
            assert body["received"] == 1  # 每个响应都 received=1
            inserted_sum.append(body["inserted"])
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=hit) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert len(inserted_sum) == 5
    assert sum(inserted_sum) == 1  # (task_id, step_index) 主键幂等：恰好一行


# ---------- ⑥ GET /api/tasks 组装契约：tasks 内嵌 steps/log，log_count 正确 ----------

@needs_db
def test_list_tasks_assembly_contract(client, conn, make_task):
    """造一个 3-step 任务、只给 step 1/3 写日志，断言 list_tasks 的组装口径：
    steps 内嵌且按 step_index 升序；有日志 step 的 log 结构完整；
    无日志 step 的 log 为 None；log_count == 有日志的 step 数。"""
    tid = make_task(status="running", claimed_by="W-list")
    # make_task 已建 step 1；追加 step 2/3，并只给 step 1/3 插入日志
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO steps (task_id, step_index, override_params) "
            "VALUES (%s, 2, '{}'::jsonb)", (tid,),
        )
        cur.execute(
            "INSERT INTO steps (task_id, step_index, override_params) "
            "VALUES (%s, 3, '{\"k\": \"v\"}'::jsonb)", (tid,),
        )
        cur.execute(
            "INSERT INTO step_logs (task_id, step_index, success, worker_id) "
            "VALUES (%s, 1, true, 'W-list')", (tid,),
        )
        cur.execute(
            "INSERT INTO step_logs (task_id, step_index, success, worker_id, error_message) "
            "VALUES (%s, 3, false, 'W-list', 'boom')", (tid,),
        )
    conn.commit()

    resp = client.get("/api/tasks")
    assert resp.status_code == 200
    tasks = resp.get_json()
    assert isinstance(tasks, list)
    mine = [t for t in tasks if t["id"] == tid]
    assert len(mine) == 1
    t = mine[0]

    # 顶层字段齐全且口径正确
    assert t["status"] == "running"
    assert t["claimed_by"] == "W-list"
    assert t["base_params"] == {}

    # steps 内嵌且按 step_index 升序组装
    assert [s["step_index"] for s in t["steps"]] == [1, 2, 3]
    step1, step2, step3 = t["steps"]

    # 有日志的 step：log 字段结构完整
    assert step1["log"] is not None
    assert step1["log"]["success"] is True
    assert step1["log"]["worker_id"] == "W-list"
    assert step1["log"]["reported_at"]          # ISO 字符串非空
    assert step1["log"]["error_message"] is None
    assert step3["log"]["success"] is False
    assert step3["log"]["error_message"] == "boom"

    # 无日志的 step：log 为 None（而不是缺键）
    assert step2["log"] is None

    # override_params 原样带出
    assert step3["override_params"] == {"k": "v"}

    # log_count = 有日志的 step 数（2/3）
    assert t["log_count"] == 2


# ---------- ⑦ 畸形 body：空 body / 非法 JSON / 缺 success / 非 dict 一律 400 ----------

def test_report_400_empty_body(client):
    # 完全不带 body：get_json(silent=True) 得 None → 400 拒绝
    resp = client.post("/api/tasks/1/steps/1/report")
    assert resp.status_code == 400
    assert "error" in resp.get_json()


def test_report_400_invalid_json(client):
    # 声明了 application/json 但内容是非法 JSON 字符串 → 400 拒绝
    resp = client.post(
        "/api/tasks/1/steps/1/report", data="not json",
        content_type="application/json",
    )
    assert resp.status_code == 400
    assert "error" in resp.get_json()


def test_report_400_missing_success(client):
    # 合法 JSON 对象但缺 success 键：不允许脑补默认 True → 400
    resp = client.post("/api/tasks/1/steps/1/report", json={})
    assert resp.status_code == 400
    assert "error" in resp.get_json()


def test_report_400_non_dict_json(client):
    # 合法 JSON 但非 dict（数组）→ 400 拒绝
    resp = client.post(
        "/api/tasks/1/steps/1/report", data="[1, 2]",
        content_type="application/json",
    )
    assert resp.status_code == 400
    assert "error" in resp.get_json()


# ---------- ⑧ 409 状态门补全：failed 终态同样拒绝（pending/done 已有存量用例） ----------

@needs_db
def test_report_409_for_failed(client, make_task):
    tid = make_task(status="failed")
    resp = client.post(f"/api/tasks/{tid}/steps/1/report", json={"success": True})
    assert resp.status_code == 409
    assert "error" in resp.get_json()


# ---------- ⑨ 500 兜底：未捕获异常走 handle_unexpected，返回 JSON 而非 HTML ----------

def test_500_handler_returns_json(client, monkeypatch):
    # list_tasks 已改走连接池 db.acquire()（T5）：替换它为抛异常的
    # 上下文管理器，即可复现建连/取连失败 → 500 JSON 兜底
    @contextlib.contextmanager
    def boom():
        raise RuntimeError("db down")
        yield  # pragma: no cover

    monkeypatch.setattr(db, "acquire", boom)
    resp = client.get("/api/tasks")
    assert resp.status_code == 500
    body = resp.get_json()
    assert body is not None and "error" in body


# ---------- ⑩ HTTPException JSON 化：未知路由 404 也返回 JSON 而非 HTML ----------

def test_http_exception_json_404(client):
    resp = client.get("/api/nonexistent")
    assert resp.status_code == 404
    assert "application/json" in resp.content_type
    body = resp.get_json()
    assert body is not None and "error" in body


# ---------- ⑪ conflict_note：口径冲突的重复上报显式提示，同值重复不提示 ----------

@needs_db
def test_report_conflict_note_and_same_value_duplicate(client, make_task):
    tid = make_task(status="running", claimed_by="W-api")

    # 首次上报 success=True：真实写入（有主任务必须携带 expected_owner）
    first = client.post(
        f"/api/tasks/{tid}/steps/1/report",
        json={"success": True, "expected_owner": "W-api"},
    )
    assert first.status_code == 200
    assert first.get_json()["inserted"] == 1

    # 口径相反的重复上报：first-report-wins，inserted=0 且带 conflict_note
    dup = client.post(
        f"/api/tasks/{tid}/steps/1/report",
        json={"success": False, "expected_owner": "W-api"},
    )
    assert dup.status_code == 200
    body = dup.get_json()
    assert body["inserted"] == 0
    assert body["duplicates_ignored"] == 1
    assert body["conflict_note"] == "first-report-wins: existing log kept"
    # 回读的现存日志行保持首次上报的口径
    assert body["log_row"]["success"] is True

    # 同值重复：同样幂等忽略，但没有口径冲突 → 不应带 conflict_note
    same = client.post(
        f"/api/tasks/{tid}/steps/1/report",
        json={"success": True, "expected_owner": "W-api"},
    )
    assert same.status_code == 200
    sbody = same.get_json()
    assert sbody["inserted"] == 0
    assert sbody["duplicates_ignored"] == 1
    assert "conflict_note" not in sbody


# ---------- ⑫ expected_owner 持有者校验：匹配放行 / 不匹配 409 / force 逃生门 / 无主任务 ----------

@needs_db
def test_report_expected_owner_match(client, make_task):
    tid = make_task(status="running", claimed_by="W-owner")
    resp = client.post(
        f"/api/tasks/{tid}/steps/1/report",
        json={"success": True, "expected_owner": "W-owner"},
    )
    assert resp.status_code == 200
    assert resp.get_json()["inserted"] == 1


@needs_db
def test_report_expected_owner_mismatch_409(client, make_task):
    tid = make_task(status="running", claimed_by="W-owner")
    resp = client.post(
        f"/api/tasks/{tid}/steps/1/report",
        json={"success": True, "expected_owner": "W-stale"},
    )
    assert resp.status_code == 409
    body = resp.get_json()
    assert "error" in body
    # body 回带实际持有者，供前端提示/纠偏
    assert body["claimed_by"] == "W-owner"


@needs_db
def test_report_expected_owner_force_bypass(client, make_task):
    tid = make_task(status="running", claimed_by="W-owner")
    resp = client.post(
        f"/api/tasks/{tid}/steps/1/report",
        json={"success": True, "expected_owner": "W-stale", "force": True},
    )
    # force=true 显式逃生门：跳过持有者校验
    assert resp.status_code == 200
    assert resp.get_json()["inserted"] == 1


@needs_db
def test_report_no_owner_without_expected_owner(client, make_task):
    # 无主任务（claimed_by=NULL）不带 expected_owner：豁免 owner 校验 → 200
    tid = make_task(status="running")
    resp = client.post(f"/api/tasks/{tid}/steps/1/report", json={"success": True})
    assert resp.status_code == 200
    assert resp.get_json()["inserted"] == 1


# ---------- ⑬ T5 序号门：抢跑未来步骤 → 409 step_not_current ----------

@needs_db
def test_report_409_step_not_current(client, conn, make_task):
    # 任务 current_step 默认 1；step 2 在 steps 表存在但尚未轮到
    tid = make_task(status="running", claimed_by="W-api")
    with conn.cursor() as cur:
        cur.execute("INSERT INTO steps (task_id, step_index) VALUES (%s, 2)", (tid,))
    conn.commit()

    resp = client.post(
        f"/api/tasks/{tid}/steps/2/report",
        json={"success": True, "expected_owner": "W-api"},
    )
    assert resp.status_code == 409
    body = resp.get_json()
    assert body["error_code"] == "step_not_current"
    assert body["current_step"] == 1  # 回带实际当前步，供前端纠偏


# ---------- ⑭ T5 持有者强制校验：缺失 400 / 类型错 400 ----------

@needs_db
def test_report_400_expected_owner_required(client, make_task):
    # 有主任务缺 expected_owner → 400（不再是加法式静默放行）
    tid = make_task(status="running", claimed_by="W-api")
    resp = client.post(f"/api/tasks/{tid}/steps/1/report", json={"success": True})
    assert resp.status_code == 400
    assert resp.get_json()["error_code"] == "expected_owner_required"


@needs_db
def test_report_400_expected_owner_invalid_type(client, make_task):
    # expected_owner 非字符串 → 400（消灭静默跳过）
    tid = make_task(status="running", claimed_by="W-api")
    resp = client.post(
        f"/api/tasks/{tid}/steps/1/report",
        json={"success": True, "expected_owner": 123},
    )
    assert resp.status_code == 400
    assert resp.get_json()["error_code"] == "expected_owner_invalid"


# ---------- ⑮ T5 expected_epoch 围栏：陈旧 409 / 匹配 200 / 类型错 400 ----------

@needs_db
def test_report_409_epoch_mismatch(client, conn, make_task):
    tid = make_task(status="running", claimed_by="W-epoch")
    with conn.cursor() as cur:
        cur.execute("UPDATE tasks SET claim_epoch = 7 WHERE id = %s", (tid,))
    conn.commit()

    resp = client.post(
        f"/api/tasks/{tid}/steps/1/report",
        json={"success": True, "expected_owner": "W-epoch", "expected_epoch": 6},
    )
    assert resp.status_code == 409
    body = resp.get_json()
    assert body["error_code"] == "epoch_mismatch"
    assert body["claim_epoch"] == "7"  # 回带实际 epoch（字符串化，防 2^53 丢精度）


@needs_db
def test_report_expected_epoch_match_200(client, conn, make_task):
    tid = make_task(status="running", claimed_by="W-epoch")
    with conn.cursor() as cur:
        cur.execute("UPDATE tasks SET claim_epoch = 7 WHERE id = %s", (tid,))
    conn.commit()

    resp = client.post(
        f"/api/tasks/{tid}/steps/1/report",
        json={"success": True, "expected_owner": "W-epoch", "expected_epoch": 7},
    )
    assert resp.status_code == 200
    assert resp.get_json()["inserted"] == 1


@needs_db
def test_report_400_expected_epoch_invalid_type(client, make_task):
    # bool 不算整数；非法类型一律 400 invalid_field
    tid = make_task(status="running", claimed_by="W-api")
    resp = client.post(
        f"/api/tasks/{tid}/steps/1/report",
        json={"success": True, "expected_owner": "W-api", "expected_epoch": True},
    )
    assert resp.status_code == 400
    assert resp.get_json()["error_code"] == "invalid_field"


# ---------- ⑯b expected_epoch 字符串化兼容：纯数字字符串匹配 / 非数字 400 ----------

@needs_db
def test_report_expected_epoch_string_match_200(client, conn, make_task):
    # GET 已把 claim_epoch 字符串化，前端原样透传字符串形态的 expected_epoch：
    # 服务端归一化 int 后与库值比对，匹配则正常写入
    tid = make_task(status="running", claimed_by="W-epoch")
    with conn.cursor() as cur:
        cur.execute("UPDATE tasks SET claim_epoch = 7 WHERE id = %s", (tid,))
    conn.commit()

    resp = client.post(
        f"/api/tasks/{tid}/steps/1/report",
        json={"success": True, "expected_owner": "W-epoch", "expected_epoch": "7"},
    )
    assert resp.status_code == 200
    assert resp.get_json()["inserted"] == 1


@needs_db
def test_report_400_expected_epoch_non_digit_string(client, make_task):
    # 非纯十进制数字字符串（含负号/小数点/字母/空串）一律 400 invalid_field
    tid = make_task(status="running", claimed_by="W-api")
    resp = client.post(
        f"/api/tasks/{tid}/steps/1/report",
        json={"success": True, "expected_owner": "W-api", "expected_epoch": "7a"},
    )
    assert resp.status_code == 400
    assert resp.get_json()["error_code"] == "invalid_field"


# ---------- ⑯ T5 force 逃生门：不带 owner/epoch 字段也能强制通过 ----------

@needs_db
def test_report_force_bypass_without_owner_fields(client, make_task):
    tid = make_task(status="running", claimed_by="W-owner")
    resp = client.post(
        f"/api/tasks/{tid}/steps/1/report",
        json={"success": True, "force": True},
    )
    assert resp.status_code == 200
    assert resp.get_json()["inserted"] == 1


# ---------- ⑯b 高-1：非连续步号任务的序号门结构性锁死，force 一并豁免 ----------

@needs_db
def test_report_force_bypass_sequence_gate_for_nonconsecutive_steps(client, conn, make_task):
    """claimed 态任务 current_step 默认 1，steps 为 2/5/7 时任何 seq 都不等
    current_step：无 force 一律 409 step_not_current（结构性锁死）；
    force=true + 完整 owner/epoch 则豁免序号门成功写入 —— force 是人工
    干预逃生门，覆盖 owner/epoch/序号三重校验（状态门仍不豁免）。"""
    tid = make_task(status="claimed", claimed_by="W-gap", steps=(2, 5, 7))
    with conn.cursor() as cur:
        cur.execute("SELECT current_step, claim_epoch FROM tasks WHERE id=%s", (tid,))
        current_step, epoch = cur.fetchone()
    assert current_step == 1  # schema 默认值：与 2/5/7 全部不等 → 锁死前提

    # 无 force：上报真实存在的 step 2 也被序号门拒绝，回带 current_step
    resp = client.post(
        f"/api/tasks/{tid}/steps/2/report",
        json={"success": True, "expected_owner": "W-gap"},
    )
    assert resp.status_code == 409
    body = resp.get_json()
    assert body["error_code"] == "step_not_current"
    assert body["current_step"] == 1

    # force=true + 完整 owner/epoch：豁免序号门与持有者/围栏门 → 200 写入
    resp = client.post(
        f"/api/tasks/{tid}/steps/2/report",
        json={"success": True, "expected_owner": "W-gap",
              "expected_epoch": epoch, "force": True},
    )
    assert resp.status_code == 200
    assert resp.get_json()["inserted"] == 1

    # 状态门不豁免的反例锚定：同口径 force 对 done 任务仍 409 task_terminal
    tid_done = make_task(status="done", steps=(2,))
    resp = client.post(
        f"/api/tasks/{tid_done}/steps/2/report",
        json={"success": True, "force": True},
    )
    assert resp.status_code == 409
    assert resp.get_json()["error_code"] == "task_terminal"


# ---------- ⑰ T5 413：超大 body → payload_too_large（无需 DB） ----------

def test_report_413_payload_too_large(client):
    big = json.dumps({"success": True, "pad": "x" * (70 * 1024)})
    resp = client.post(
        "/api/tasks/1/steps/1/report", data=big,
        content_type="application/json",
    )
    assert resp.status_code == 413
    assert resp.get_json()["error_code"] == "payload_too_large"


# ---------- ⑱ T5 限流：超阈值 → 429 rate_limited ----------

def test_report_rate_limited_429(client, monkeypatch):
    # monkeypatch 阈值为 3，避免真等 60s 窗口；判定读模块常量，即时生效
    monkeypatch.setattr(api_mod, "RATE_LIMIT_MAX", 3)
    last = None
    for _ in range(5):
        last = client.post("/api/tasks/1/steps/1/report", json={"success": True})
    assert last.status_code == 429
    assert last.get_json()["error_code"] == "rate_limited"


# ---------- ⑲ T5 healthz：200 + ok 字段 + 池统计 ----------

@needs_db
def test_healthz_ok(client):
    resp = client.get("/healthz")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is True
    assert "pool" in body and "pool_size" in body["pool"]


# ---------- ⑳ T5 ETag：If-None-Match 命中 → 304 空体 ----------

@needs_db
def test_list_tasks_etag_304(client):
    first = client.get("/api/tasks")
    assert first.status_code == 200
    etag = first.headers.get("ETag")
    assert etag  # 响应必带 ETag

    second = client.get("/api/tasks", headers={"If-None-Match": etag})
    assert second.status_code == 304
    assert second.data == b""  # 空体


# ---------- ㉑ T5 读路径新字段：claim_epoch / channel；分页参数校验 ----------

@needs_db
def test_list_tasks_claim_epoch_and_channel_fields(client, conn, make_task):
    tid = make_task(status="running", claimed_by="W-fields")
    with conn.cursor() as cur:
        cur.execute("UPDATE tasks SET claim_epoch = 3 WHERE id = %s", (tid,))
        cur.execute(
            "INSERT INTO step_logs (task_id, step_index, success, worker_id, channel) "
            "VALUES (%s, 1, true, 'W-fields', 'worker')", (tid,),
        )
    conn.commit()

    resp = client.get("/api/tasks")
    assert resp.status_code == 200
    mine = [t for t in resp.get_json() if t["id"] == tid]
    assert len(mine) == 1
    # claim_epoch 字符串化输出：防 bigint 超 JS 2^53 安全整数边界
    assert mine[0]["claim_epoch"] == "3"
    assert mine[0]["steps"][0]["log"]["channel"] == "worker"


@needs_db
def test_list_tasks_pagination_and_invalid_params(client, make_task):
    make_task(status="running", claimed_by="W-page")

    # limit=1：只回一个任务（keyset 分页收敛）
    resp = client.get("/api/tasks?limit=1")
    assert resp.status_code == 200
    assert len(resp.get_json()) == 1

    # 非法 limit/after_id → 400 invalid_field
    resp = client.get("/api/tasks?limit=abc")
    assert resp.status_code == 400
    assert resp.get_json()["error_code"] == "invalid_field"
    resp = client.get("/api/tasks?after_id=-")
    assert resp.status_code == 400
    assert resp.get_json()["error_code"] == "invalid_field"


# ---------- ㉒a API_TOKEN 可选认证：未设放行 / 缺 token 401 / 正确 token 通过 ----------
# 钩子每请求读 os.environ，monkeypatch setenv 即时生效；沿用既有 client fixture，
# autouse 的限流清账 fixture 同样覆盖本组用例。选不需 DB 的参数校验路径
# （limit=abc → 400）断言“认证已通过”，避免无库环境 skip。

def test_auth_401_when_token_required_but_missing(client, monkeypatch):
    monkeypatch.setenv("API_TOKEN", "secret-token")
    resp = client.get("/api/tasks")
    assert resp.status_code == 401
    body = resp.get_json()
    assert body["error_code"] == "unauthorized"
    assert "error" in body


def test_auth_pass_with_valid_bearer_token(client, monkeypatch):
    monkeypatch.setenv("API_TOKEN", "secret-token")
    # 携带正确 token：请求通过认证门进入参数校验段（非法 limit → 400
    # invalid_field 而非 401，证明未被认证拦截）
    resp = client.get(
        "/api/tasks?limit=abc",
        headers={"Authorization": "Bearer secret-token"},
    )
    assert resp.status_code == 400
    assert resp.get_json()["error_code"] == "invalid_field"


def test_auth_passthrough_when_token_unset(client, monkeypatch):
    # API_TOKEN 未设置：行为不变，无 token 也直达参数校验段
    monkeypatch.delenv("API_TOKEN", raising=False)
    resp = client.get("/api/tasks?limit=abc")
    assert resp.status_code == 400
    assert resp.get_json()["error_code"] == "invalid_field"


# ---------- ㉒ T5 连接池：归还后隔离级别复位（池默认 reset 兜底） ----------

@needs_db
def test_pool_acquire_resets_isolation_level():
    # 第一次借出：抬隔离级别到 repeatable read，用后归还（不显式 rollback）
    with db.acquire() as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cur.execute("SHOW transaction_isolation")
            assert cur.fetchone()[0] == "repeatable read"

    # 再次借出：会话级隔离级别必须已复位回默认 read committed
    with db.acquire() as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW transaction_isolation")
            assert cur.fetchone()[0] == "read committed"
