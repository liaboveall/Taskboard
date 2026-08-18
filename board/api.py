# board/api.py —— Flask API + 看板静态页
# 启动: python -m board.api  (端口 5000)
#
# 设计要点（状态机单写者）：
#   任务状态流转（pending/claimed/running/done/failed）的唯一写者是 worker；
#   API 手动上报通道【只写 step_logs】，绝不碰 tasks.status。
#   这是修复"手动抢跑导致失败被吞"事故链的根因方案：旧版 API 会在线程池
#   演示后顺手把 running 翻 done，与 worker 的真实进度竞争、可能吞掉后续失败。
#
# 手动上报契约（POST /api/tasks/<tid>/steps/<seq>/report，T5 收紧版）：
#   URL 参数：tid（任务 id）、seq（step_index）
#   body（JSON object，必填键）：
#     success          bool，必填 —— 显式声明成败，禁止缺省脑补
#   body（可选键）：
#     expected_owner   str  —— 有主任务（claimed_by 非 NULL）必填；与实际
#                             持有者比对，不匹配 409 owner_mismatch
#     expected_epoch   int  —— 有主任务可选围栏；与 tasks.claim_epoch 比对，
#                             不匹配 409 epoch_mismatch（bool 不算整数）
#     force            bool —— 严格 True 时为人工干预逃生门：一并豁免
#                             owner/epoch 与序号门三重校验（【不】豁免状态门），
#                             必写结构化审计日志
#   检查顺序固定（fail-safe，逐级拒绝）：
#     限流(429) → body 类型(400) → 存在性(404 任务/步骤) → 状态门(409)
#     → force 解析(400) → 序号门(409，force=true 豁免) → 持有者/围栏门(400/409，
#     force=true 豁免) → 写库
#   所有 400/404/409/413/429/500 响应体携带机器可读 error_code。
#   无论成功/被拒，app.logger.info 记录一行结构化审计日志。
#
# 读路径（GET /api/tasks，T5 增强版）：
#   每任务含 claim_epoch；step log 含 channel；响应带 ETag（payload sha1），
#   If-None-Match 命中返回 304 空体；可选 keyset 分页 after_id/limit。
#   连接全部走 db.acquire()（psycopg_pool 连接池）。
#
#   GET  /healthz                               池内 SELECT 1 探活 + 池统计
#   GET  /api/tasks                             任务/步骤/日志快照（轮询）
#   POST /api/tasks/<tid>/steps/<seq>/report    幂等上报，契约如上
import hashlib
import os
import threading
import time
from pathlib import Path

from flask import Flask, jsonify, request, send_file
from werkzeug.exceptions import HTTPException

from board import db, logs, logconf

ROOT = Path(__file__).resolve().parent.parent          # taskboard/ 根目录
STATIC_DIR = ROOT / "static"

app = Flask(__name__, static_folder=str(STATIC_DIR), static_url_path="/static")
# 请求体上限：手动上报 body 是 JSON 小对象，64KB 足够；超限由 413 handler
# 以 JSON + error_code=payload_too_large 拒绝（见文件尾部 errorhandler）。
app.config["MAX_CONTENT_LENGTH"] = 64 * 1024


# ---------- report 端点限流：内存级 per-IP 滑动窗口 ----------
# 仅管 report（写端点）；读路径轮询不在此列。threading.Lock + dict 实现：
# ip -> 窗口内请求时间戳列表（monotonic 秒）。超阈值直接 429，不记账。
# 阈值常量模块级暴露：测试可 monkeypatch 成小值，避免真实等 60s。
RATE_LIMIT_MAX = 60                    # 窗口内允许的 report 请求数
# （阈值口径：前端“并发上报×5”一轮即 5 个 POST，演示需支持反复点击与
# 多任务串场，60/60s 留足节奏余量；限流回归用例 monkeypatch 小阈值，
# 不受该常量调整影响）
RATE_LIMIT_WINDOW_SECONDS = 60         # 滑动窗口长度（秒）
_RATE_SWEEP_AT = 1024                  # dict 超过该规模时顺带全量清扫过期条目
_rate_lock = threading.Lock()
_rate_hits = {}                        # ip -> [timestamp, ...]


def _rate_limit_allow(ip):
    """滑动窗口判定：窗口内请求数 < RATE_LIMIT_MAX 则记账放行，否则拒绝。
    每次调用先修剪本 IP 的过期条目；条目表过大时全量清扫，防无界增长。"""
    now = time.monotonic()
    cutoff = now - RATE_LIMIT_WINDOW_SECONDS
    with _rate_lock:
        if len(_rate_hits) > _RATE_SWEEP_AT:
            for k in [k for k, v in _rate_hits.items() if not v or v[-1] <= cutoff]:
                del _rate_hits[k]
        hits = _rate_hits.setdefault(ip, [])
        while hits and hits[0] <= cutoff:
            hits.pop(0)
        if len(hits) >= RATE_LIMIT_MAX:
            return False
        hits.append(now)
        return True


def _reset_rate_limit():
    """测试钩子：清空限流记账，保证用例间互不干扰。"""
    with _rate_lock:
        _rate_hits.clear()


def _iso(ts):
    """timestamptz -> ISO 字符串（None 原样返回），方便前端展示。"""
    return ts.isoformat() if ts is not None else None


# ---------- GET / ：托管单文件看板 ----------
@app.get("/")
def index():
    return send_file(STATIC_DIR / "index.html")


# ---------- GET /api/tasks ：快照读，供前端轮询（支持 ETag/分页） ----------
@app.get("/api/tasks")
def list_tasks():
    # 可选 keyset 分页：after_id（int）与 limit（int，默认不限）。
    # 不带参数时行为与现状完全一致；非法值一律 400 invalid_field（不静默）。
    after_id = 0
    limit = None
    after_id_raw = request.args.get("after_id")
    if after_id_raw is not None:
        try:
            after_id = int(after_id_raw)
        except (TypeError, ValueError):
            return jsonify({"error": "after_id must be an integer",
                            "error_code": "invalid_field"}), 400
    limit_raw = request.args.get("limit")
    if limit_raw is not None:
        try:
            limit = int(limit_raw)
        except (TypeError, ValueError):
            limit = None  # 统一由下方 <1 分支报 invalid_field
        if limit is None or limit < 1:
            return jsonify({"error": "limit must be a positive integer",
                            "error_code": "invalid_field"}), 400
    paginated = after_id_raw is not None or limit_raw is not None

    with db.acquire() as conn:
        # 单快照一致读：三条 SELECT 包进同一 REPEATABLE READ 事务，
        # worker 并发写入不会让 tasks/steps/step_logs 三表口径错位。
        # psycopg3：SET TRANSACTION 必须是事务内第一条语句，故紧跟 BEGIN 之后。
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                task_sql = ("SELECT id, status, claimed_by, current_step, "
                            "claim_epoch, base_params FROM tasks")
                task_args = []
                if paginated:
                    # keyset 分页：WHERE id > after_id ORDER BY id LIMIT limit
                    task_sql += " WHERE id > %s"
                    task_args.append(after_id)
                task_sql += " ORDER BY id"
                if limit is not None:
                    task_sql += " LIMIT %s"
                    task_args.append(limit)
                cur.execute(task_sql, task_args)
                task_rows = cur.fetchall()
                if paginated:
                    # 分页时 steps/step_logs 收敛到分页任务集，不拉全表
                    task_ids = [r[0] for r in task_rows]
                    cur.execute(
                        "SELECT task_id, step_index, override_params "
                        "FROM steps WHERE task_id = ANY(%s) "
                        "ORDER BY task_id, step_index",
                        (task_ids,),
                    )
                    step_rows = cur.fetchall()
                    cur.execute(
                        "SELECT task_id, step_index, success, reported_at, "
                        "worker_id, error_message, channel "
                        "FROM step_logs WHERE task_id = ANY(%s)",
                        (task_ids,),
                    )
                    log_rows = cur.fetchall()
                else:
                    cur.execute(
                        "SELECT task_id, step_index, override_params "
                        "FROM steps ORDER BY task_id, step_index"
                    )
                    step_rows = cur.fetchall()
                    cur.execute(
                        "SELECT task_id, step_index, success, reported_at, "
                        "worker_id, error_message, channel "
                        "FROM step_logs"
                    )
                    log_rows = cur.fetchall()

    # 日志按 (task_id, step_index) 索引；每个 step 至多一行（主键保证）
    log_map = {}
    for tid, sidx, success, reported_at, worker_id, error_message, channel in log_rows:
        log_map[(tid, sidx)] = {
            "success": success,
            "reported_at": _iso(reported_at),
            "worker_id": worker_id,
            # 失败上报携带的异常摘要；None 亦原样返回，前端口径稳定
            "error_message": error_message,
            # 上报通道审计列（'worker'/'manual'，schema v2）
            "channel": channel,
        }

    steps_by_task = {}
    for tid, sidx, override_params in step_rows:
        steps_by_task.setdefault(tid, []).append({
            "step_index": sidx,
            "override_params": override_params or {},
            "log": log_map.get((tid, sidx)),          # 无日志则为 None
        })

    tasks = []
    for tid, status, claimed_by, current_step, claim_epoch, base_params in task_rows:
        steps = steps_by_task.get(tid, [])
        tasks.append({
            "id": tid,
            "status": status,
            "claimed_by": claimed_by,
            "current_step": current_step,
            # 围栏 token 审计列：前端可据此构造 expected_epoch 防陈旧页误报
            "claim_epoch": claim_epoch,
            "base_params": base_params or {},
            "steps": steps,
            # 该任务已有日志的 step 数
            "log_count": sum(1 for s in steps if s["log"] is not None),
        })

    resp = jsonify(tasks)
    # ETag：对最终 JSON payload 计 sha1 摘要。前端轮询携带 If-None-Match，
    # 命中即 304 空体，省带宽与前端重渲染（前端手动携带，不依赖浏览器缓存语义）。
    payload = resp.get_data()
    etag = '"' + hashlib.sha1(payload).hexdigest() + '"'
    if_none_match = (request.headers.get("If-None-Match") or "").strip()
    if if_none_match and if_none_match in (etag, etag.strip('"')):
        not_modified = app.response_class(status=304)
        not_modified.headers["ETag"] = etag
        return not_modified
    resp.headers["ETag"] = etag
    return resp


# ---------- 手动上报结构化审计日志 ----------
def _audit_manual(result, error_code=None, success=None, expected_owner=None,
                  force=None):
    """无论成功/被拒均记一行：remote_addr/tid/seq/success/expected_owner/
    force/result/error_code。排查"谁在什么时候以什么口径上报/被拒"的唯一事实源。
    tid/seq 从路由参数取（拒绝路径发生在 body 校验之前也可完整记录）。"""
    view_args = request.view_args or {}
    app.logger.info(
        "manual_report remote_addr=%s tid=%s seq=%s success=%s "
        "expected_owner=%r force=%s result=%s error_code=%s",
        request.remote_addr, view_args.get("tid"), view_args.get("seq"),
        success, expected_owner, force, result, error_code,
    )


# ---------- POST /api/tasks/<tid>/steps/<seq>/report ----------
# 状态机单写者：本端点只写 step_logs（幂等 first-report-wins），
# 绝不流转 tasks.status —— 那是 worker 的专属职责。
@app.post("/api/tasks/<int:tid>/steps/<int:seq>/report")
def report(tid, seq):
    # 0. 限流门：窗口内超阈值直接 429，不记账不读库。
    if not _rate_limit_allow(request.remote_addr or "unknown"):
        _audit_manual("rejected", "rate_limited")
        return jsonify({"error": "too many report requests from this client, "
                                 "please slow down",
                        "error_code": "rate_limited"}), 429

    # 0b. 体积门显式前置：get_json(silent=True) 会吞掉 413 返回 None，
    # 不前置检查会把超大 body 误判成 bad_body 400。
    if request.content_length and request.content_length > app.config["MAX_CONTENT_LENGTH"]:
        _audit_manual("rejected", "payload_too_large")
        return jsonify({"error": "request body exceeds 64KB limit",
                        "error_code": "payload_too_large"}), 413

    # body 契约：畸形/空 body（get_json(silent=True) 解析失败返回 None）
    # 与非 dict 一律 400 拒绝。失败安全的默认必须是拒绝，而不是替调用方
    # 脑补一个方向正确的语义。
    body = request.get_json(silent=True)
    if body is None or not isinstance(body, dict):
        _audit_manual("rejected", "bad_body")
        return jsonify({"error": "request body must be a JSON object",
                        "error_code": "bad_body"}), 400
    # success 键缺失或非布尔 → 400：上报成功与否必须由调用方显式声明。
    success = body.get("success")
    if not isinstance(success, bool):
        _audit_manual("rejected", "invalid_field")
        return jsonify({"error": "success is required and must be boolean",
                        "error_code": "invalid_field"}), 400

    def reject(payload, status, code):
        """拒绝路径统一出口：先记审计日志再返回，保证"被拒也有痕"。"""
        _audit_manual("rejected", code,
                      success=body.get("success"),
                      expected_owner=body.get("expected_owner"),
                      force=body.get("force"))
        return jsonify(payload), status

    try:
        with db.acquire() as conn:
            # 1. 校验任务与步骤存在（steps 表是唯一事实源）。
            # 状态读用 FOR UPDATE：行锁持有到 report_step 内部 commit 才释放，
            # 与 reaper 回收互斥 —— 已用行锁关闭"读状态时合法、写入前被夺权"的窗口。
            # 顺带读 claimed_by/current_step/claim_epoch：供下方序号门与持有者
            # 围栏门使用（全部在同一次加锁读内取值，口径自洽）。
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT status, claimed_by, current_step, claim_epoch "
                    "FROM tasks WHERE id=%s FOR UPDATE", (tid,)
                )
                task_row = cur.fetchone()
                if task_row is None:
                    return reject({"error": f"task {tid} not found",
                                   "error_code": "task_not_found"},
                                  404, "task_not_found")
                task_status, claimed_by, current_step, claim_epoch = task_row
                cur.execute(
                    "SELECT 1 FROM steps WHERE task_id=%s AND step_index=%s", (tid, seq)
                )
                if cur.fetchone() is None:
                    return reject({"error": f"step {seq} of task {tid} not found",
                                   "error_code": "step_not_found"},
                                  404, "step_not_found")

            # 2. 状态门：只接受执行中（claimed/running）的任务。终态任务的迟到
            # 上报直接 409 —— 不再默默写进历史，避免误导看板与后续排查。
            # 状态门不受 force 豁免（force 仅豁免序号门与 owner/epoch，见步骤 3）。
            if task_status not in ("claimed", "running"):
                return reject({"error": f"task {tid} is {task_status}, manual report "
                                        "only allowed for claimed/running",
                               "error_code": "task_terminal"},
                              409, "task_terminal")

            # 3. force 解析前置（严格布尔，类型错 400）：force 是人工干预逃生门，
            # 口径留档——同时豁免下方序号门与持有者/围栏门两重校验（覆盖
            # owner/epoch/序号三重校验中的后两重），但【不豁免】上方状态门：
            # 终态任务的迟报无论是否 force 都拒绝；任何 force 路径必写审计日志。
            # 为什么前置于序号门：claimed 态任务 current_step 默认 1，steps 为
            # 2/5/7 这类非连续步号的任务任何 seq 都不等 current_step，会被序号门
            # 结构性锁死（任何上报一律 409），force 必须能豁免序号门才有逃生意义。
            force = body.get("force")
            if force is not None and not isinstance(force, bool):
                return reject({"error": "force must be a boolean",
                               "error_code": "invalid_field"},
                              400, "invalid_field")

            # 3b. 序号门（fail-safe 拒绝）：只接受 seq == current_step 的上报；
            # force=true 豁免（逃生门口径见上方注释，不豁免状态门）。
            # 为什么：手动通道抢跑未来步骤会把"尚未执行"的步提前写进日志，
            # 污染看板与 first-report-wins 语义；迟到旧步骤同理拒绝。
            # 口径说明：claimed 态任务的 current_step 默认 1（schema 默认值）；
            # 非连续 step_index 任务（如 steps=2/5/7）在无 force 时结构性被拒，
            # 属 fail-safe 预期，人工干预走 force 逃生门。
            if force is not True and seq != current_step:
                return reject({"error": f"step {seq} is not the current step of task "
                                        f"{tid} (current_step={current_step})",
                               "error_code": "step_not_current",
                               "current_step": current_step},
                              409, "step_not_current")

            # 4. 持有者/围栏门（强制校验，force=true 为显式逃生门）：
            #    有主任务（claimed_by 非 NULL）必须携带 expected_owner 字符串并
            #    与 FOR UPDATE 读到的实际值比对；可选 expected_epoch 与
            #    claim_epoch 比对 —— 防御陈旧页面对已换主/已重认领任务的误报。
            #    类型错误一律 400（消灭静默跳过）：expected_owner 必须字符串，
            #    expected_epoch 必须整数（bool 不算）；force 的类型校验已前置到
            #    序号门之前（见上方注释）。
            expected_owner = body.get("expected_owner")
            if expected_owner is not None and not isinstance(expected_owner, str):
                return reject({"error": "expected_owner must be a string",
                               "error_code": "expected_owner_invalid"},
                              400, "expected_owner_invalid")
            expected_epoch = body.get("expected_epoch")
            if expected_epoch is not None and not (
                isinstance(expected_epoch, int) and not isinstance(expected_epoch, bool)
            ):
                return reject({"error": "expected_epoch must be an integer "
                                        "(boolean is not accepted)",
                               "error_code": "invalid_field"},
                              400, "invalid_field")

            if force is True:
                # 逃生门：豁免序号门（上方）与 owner/epoch 校验（本门）。允许
                # 人工强制干预，但必须留痕 —— 下方成功/被拒均写结构化审计日志，
                # force 字段一并记录。
                pass
            elif claimed_by is not None:
                # 有主任务：expected_owner 必填
                if expected_owner is None:
                    return reject({"error": f"task {tid} is claimed by {claimed_by!r}; "
                                            "expected_owner is required",
                                   "error_code": "expected_owner_required",
                                   "claimed_by": claimed_by},
                                  400, "expected_owner_required")
                if expected_owner != claimed_by:
                    return reject({"error": f"owner mismatch: expected {expected_owner!r} "
                                            f"but task {tid} is claimed by {claimed_by!r}",
                                   "error_code": "owner_mismatch",
                                   "claimed_by": claimed_by},
                                  409, "owner_mismatch")
                if expected_epoch is not None and expected_epoch != claim_epoch:
                    return reject({"error": f"epoch mismatch: expected {expected_epoch} "
                                            f"but task {tid} claim_epoch is {claim_epoch}",
                                   "error_code": "epoch_mismatch",
                                   "claim_epoch": claim_epoch},
                                  409, "epoch_mismatch")
            # 无主任务（claimed_by IS NULL）豁免 owner/epoch 校验。
            # 注：此处【不】提前 commit —— 状态门的行锁必须保持到
            # report_step 内部 commit 释放，否则窗口重新敞开。

            # 5. 单连接一次真实上报（owner=None：API 手动通道不带持有围栏；
            # channel='manual' 写审计列，不带 epoch/duration——手动通道无持有关系与步耗时）
            inserted = logs.report_step(conn, tid, seq, success, "api-manual", channel='manual')

            # 6. 回读现存日志行（无论首次还是重复上报，都返回库里现存的那一行）
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT success, reported_at, worker_id, error_message FROM step_logs "
                    "WHERE task_id=%s AND step_index=%s",
                    (tid, seq),
                )
                row = cur.fetchone()
            log_row = (
                {"task_id": tid, "step_index": seq, "success": row[0],
                 "reported_at": _iso(row[1]), "worker_id": row[2],
                 "error_message": row[3]}
                if row else None
            )
    except Exception:
        # DB 段未预期异常（连接故障/语句错误等）的审计补录：先记一行
        # rejected/internal_error，再 re-raise 交全局 errorhandler 兜底成
        # JSON 500 —— 500 路径不再无痕（排查“谁的上报炸了”有据可查）。
        _audit_manual("rejected", "internal_error",
                      success=success, expected_owner=body.get("expected_owner"),
                      force=body.get("force"))
        raise

    resp = {
        "received": 1,
        "inserted": 1 if inserted else 0,
        "duplicates_ignored": 0 if inserted else 1,
        "log_row": log_row,
    }
    # 重复上报且现存行与本次请求口径不同：显式提示 first-report-wins，
    # 调用方无需猜测为什么自己的上报"没生效"
    if not inserted and log_row is not None and log_row["success"] != success:
        resp["conflict_note"] = "first-report-wins: existing log kept"
    _audit_manual("inserted" if inserted else "duplicate",
                  success=success, expected_owner=body.get("expected_owner"),
                  force=body.get("force"))
    return jsonify(resp)


# ---------- GET /healthz ：池内探活 ----------
@app.get("/healthz")
def healthz():
    """走连接池 SELECT 1：既探 DB 活性，也验证池本身可用。
    成功 200（附池统计 get_stats()）；任何异常 503。"""
    try:
        with db.acquire() as conn:
            # 包进显式事务块：退出时自动 commit，归还时连接处于 idle，
            # 避免池对 INTRANS 归还打 "rolling back returned connection" 告警
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
    except Exception as exc:
        app.logger.warning("healthz probe failed: %s", exc)
        return jsonify({"ok": False, "error": "database unreachable",
                        "error_code": "internal_error"}), 503
    # get_stats() 可用字段（psycopg_pool 3.3）：pool_min/pool_max/pool_size/
    # pool_available/requests_num/requests_waiting 等
    stats = db.get_pool().get_stats()
    return jsonify({
        "ok": True,
        "pool": {
            # getconn 视角的池容量与当前空闲连接数
            "pool_size": stats.get("pool_size"),
            "pool_available": stats.get("pool_available"),
            "pool_min": stats.get("pool_min"),
            "pool_max": stats.get("pool_max"),
            "requests_num": stats.get("requests_num"),
            "requests_waiting": stats.get("requests_waiting"),
        },
    })


# ---------- 413：超大 body 统一 JSON 化 ----------
@app.errorhandler(413)
def handle_payload_too_large(e):
    return jsonify({"error": "request body exceeds 64KB limit",
                    "error_code": "payload_too_large"}), 413


# ---------- 全局错误兜底：未捕获异常返回 JSON 而不是 HTML 500 ----------
@app.errorhandler(Exception)
def handle_unexpected(e):
    # HTTPException（Flask/werkzeug 的 404/409 等）转交给下方专用 handler，
    # 只接管真正未预期的异常 —— 避免吃掉框架自带的状态码语义。
    if isinstance(e, HTTPException):
        raise e
    # 信息收敛：异常详情只进服务端日志（含 traceback），
    # 客户端只拿固定文案，不泄露内部实现细节。
    app.logger.exception("unhandled exception")
    return jsonify({"error": "internal server error",
                    "error_code": "internal_error"}), 500


# ---------- HTTPException 统一 JSON 化 ----------
# 404/405/未匹配路由等 werkzeug 默认返回 HTML，前端轮询与 API 调用方
# 拿到 HTML 会解析失败；这里按最具体类型匹配接管，状态码语义保持不动。
# error_code 补充三个通用码（not_found/method_not_allowed/http_error），
# 覆盖未匹配路由等框架级异常 —— 业务错误码仍严格使用契约枚举。
_HTTP_ERROR_CODES = {404: "not_found", 405: "method_not_allowed"}


@app.errorhandler(HTTPException)
def handle_http_exception(e):
    return jsonify({"error": e.description,
                    "error_code": _HTTP_ERROR_CODES.get(e.code, "http_error")}), e.code


if __name__ == "__main__":
    # 统一 logging 配置（本入口无自有 print；Flask/werkzeug 请求日志随之走统一格式）
    logconf.setup("board.api")
    # threaded=True：支持前端轮询与并发 POST 同时进行；
    # 端口支持环境变量 PORT（默认 5000）。
    app.run(host="127.0.0.1", port=int(os.environ.get("PORT", "5000")), threaded=True)
