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
#     expected_epoch   int|str —— 有主任务可选围栏；接受 int（bool 拒收）或
#                             纯十进制数字字符串（GET 已把 claim_epoch
#                             字符串化，前端原样透传），归一化 int 后与
#                             tasks.claim_epoch 比对，不匹配 409 epoch_mismatch
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
#   每任务含 claim_epoch（输出时字符串化：bigint 可能超过 JS Number 的
#   2^53 安全整数边界，字符串透传才能无损往返）；step log 含 channel；
#   响应带 ETag（payload sha256），If-None-Match 命中返回 304 空体；
#   可选 keyset 分页 after_id/limit。
#   连接全部走 db.acquire()（psycopg_pool 连接池）。
#
#   GET  /healthz                               池内 SELECT 1 探活 + 池统计
#   GET  /api/tasks                             任务/步骤/日志快照（轮询）
#   POST /api/tasks/<tid>/steps/<seq>/report    幂等上报，契约如上
import collections
import hashlib
import hmac
import logging
import os
import re
import threading
import time
import uuid
from pathlib import Path

from flask import Flask, g, jsonify, request, send_file
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
# ip -> 窗口内请求时间戳 deque（monotonic 秒，左旧右新）。超阈值直接 429，不记账。
# deque 而非 list：过期条目从头部弹出是 O(1)（list.pop(0) 是 O(n)），
# 高并发限流判定不再随窗口内条目数线性劣化。
# 阈值常量模块级暴露：测试可 monkeypatch 成小值，避免真实等 60s。
RATE_LIMIT_MAX = 60                    # 窗口内允许的 report 请求数
# （阈值口径：前端“并发上报×5”一轮即 5 个 POST，演示需支持反复点击与
# 多任务串场，60/60s 留足节奏余量；限流回归用例 monkeypatch 小阈值，
# 不受该常量调整影响）
RATE_LIMIT_WINDOW_SECONDS = 60         # 滑动窗口长度（秒）
_RATE_SWEEP_AT = 1024                  # dict 超过该规模时顺带全量清扫过期条目
_rate_lock = threading.Lock()
_rate_hits = {}                        # ip -> deque([timestamp, ...])

# ---------- 读路径分页上限：防大表全量快照拖垮看板与 DB ----------
# 阈值口径（与 RATE_LIMIT_MAX 同为模块级常量，测试可 monkeypatch）：
# 演示/单机部署任务量级千以内，DEFAULT_LIMIT=1000 留足余量；
# MAX_LIMIT=2000 封顶显式请求，防客户端传大 limit 把单次快照打爆。
# 缺省不传 limit 即取 DEFAULT_LIMIT；显式传入超 MAX_LIMIT 夹紧不报错
# （宽容口径：分页是优化参数，越界不构成语义错误）。超页与否由
# 多取 1 行判定，经 X-Has-More 响应头告知调用方。
DEFAULT_LIMIT = 1000
MAX_LIMIT = 2000


# ---------- 可选 token 认证：API_TOKEN 非空时拦截全部 /api 请求 ----------
# opt-in 口径：未设置 API_TOKEN 时全放行（可信本机环境），只打一次性
# WARNING（模块级标志防轮询刷屏）；设置后 Bearer token 不匹配一律 401。
# 钩子位置口径：before_request 先于视图函数体执行 → 认证门天然位于
# report 限流判定之前：非法请求在认证层即被拒，不消耗限流窗口额度，
# 防无效 token 洪水耗尽合法客户端配额。比对用 hmac.compare_digest
# 常量时间比较，防时序侧信道逐字符猜 token。
_TOKEN_WARNED = False


@app.before_request
def _check_api_token():
    global _TOKEN_WARNED
    # 首行路径门：只管 /api；看板静态页与 /healthz 探活不拦
    if not request.path.startswith("/api"):
        return None
    expected = os.environ.get("API_TOKEN") or ""
    if not expected:
        # 未设置：放行 + 一次性 WARNING（默认仅适用于可信本机）
        if not _TOKEN_WARNED:
            app.logger.warning(
                "API_TOKEN 未设置：/api 全放行，仅限可信本机环境；"
                "对外部署必须设置 API_TOKEN 并配合反向代理")
            _TOKEN_WARNED = True
        return None
    auth = request.headers.get("Authorization") or ""
    scheme, _, token = auth.partition(" ")
    token = token.strip()
    # encode 后比较：compare_digest 对 str 要求 ASCII，encode 口径更稳
    if scheme != "Bearer" or not token or not hmac.compare_digest(
            token.encode("utf-8"), expected.encode("utf-8")):
        app.logger.info("auth rejected: remote_addr=%s path=%s",
                        request.remote_addr, request.path)
        return jsonify({"error": "authentication required: "
                                 "provide Authorization: Bearer <token>",
                        "error_code": "unauthorized"}), 401
    return None


# ---------- 请求级跟踪 id：日志与响应头的关联纽带 ----------
@app.before_request
def _assign_request_id():
    """每个请求生成唯一跟踪 id（uuid4 hex）：
    logconf.RequestIdFilter 从 flask.g 取它注入每条日志，
    after_request 再经 X-Request-Id 响应头回给调用方——
    前端报错时可拿该 id 直接定位服务端对应日志行。"""
    g.request_id = uuid.uuid4().hex


@app.after_request
def _stamp_request_id(resp):
    # 无论成败/304/401 都回写；before_request 未走到（如更早阶段
    # 被拦）时 g 上无值，回退 "-" 保证头恒存在
    resp.headers["X-Request-Id"] = getattr(g, "request_id", "-")
    return resp


def _rate_limit_allow(ip):
    """滑动窗口判定：窗口内请求数 < RATE_LIMIT_MAX 则记账放行，否则拒绝。
    每次调用先修剪本 IP 的过期条目（deque 头部 popleft，O(1)）；
    条目表过大时全量清扫，防无界增长。"""
    now = time.monotonic()
    cutoff = now - RATE_LIMIT_WINDOW_SECONDS
    with _rate_lock:
        if len(_rate_hits) > _RATE_SWEEP_AT:
            # 全量清扫：空窗或最后一条时间戳已过期即整体作废。
            # deque 支持 v[-1] 下标访问（O(n)，仅清扫路径使用，不影响热路径）。
            for k in [k for k, v in _rate_hits.items() if not v or v[-1] <= cutoff]:
                del _rate_hits[k]
        hits = _rate_hits.setdefault(ip, collections.deque())
        while hits and hits[0] <= cutoff:
            hits.popleft()
        if len(hits) >= RATE_LIMIT_MAX:
            return False
        hits.append(now)
        return True


def _reset_rate_limit():
    """测试钩子：清空限流记账，保证用例间互不干扰。"""
    with _rate_lock:
        _rate_hits.clear()


def _etag_match(if_none_match, etag):
    """RFC 7232 口径的 If-None-Match 匹配（命中即 304）：
      - `*` 表示匹配任意当前表示 → 直接命中；
      - 多个候选逗号分隔，任一命中即命中；
      - 忽略弱校验前缀 W/ 后参与比对（304 语义是“无需重传”，
        强弱之分不影响省带宽目标；前缀与候选间空白一并容忍）；
      - 与带引号形态及 strip('"') 裸摘要形态做强比对
        （兼容不发引号、裸贴摘要的客户端）。"""
    if not if_none_match:
        return False
    bare = etag.strip('"')
    for cand in if_none_match.split(","):
        cand = cand.strip()
        if cand == "*":
            return True
        if cand.startswith("W/"):
            cand = cand[2:].strip()
        if cand in (etag, bare):
            return True
    return False


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
    # keyset 分页：after_id（int）可选；limit（int）缺省 DEFAULT_LIMIT、
    # 显式超 MAX_LIMIT 夹紧不报错；非法值一律 400 invalid_field（不静默）。
    # limit 恒有值 → 读路径统一走分页分支：steps/step_logs 收敛到本页
    # 任务集，输出 JSON 结构不变。
    after_id = 0
    limit = DEFAULT_LIMIT
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
        if limit > MAX_LIMIT:
            limit = MAX_LIMIT  # 显式超限：夹紧到封顶值，不报错

    with db.acquire() as conn:
        # 单快照一致读：三条 SELECT 包进同一 REPEATABLE READ 事务，
        # worker 并发写入不会让 tasks/steps/step_logs 三表口径错位。
        # psycopg3：SET TRANSACTION 必须是事务内第一条语句，故紧跟 BEGIN 之后。
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                task_sql = ("SELECT id, status, claimed_by, current_step, "
                            "claim_epoch, base_params, retry_count, max_retries "
                            "FROM tasks")
                task_args = []
                if after_id_raw is not None:
                    # keyset 分页：WHERE id > after_id ORDER BY id LIMIT limit
                    task_sql += " WHERE id > %s"
                    task_args.append(after_id)
                task_sql += " ORDER BY id"
                # 多取 1 行判定 has_more：返回行数 > limit 即还有下一页
                task_sql += " LIMIT %s"
                task_args.append(limit + 1)
                cur.execute(task_sql, task_args)
                task_rows = cur.fetchall()
                has_more = len(task_rows) > limit
                if has_more:
                    task_rows = task_rows[:limit]
                # steps/step_logs 收敛到分页任务集，不拉全表
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
    for (tid, status, claimed_by, current_step, claim_epoch, base_params,
         retry_count, max_retries) in task_rows:
        steps = steps_by_task.get(tid, [])
        tasks.append({
            "id": tid,
            "status": status,
            "claimed_by": claimed_by,
            "current_step": current_step,
            # 围栏 token 审计列：前端可据此构造 expected_epoch 防陈旧页误报。
            # 字符串化输出：claim_epoch 是 PG bigint，单调递增无上限，可能超过
            # JS Number 的 2^53 安全整数边界（JSON 数字往返丢精度）；
            # 字符串透传无损，前端原样回传、服务端归一化 int 后比对。
            "claim_epoch": str(claim_epoch),
            "base_params": base_params or {},
            # 死信机制审计列（schema v3）：重试计数与上限；前端可暂不展示，
            # int 值域无 2^53 精度风险，无需字符串化。
            "retry_count": retry_count,
            "max_retries": max_retries,
            "steps": steps,
            # 该任务已有日志的 step 数
            "log_count": sum(1 for s in steps if s["log"] is not None),
        })

    resp = jsonify(tasks)
    # 分页越页提示：总任务数超出一页时为 "true"，前端据此显示醒目横幅
    resp.headers["X-Has-More"] = "true" if has_more else "false"
    # ETag：对最终 JSON payload 计 sha256 摘要（sha1 已不再视为抗碰撞安全，
    # 摘要长度对省带宽目标无实质影响，直接用更稳的 sha256）。
    # 前端轮询携带 If-None-Match，命中即 304 空体，省带宽与前端重渲染
    # （前端手动携带，不依赖浏览器缓存语义；匹配口径见 _etag_match）。
    payload = resp.get_data()
    etag = '"' + hashlib.sha256(payload).hexdigest() + '"'
    if _etag_match(request.headers.get("If-None-Match"), etag):
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
                    # 信息收敛：不回显具体 tid 的存在性（探测枚举防护），
                    # 调用方靠机器可读的 error_code 区分语义
                    return reject({"error": "requested task or step does not exist",
                                   "error_code": "task_not_found"},
                                  404, "task_not_found")
                task_status, claimed_by, current_step, claim_epoch = task_row
                cur.execute(
                    "SELECT 1 FROM steps WHERE task_id=%s AND step_index=%s", (tid, seq)
                )
                if cur.fetchone() is None:
                    # 同 task_not_found 口径：统一模糊文案，不回显 tid/seq 存在性
                    return reject({"error": "requested task or step does not exist",
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
            #    expected_epoch 必须整数或纯数字字符串（bool 不算，字符串归一化
            #    int 后比对）；force 的类型校验已前置到序号门之前（见上方注释）。
            expected_owner = body.get("expected_owner")
            if expected_owner is not None and not isinstance(expected_owner, str):
                return reject({"error": "expected_owner must be a string",
                               "error_code": "expected_owner_invalid"},
                              400, "expected_owner_invalid")
            expected_epoch = body.get("expected_epoch")
            # 双入参兼容（claim_epoch 字符串化的配套放宽）：接受 int（bool 拒收）
            # 或纯十进制数字字符串（^\d+$，GET 字符串化后前端原样透传的形态）；
            # 其余类型一律 400，消灭静默跳过。
            if expected_epoch is not None and not (
                (isinstance(expected_epoch, int)
                 and not isinstance(expected_epoch, bool))
                or (isinstance(expected_epoch, str)
                    and re.fullmatch(r"\d+", expected_epoch))
            ):
                return reject({"error": "expected_epoch must be an integer or a "
                                        "decimal-digit string "
                                        "(boolean is not accepted)",
                               "error_code": "invalid_field"},
                              400, "invalid_field")
            # 归一化：字符串形态转 int，下方与库内 bigint 直接比对
            if isinstance(expected_epoch, str):
                expected_epoch = int(expected_epoch)

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
                    # 回带库内 epoch 同样字符串化：与 GET 输出口径一致，
                    # 前端拿到即可直接比对/展示，无 2^53 精度风险
                    return reject({"error": f"epoch mismatch: expected {expected_epoch} "
                                            f"but task {tid} claim_epoch is {claim_epoch}",
                                   "error_code": "epoch_mismatch",
                                   "claim_epoch": str(claim_epoch)},
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
    logger = logging.getLogger("board.api")
    # 服务器选型口径：gunicorn 不支持 Windows，本项目演示/部署环境以 Windows 为主；
    # waitress 是纯 Python 单进程多线程 WSGI 服务器，并发模型与项目内既有的
    # threading.Lock 限流、模块级状态及 psycopg_pool 连接池（进程内线程安全
    # 共享）语义一致，无需为多进程部署重审状态边界。threads 可经
    # TB_WSGI_THREADS 覆盖（默认 8）。
    # waitress 缺失（最小环境未装）时回退 werkzeug 自带 app.run（debug 关闭）：
    # 明确 WARNING 提示非生产级，保演示链路不断。
    host = "127.0.0.1"
    port = db.env_int("PORT", 5000)
    try:
        import waitress
    except ImportError:
        logger.warning("waitress 未安装：回退 werkzeug 开发服务器"
                       "（单线程语义弱化、非生产级，建议 pip install waitress）")
        app.run(host=host, port=port, debug=False)
    else:
        waitress.serve(app, host=host, port=port,
                       threads=db.env_int("TB_WSGI_THREADS", 8))
