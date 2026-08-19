# scripts/attack_claim.py —— 真多进程认领攻击脚本（独立可执行，不依赖 pytest）
#
# 设计要点：
#   - spawn 上下文启动 10 个 worker 进程；连接在【子进程内部】新建，
#     绝不从父进程传递（数据库连接不能跨进程共享）。
#   - worker_main 定义在模块顶层（可 pickle，spawn 可序列化）。
#   - Barrier(10) 让 10 个 worker 在每轮同一时刻起跑（带 timeout）；
#     barrier 死亡（超时/损坏）不再静默少人——worker 经 out_q 上报
#     ("BARRIER_DEAD", worker_id)，父进程收集到任何一个即整轮判 FAIL。
#   - 进程只创建一次、跨轮复用；每轮通过控制队列 start_q 发放 ("GO", mode)。
#   - 每轮结束的重复认领判定 = ① queue 回传的 id 无重复（dup_queue）
#     ② DB 侧计数核对：claimed 行数 == 投放数 且 pending == 0。
#     （tasks.id 是主键，数据库里不可能出现重复 id 行，所以不查
#     GROUP BY id HAVING count(*)>1——那是恒为零行的死检查；
#     真正的"同一任务被两个 worker 认领"只可能表现为队列回传重复 id。）
#     ③ 参与度核对：distinct claimed_by >= 2，防"单进程抢先锁完全部"
#     的退化假阳性（多 worker 名存实亡）。
#   - claim×reaper 组合攻击轮（--no-reaper-round 可关）：全员认领耗尽后，
#     父进程用 SQL 把 claimed_at 回填到过去制造租约过期（不真实等 60s），
#     再并发 reclaim_expired + 再认领，断言：任务不被重复持有（队列与 DB
#     双向核对）、回收后重认领 epoch 严格递增、旧 epoch 的 transition
#     写入全部 rowcount=0（代际围栏对"回收后重认领"仍然有效）。
#   - report_step 洪泛攻击轮（--no-report-round 可关）：认领耗尽后 SQL 回填
#     claimed_at 制造过期 → 并发 reclaim + 再认领制造新旧两代持有关系，
#     随后 10 个进程对同一批 (task, step) 并发洪泛上报，混合三种口径：
#     旧代围栏 (owner_a, epoch_a)、当代围栏 (owner_b, epoch_b，仅当代持有者
#     能通过)、无围栏直插（owner=None）。断言：每 (task_id, step_index)
#     恰一行、首报归属正确（DB worker_id == 唯一插入成功者）、旧代围栏
#     上报 0 写入。
#   - TRUNCATE 护栏：本脚本会 TRUNCATE 四表。启动最早期（任何写操作前）
#     解析 db.database_url() 库名，不含 "test" 且未显式传 --truncate-ok
#     时打印警告并 exit 2，防止误清生产/演示库。
#   - 全部输出为 ASCII，并同步写入日志文件（默认 evidence\claim_attack_run.log，
#     可用 --out 参数改路径）。
import argparse
import multiprocessing as mp
import queue
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit

import psycopg

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from board import claim, db, logs  # noqa: E402

ROUNDS = 10
WORKERS = 10
TASKS_PER_ROUND = 100
LEASE_SECONDS = 60          # reclaim_expired 的租约口径（与 worker 一致）
BACKDATE_SECONDS = 3600     # 父进程回填 claimed_at 的过期深度（远大于租约）

# —— report_step 洪泛攻击轮参数 ——
REPORT_ROUNDS = 3           # 洪泛轮数
REPORT_TASKS = 8            # 每轮投放任务数
REPORT_STEP_INDICES = (1, 2)  # 每任务的 step 编号（worker 侧按常量周知）
REPORT_FLOOD_MULT = 2       # 每 worker 对每 (task, step) 的无围栏洪泛倍数


def worker_main(worker_id, start_q, out_q, barrier):
    """worker 进程入口（模块顶层函数，可被 spawn pickle）。"""
    # 连接在子进程内部新建 —— 绝不复用/继承父进程的连接
    conn = db.connect()
    try:
        while True:
            msg = start_q.get()          # 等待本轮 ("GO", mode) 或 "STOP"
            if msg == "STOP":
                break
            mode = msg[1] if isinstance(msg, tuple) else "claim"
            try:
                barrier.wait(timeout=60)  # 10 个 worker 对齐起跑线
            except Exception:
                # barrier 超时/损坏：先上报死亡再退出 —— 父进程据此判 FAIL，
                # 而不是静默少人、把残缺参与误当成"没重复就是赢"。
                out_q.put(("BARRIER_DEAD", worker_id))
                break
            if mode == "reaper":
                # reaper 组合轮：起跑后先各自尝试回收过期租约（advisory
                # 选主收敛为一份实际扫描），随后进入再认领循环。
                claim.reclaim_expired(conn, LEASE_SECONDS)
            elif mode == "report":
                # report 洪泛轮：payload = {tid: (owner_a, epoch_a, owner_b, epoch_b)}，
                # 每个 worker 对全部 (task, step) 混合三种口径洪泛上报，
                # 聚合结果经 out_q 回传后结束本轮（不进入下方认领循环）。
                payload = msg[2] if isinstance(msg, tuple) and len(msg) > 2 else {}
                results = _report_flood(conn, worker_id, payload)
                out_q.put(("REPORTS", worker_id, results))
                out_q.put("DONE")
                continue
            # 循环认领直到 claim_next 返回 None
            while True:
                got = claim.claim_next(conn, worker_id)
                if got is None:
                    break
                task_id, epoch = got     # claim_next 返回 (task_id, claim_epoch)
                out_q.put(("CLAIM", worker_id, task_id, epoch))
            out_q.put("DONE")             # 本轮该 worker 已耗尽队列
    finally:
        conn.close()


def guard_truncate_target(args):
    """TRUNCATE 护栏：必须早于任何连接/写操作执行。
    解析目标库名，不含 "test" 且未显式 --truncate-ok → 打印警告并 exit 2。"""
    url = db.database_url()
    dbname = urlsplit(url).path.lstrip("/")
    if "test" not in dbname.lower() and not args.truncate_ok:
        print(
            f"REFUSING TO RUN: database '{dbname}' does not look like a test db.\n"
            "This script TRUNCATES step_logs/steps/tasks/task_groups.\n"
            "Point DATABASE_URL at a test database, or re-run with "
            "--truncate-ok to acknowledge."
        )
        sys.exit(2)
    return dbname


def reset_and_seed(conn):
    """清空并插入本轮的 TASKS_PER_ROUND 个 pending 任务。"""
    with conn.cursor() as cur:
        cur.execute("TRUNCATE step_logs, steps, tasks, task_groups RESTART IDENTITY")
        cur.execute(
            "INSERT INTO tasks (base_params) "
            "SELECT '{}'::jsonb FROM generate_series(1, %s)",
            (TASKS_PER_ROUND,),
        )
    conn.commit()


def _report_once(conn, fn):
    """执行一次上报，返回 inserted bool；对 DeadlockDetected 回滚后有限重试。

    死锁机理（洪泛轮特有）：围栏路径的 INSERT ... SELECT ... FOR UPDATE
    先拿 tasks 行锁再做 step_logs 投机插入，与无围栏直插的
    （投机插入锁 → tasks 外键 KEY SHARE）锁序相反，同一 (task, step)
    上两路并发会构成锁环——PG 死锁检测牺牲其中一方。
    受害者的语句被中止、【未写入任何行】，回滚后重试等价于重发；
    重试耗尽则如实记为未插入——"每对恰一行"的行不变量始终由
    主键约束兜底，不因死锁而破坏。"""
    for _attempt in range(3):
        try:
            return bool(fn())
        except psycopg.errors.DeadlockDetected:
            conn.rollback()
    return False


def _report_flood(conn, worker_id, payload):
    """单 worker 的洪泛上报循环，返回 (tid, step_index, kind, inserted) 列表。
    三种口径混合（顺序刻意：旧代围栏先行，验证其 0 写入后才开洪泛）：
      - stale_fenced ：持旧代 (owner_a, epoch_a) 围栏 —— 必须 0 写入；
      - fresh_fenced ：持当代 (owner_b, epoch_b) 围栏 —— 仅当代持有者能过围栏；
      - unfenced     ：owner=None 直插路径 —— 主键冲突裁决，恰一人胜出。
    每条上报都记录 inserted 返回值，供父进程做"首报归属"双向核对。"""
    results = []
    for tid, (owner_a, epoch_a, owner_b, epoch_b) in payload.items():
        for si in REPORT_STEP_INDICES:
            ins = _report_once(
                conn,
                lambda tid=tid, si=si: logs.report_step(
                    conn, tid, si, True, worker_id,
                    owner=owner_a, owner_epoch=epoch_a),
            )
            results.append((tid, si, "stale_fenced", ins))
            if worker_id == owner_b:
                ins = _report_once(
                    conn,
                    lambda tid=tid, si=si: logs.report_step(
                        conn, tid, si, True, worker_id,
                        owner=owner_b, owner_epoch=epoch_b),
                )
                results.append((tid, si, "fresh_fenced", ins))
            for _ in range(REPORT_FLOOD_MULT):
                ins = _report_once(
                    conn,
                    lambda tid=tid, si=si: logs.report_step(
                        conn, tid, si, True, worker_id),
                )
                results.append((tid, si, "unfenced", ins))
    return results


def start_phase(start_q, mode, payload=None):
    """给全体 worker 发放本轮起跑信号（mode: claim / reaper / report；
    report 模式附带 payload 载荷）。"""
    for _ in range(WORKERS):
        if payload is None:
            start_q.put(("GO", mode))
        else:
            start_q.put(("GO", mode, payload))


def collect_round(out_q):
    """收齐一轮回传：WORKERS 个 "DONE" 才算结束（barrier 死亡者以
    BARRIER_DEAD 顶额，避免父进程死等永远不会来的 DONE）。
    返回 (claims, dead)：claims 为 (worker_id, task_id, epoch) 列表，
    dead 为死亡/失联的 worker 标记列表（非空即本轮 FAIL）。"""
    claims, dead = [], []
    finished = 0
    while finished < WORKERS:
        try:
            item = out_q.get(timeout=120)
        except queue.Empty:
            # 回传枯竭：有 worker 静默死亡。记 FAIL 证据并结束本轮收集。
            dead.append("QUEUE_TIMEOUT")
            break
        if item == "DONE":
            finished += 1
        elif isinstance(item, tuple) and item[:1] == ("BARRIER_DEAD",):
            dead.append(item[1])
            finished += 1                  # 死者不再产出 DONE，提前计入额度
        elif isinstance(item, tuple) and item[:1] == ("CLAIM",):
            claims.append(item[1:])        # (worker_id, task_id, epoch)
    return claims, dead


def verify_round(conn, claims):
    """诚实判定：DB 侧计数核对 + 队列去重。

    重复认领的判据是队列回传的 id 是否重复（同一任务 id 被两个
    worker 各自认领才会出现）；DB 侧则核对 claimed 行数等于投放数、
    且无 pending 残留（任务不丢）；另核对认领确实由多个 worker
    分摊（distinct claimed_by），排除单进程锁完全部的退化情况。
    """
    claimed_ids = [tid for _wid, tid, _epoch in claims]
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM tasks WHERE status='claimed'")
        claimed_db = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM tasks WHERE status='pending'")
        pending_db = cur.fetchone()[0]
        cur.execute("SELECT count(DISTINCT claimed_by) FROM tasks WHERE status='claimed'")
        workers_db = cur.fetchone()[0]
    dup_queue = len(claimed_ids) - len(set(claimed_ids))
    return dup_queue, claimed_db, pending_db, workers_db


def run_reaper_round(conn, start_q, out_q, emit):
    """claim×reaper 组合攻击轮（返回本轮是否通过）：
    阶段 A 全员并发认领至耗尽 → 父进程 SQL 回填 claimed_at 制造过期
    （不真实等租约耗尽）→ 阶段 B 并发 reclaim_expired + 再认领。
    断言三件事：
      1) 任何任务不被重复持有：两个阶段各自队列去重 + DB 计数双向核对；
      2) 回收后重认领的 epoch 严格递增（回收不清零、认领 +1）；
      3) 持旧代 (claimed_by, claim_epoch) 的僵尸 transition 全部 rowcount=0。
    """
    # ---- 阶段 A：常规并发认领至耗尽 ----
    reset_and_seed(conn)
    start_phase(start_q, "claim")
    claims_a, dead_a = collect_round(out_q)
    if dead_a:
        emit(f"reaper-round phase=A FAIL: barrier dead/timeout workers={dead_a}")
        return False
    ids_a = [tid for _w, tid, _e in claims_a]
    dup_a = len(ids_a) - len(set(ids_a))
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM tasks WHERE status='claimed'")
        claimed_db_a = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM tasks WHERE status='pending'")
        pending_db_a = cur.fetchone()[0]
    phase_a_ok = (
        dup_a == 0
        and len(ids_a) == TASKS_PER_ROUND
        and claimed_db_a == TASKS_PER_ROUND
        and pending_db_a == 0
    )
    emit(
        f"reaper-round phase=A queue_claims={len(ids_a)} dup={dup_a} "
        f"db_claimed={claimed_db_a} db_pending={pending_db_a} "
        f"{'OK' if phase_a_ok else 'FAIL'}"
    )
    if not phase_a_ok:
        return False

    # ---- 制造过期：父进程 SQL 回填 claimed_at（不真实等 60s） ----
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE tasks SET claimed_at = now() - make_interval(secs => %s) "
            "WHERE status IN ('claimed','running')",
            (BACKDATE_SECONDS,),
        )
    conn.commit()

    # ---- 阶段 B：并发 reclaim_expired + 再认领 ----
    start_phase(start_q, "reaper")
    claims_b, dead_b = collect_round(out_q)
    if dead_b:
        emit(f"reaper-round phase=B FAIL: barrier dead/timeout workers={dead_b}")
        return False
    ids_b = [tid for _w, tid, _e in claims_b]
    dup_b = len(ids_b) - len(set(ids_b))
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM tasks WHERE status='claimed'")
        claimed_db_b = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM tasks WHERE status='pending'")
        pending_db_b = cur.fetchone()[0]
    phase_b_ok = (
        dup_b == 0
        and len(ids_b) == TASKS_PER_ROUND   # 回收后全部被重新认领，一个不少
        and claimed_db_b == TASKS_PER_ROUND
        and pending_db_b == 0
        and set(ids_b) == set(ids_a)        # 队列集合再对一遍：回收的就是原班任务
    )
    emit(
        f"reaper-round phase=B queue_reclaims={len(ids_b)} dup={dup_b} "
        f"db_claimed={claimed_db_b} db_pending={pending_db_b} "
        f"{'OK' if phase_b_ok else 'FAIL'}"
    )
    if not phase_b_ok:
        return False

    # ---- 断言 2：回收后重认领 epoch 严格递增 ----
    epoch_a = {tid: epoch for _w, tid, epoch in claims_a}
    epoch_b = {tid: epoch for _w, tid, epoch in claims_b}
    worker_a = {tid: w for w, tid, _e in claims_a}
    epoch_violations = [tid for tid, ep in epoch_b.items() if ep <= epoch_a[tid]]
    emit(f"reaper-round epoch-monotonic violations={len(epoch_violations)} (must be 0)")
    if epoch_violations:
        return False

    # ---- 断言 3：旧代 (claimed_by, epoch) 的僵尸 transition 全部 rowcount=0 ----
    # 任务此刻处于新代 claimed 状态；旧持有者即使 claimed_by 碰巧再次匹配
    # （同一 worker 重认领同一任务），过期 epoch 也永远对不上 → transition False。
    zombie_hits = 0
    for tid, old_epoch in epoch_a.items():
        if claim.transition(
            conn, tid, "claimed", "running",
            claimed_by=worker_a[tid], claim_epoch=old_epoch,
        ):
            zombie_hits += 1
    emit(f"reaper-round stale-epoch transition hits={zombie_hits} (must be 0)")
    return zombie_hits == 0


def reset_and_seed_report(conn):
    """清空并投放洪泛轮的 REPORT_TASKS 个任务（每任务带
    REPORT_STEP_INDICES 全部 step），返回任务 id 列表。"""
    with conn.cursor() as cur:
        cur.execute("TRUNCATE step_logs, steps, tasks, task_groups RESTART IDENTITY")
        cur.execute(
            "INSERT INTO tasks (base_params) "
            "SELECT '{}'::jsonb FROM generate_series(1, %s) RETURNING id",
            (REPORT_TASKS,),
        )
        tids = [r[0] for r in cur.fetchall()]
        for tid in tids:
            for si in REPORT_STEP_INDICES:
                cur.execute(
                    "INSERT INTO steps (task_id, step_index) VALUES (%s, %s)",
                    (tid, si),
                )
    conn.commit()
    return tids


def collect_report_round(out_q):
    """收齐洪泛轮回传：WORKERS 个 "DONE" 才算结束（与 collect_round 同口径，
    barrier 死亡者以 BARRIER_DEAD 顶额）。返回 (reports, dead)：
    reports 为 (worker_id, tid, step_index, kind, inserted) 平铺列表。"""
    reports, dead = [], []
    finished = 0
    while finished < WORKERS:
        try:
            item = out_q.get(timeout=120)
        except queue.Empty:
            dead.append("QUEUE_TIMEOUT")
            break
        if item == "DONE":
            finished += 1
        elif isinstance(item, tuple) and item[:1] == ("BARRIER_DEAD",):
            dead.append(item[1])
            finished += 1
        elif isinstance(item, tuple) and item[:1] == ("REPORTS",):
            wid, results = item[1], item[2]
            reports.extend((wid, tid, si, kind, ins) for tid, si, kind, ins in results)
    return reports, dead


def run_report_round(conn, start_q, out_q, emit, rnd):
    """report_step 洪泛攻击轮（返回本轮是否通过）：
    阶段 A 全员并发认领（建立持有关系）→ 父进程 SQL 回填 claimed_at
    制造过期 → 阶段 B 并发 reclaim + 再认领（旧代持有关系变成 stale）
    → 阶段 C 10 进程对同一批 (task, step) 并发洪泛上报（旧代围栏/
    当代围栏/无围栏混合）。断言三件事：
      1) 每 (task_id, step_index) 恰一行（DB 侧 GROUP BY 逐对核对）；
      2) 首报归属正确：DB 行 worker_id == 队列中唯一报插入成功者；
      3) 旧代围栏上报 0 写入（stale_fenced 的 inserted 全为 False）。
    """
    reset_and_seed_report(conn)

    # ---- 阶段 A：并发认领至耗尽 ----
    start_phase(start_q, "claim")
    claims_a, dead_a = collect_round(out_q)
    if dead_a:
        emit(f"report-round={rnd} phase=A FAIL: barrier dead/timeout workers={dead_a}")
        return False
    ids_a = [tid for _w, tid, _e in claims_a]
    dup_a = len(ids_a) - len(set(ids_a))
    if dup_a or len(ids_a) != REPORT_TASKS:
        emit(f"report-round={rnd} phase=A FAIL: claims={len(ids_a)} dup={dup_a}")
        return False

    # ---- 制造过期：SQL 回填 claimed_at（不真实等租约） ----
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE tasks SET claimed_at = now() - make_interval(secs => %s) "
            "WHERE status IN ('claimed','running')",
            (BACKDATE_SECONDS,),
        )
    conn.commit()

    # ---- 阶段 B：并发 reclaim + 再认领（制造新旧两代） ----
    start_phase(start_q, "reaper")
    claims_b, dead_b = collect_round(out_q)
    if dead_b:
        emit(f"report-round={rnd} phase=B FAIL: barrier dead/timeout workers={dead_b}")
        return False
    ids_b = [tid for _w, tid, _e in claims_b]
    dup_b = len(ids_b) - len(set(ids_b))
    if dup_b or len(ids_b) != REPORT_TASKS or set(ids_b) != set(ids_a):
        emit(f"report-round={rnd} phase=B FAIL: reclaims={len(ids_b)} dup={dup_b}")
        return False
    epoch_a = {tid: e for _w, tid, e in claims_a}
    owner_a = {tid: w for w, tid, _e in claims_a}
    epoch_b = {tid: e for _w, tid, e in claims_b}
    owner_b = {tid: w for w, tid, _e in claims_b}
    if any(epoch_b[tid] <= epoch_a[tid] for tid in epoch_a):
        emit(f"report-round={rnd} phase=B FAIL: epoch not monotonic")
        return False

    # ---- 阶段 C：并发洪泛上报 ----
    payload = {
        tid: (owner_a[tid], epoch_a[tid], owner_b[tid], epoch_b[tid])
        for tid in ids_b
    }
    start_phase(start_q, "report", payload)
    reports, dead_c = collect_report_round(out_q)
    if dead_c:
        emit(f"report-round={rnd} phase=C FAIL: barrier dead/timeout workers={dead_c}")
        return False

    # ---- 断言 3：旧代围栏上报 0 写入 ----
    stale_total = sum(1 for _w, _t, _s, kind, _i in reports if kind == "stale_fenced")
    stale_hits = sum(1 for _w, _t, _s, kind, ins in reports if kind == "stale_fenced" and ins)

    # ---- 断言 1/2 的队列侧：每 (task, step) 恰一个插入成功者 ----
    winners = {}
    dup_wins = 0
    for wid, tid, si, _kind, ins in reports:
        if ins:
            if (tid, si) in winners:
                dup_wins += 1
            else:
                winners[(tid, si)] = wid
    expect_pairs = REPORT_TASKS * len(REPORT_STEP_INDICES)

    # ---- DB 侧核对：每对恰一行 + 归属 == 唯一插入成功者 ----
    with conn.cursor() as cur:
        cur.execute(
            "SELECT task_id, step_index, count(*), min(worker_id), max(worker_id) "
            "FROM step_logs GROUP BY task_id, step_index"
        )
        rows = cur.fetchall()
        cur.execute("SELECT count(*) FROM step_logs")
        total_rows = cur.fetchone()[0]
    row_ok = (
        len(rows) == expect_pairs
        and total_rows == expect_pairs
        and all(cnt == 1 and wmin == wmax for _t, _s, cnt, wmin, wmax in rows)
    )
    attribution_ok = all(
        wmin == winners.get((tid, si)) for tid, si, _cnt, wmin, _wmax in rows
    )
    emit(
        f"report-round={rnd} reports={len(reports)} stale_fenced={stale_total} "
        f"stale_hits={stale_hits} winners={len(winners)}/{expect_pairs} dup_wins={dup_wins} "
        f"db_rows={total_rows}/{expect_pairs} row_ok={row_ok} attribution_ok={attribution_ok}"
    )
    return (
        stale_total == WORKERS * expect_pairs   # 每 worker 对每对都发了旧代围栏上报
        and stale_hits == 0
        and dup_wins == 0
        and len(winners) == expect_pairs
        and row_ok
        and attribution_ok
    )


def main():
    parser = argparse.ArgumentParser(description="multi-process claim attack")
    parser.add_argument(
        "--out",
        default=str(ROOT / "evidence" / "claim_attack_run.log"),
        help="log output path (default: evidence/claim_attack_run.log)",
    )
    parser.add_argument(
        "--truncate-ok",
        action="store_true",
        help="acknowledge TRUNCATE of step_logs/steps/tasks/task_groups "
             "(required when target db name does not contain 'test')",
    )
    parser.add_argument(
        "--no-reaper-round",
        action="store_true",
        help="skip the claim x reaper combo round",
    )
    parser.add_argument(
        "--no-report-round",
        action="store_true",
        help="skip the report_step flood attack rounds",
    )
    args = parser.parse_args()

    # TRUNCATE 护栏：必须早于任何连接/写操作
    dbname = guard_truncate_target(args)

    log_path = Path(args.out)
    if not log_path.is_absolute():
        log_path = ROOT / log_path
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_file = open(log_path, "w", encoding="ascii")

    def emit(line):
        print(line, flush=True)
        log_file.write(line + "\n")
        log_file.flush()

    emit(
        f"claim attack start: db={dbname} rounds={ROUNDS} workers={WORKERS} "
        f"tasks_per_round={TASKS_PER_ROUND} "
        f"reaper_round={'off' if args.no_reaper_round else 'on'} "
        f"report_round={'off' if args.no_report_round else 'on'} "
        f"(report_rounds={REPORT_ROUNDS} tasks={REPORT_TASKS} "
        f"steps={list(REPORT_STEP_INDICES)} flood_mult={REPORT_FLOOD_MULT})"
    )

    ctx = mp.get_context("spawn")
    barrier = ctx.Barrier(WORKERS)
    start_q = ctx.SimpleQueue()
    out_q = ctx.Queue()

    # 进程只创建一次，跨轮复用
    procs = [
        ctx.Process(
            target=worker_main,
            args=(f"W{i:02d}", start_q, out_q, barrier),
            name=f"W{i:02d}",
        )
        for i in range(WORKERS)
    ]
    for p in procs:
        p.start()

    conn = db.connect()
    total_claims = 0
    total_dups = 0
    ok_all = True
    t0 = time.time()
    try:
        for rnd in range(1, ROUNDS + 1):
            reset_and_seed(conn)
            start_phase(start_q, "claim")    # 发放本轮起跑信号
            claims, dead = collect_round(out_q)
            if dead:
                # barrier 死亡/失联：整轮判 FAIL 并终止（带死者继续跑只会假绿）
                emit(f"round={rnd} FAIL: barrier dead/timeout workers={dead}")
                ok_all = False
                break
            dup_queue, claimed_db, pending_db, workers_db = verify_round(conn, claims)
            total_claims += len(claims)
            total_dups += dup_queue
            round_ok = (
                dup_queue == 0
                and claimed_db == TASKS_PER_ROUND
                and pending_db == 0
                and len(claims) == TASKS_PER_ROUND
                and workers_db >= 2
            )
            ok_all = ok_all and round_ok
            emit(
                f"round={rnd} queue_claims={len(claims)} db_claimed={claimed_db} "
                f"db_pending={pending_db} db_workers={workers_db} dup_queue_ids={dup_queue} "
                f"{'OK' if round_ok else 'FAIL'}"
            )
            if not round_ok:
                break

        # claim×reaper 组合攻击轮（常规轮全绿才跑，可用 --no-reaper-round 关闭）
        if ok_all and not args.no_reaper_round:
            reaper_ok = run_reaper_round(conn, start_q, out_q, emit)
            ok_all = ok_all and reaper_ok
            emit(f"reaper-round result={'OK' if reaper_ok else 'FAIL'}")

        # report_step 洪泛攻击轮（前置轮全绿才跑，可用 --no-report-round 关闭）
        if ok_all and not args.no_report_round:
            for rrnd in range(1, REPORT_ROUNDS + 1):
                report_ok = run_report_round(conn, start_q, out_q, emit, rrnd)
                ok_all = ok_all and report_ok
                emit(f"report-round={rrnd} result={'OK' if report_ok else 'FAIL'}")
                if not report_ok:
                    break
    finally:
        for _ in range(WORKERS):
            start_q.put("STOP")
        for p in procs:
            p.join(timeout=30)
            if p.is_alive():
                p.terminate()
        conn.close()
        emit(f"rounds={ROUNDS}, workers={WORKERS}, tasks={total_claims}, duplicate_claims={total_dups}")
        emit(f"result={'PASS' if ok_all else 'FAIL'}, elapsed={time.time() - t0:.2f}s")
        log_file.close()
    sys.exit(0 if ok_all else 1)


if __name__ == "__main__":
    main()
