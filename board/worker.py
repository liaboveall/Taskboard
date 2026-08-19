# board/worker.py —— 演示用 worker 进程
# 用法: python -m board.worker --id W1
#
# 主循环：claim_next 认领（返回 (task_id, claim_epoch)）→ claimed→running
#   → 逐 step 执行。每个 step：resolve 出该 step 的生效参数（打印）
#   → sleep 模拟执行 → report_step 幂等上报（带 claimed_by+epoch 双围栏）
#   → 推进 current_step（纯围栏 CAS）。全部成功 → running→done；
#   任一步骤异常 → running→failed。
#
# 心跳模型（批次 4）：续租是 Heartbeat daemon 线程的单一职责——独立连接、
#   每 LEASE_SECONDS//3 秒刷新一次 claimed_at；主循环的步推进不再顺带续租，
#   退化为纯围栏 CAS。心跳 UPDATE 带 (claimed_by, claim_epoch, status) 全围栏：
#   rowcount==0 是【确定性失权信号】（fenced Event），与"连接坏了"（重建重试）
#   严格区分，二者绝不混淆。
#
# 围栏三检查点（批次 4）：
#   ① 每步开始与上报后检查心跳 fenced 事件，置位即中止；
#   ② report_step 返回 False 时不直接中止——False 有"幂等重复"与"失权"两种
#      含义，必须用持有者探针 SELECT 消歧（见 _still_owner 注释）；
#   ③ step 推进 UPDATE 检查 rowcount：0 即确定性失权，直接中止（无需探针）。
#
#   reaper 定期化：主循环用 time.monotonic 节流，距上次回收 ≥ REAP_INTERVAL_SECONDS
#   即跑一轮 reclaim_expired（不再只在队列空时回收）。
# LEASE_SECONDS：租约秒数（环境变量可覆盖，默认 60）。
# 幂等演示已由 tests/test_idempotent_log.py 与 test_api.py 并发用例覆盖，
# worker 不再内置 chaos 重复上报分支。
import argparse
import json
import random
import sys
import threading
import time

from board import claim, db, logconf, logs, params

# 模块导入即配置 logging（幂等）；main() 入口再调一次 setup 仅作显式保证
logger = logconf.setup("board.worker")

# 模块级一次性读取：租约秒数 —— 容错解析：缺失/非法值回退默认并打 WARNING，
# 一条脏环境变量不再把整个 worker 炸掉。
# Heartbeat 心跳线程定期续租 claimed_at，
# reaper 把超过该时长未续租的 claimed/running 任务收回归队
LEASE_SECONDS = db.env_int("LEASE_SECONDS", 60)

# reaper 回收节流间隔：单语句短事务成本可忽略，队列繁忙时也能及时回收过期租约
REAP_INTERVAL_SECONDS = 5


class Heartbeat:
    """租约续租心跳：独立 daemon 线程 + 独立数据库连接。

    为什么自建连接：psycopg 连接非线程安全，绝不能与主循环共享同一条连接；
    为什么独立线程：步执行是模拟 sleep，续租不能依赖"步推进顺带续租"——
    单步执行时间可能逼近租约，必须有心跳专职刷新 claimed_at。

    失权判定严格二分（绝不混淆）：
      - UPDATE rowcount==0：围栏全部条件不匹配 → 【确定性失权】，
        置 fenced Event 并停跳，主循环检查到后中止任务；
      - 连接/语句异常：可能只是网络抖动或连接损坏 → 仅打印日志、
        尽力重建连接下轮重试，【不误判失权】。

    stop() 幂等：置 _stop_event 打断可打断睡眠 → join 等线程收尾
    → 连接关闭（join 后二次兜底，防重连竞态泄漏）；重复调用安全。
    """

    def __init__(self, task_id, worker_id, claim_epoch=None, interval=None):
        # interval 参数注入：生产默认租约的三分之一（≥1s 防除零/过小值），
        # 测试可传 0.05 加快观察续租效果
        if interval is None:
            interval = max(LEASE_SECONDS // 3, 1)
        self.task_id = task_id
        self.worker_id = worker_id
        self.claim_epoch = claim_epoch
        self.interval = interval
        self.fenced = threading.Event()       # 确定性失权信号（rowcount==0）
        self._stop_event = threading.Event()  # 停跳信号；wait 实现可打断睡眠
        self._conn = None
        self._stopped = False
        self._thread = threading.Thread(
            target=self._run, daemon=True, name=f"hb-task-{task_id}"
        )

    def _heartbeat_sql(self):
        """续租 UPDATE：带完整围栏（持有者 + 代际 + 活跃状态）。
        谓词统一经 claim._fence_where(active_status=True, status_last=True)
        拼装，与 logs 围栏上报/_still_owner 同一口径单点收敛；
        claim_epoch 为 None 时省略该条件，与 claim/logs 的可选参口径一致。"""
        frag, params = claim._fence_where(self.worker_id, self.claim_epoch,
                                          active_status=True, status_last=True)
        sql = "UPDATE tasks SET claimed_at=now() WHERE id=%s" + frag
        return sql, tuple([self.task_id] + params)

    def start(self):
        """建独立连接并启动心跳线程。"""
        self._conn = db.connect()  # 独立连接：绝不与主循环共享
        self._thread.start()

    def _reconnect(self):
        """尽力重建连接（连接坏了≠失权）：旧连接尽力关闭，新建失败留待下轮。

        连接泄漏竞态修复（双复检闭环）：新建连接【之前】先查 _stop_event ——
        stop() 已置位则不新建、直接返回；db.connect() 【成功返回后】再复检
        一次 —— connect 带退避重试最坏可达 ~15.7s（远超 stop() 的 join 5s），
        仅前置检查不够：连接建立期间 stop() 可能已走完 join+关连接收尾，
        此时新建的连接若不自检关闭就无人认领（泄漏）。复检置位则关闭
        新连接并返回，交给 stop() 语义收尾。
        """
        try:
            if self._conn is not None:
                self._conn.close()
        except Exception:
            pass
        self._conn = None
        if self._stop_event.is_set():
            return  # 已收到停跳信号：不再新建连接，交给 stop() 收尾
        try:
            new_conn = db.connect()
        except Exception as exc:
            logger.warning("[heartbeat task %s] reconnect failed: %s", self.task_id, exc)
            return
        # 成功返回后复检：connect 最坏 ~15.7s > stop() 的 join 5s，
        # 建连期间收到停跳信号时，新连接必须立即关闭（防泄漏）
        if self._stop_event.is_set():
            try:
                new_conn.close()
            except Exception:
                pass
            return
        self._conn = new_conn

    def _close_conn(self):
        """幂等关闭心跳连接：吞掉 close 异常，重复调用安全。"""
        try:
            if self._conn is not None:
                self._conn.close()
        except Exception:
            pass
        self._conn = None

    def _run(self):
        sql, args = self._heartbeat_sql()
        while not self._stop_event.wait(self.interval):  # 可打断睡眠：stop 秒级收线
            if self._conn is None:
                # 上一轮连接损坏且重建失败：本轮再试一次，仍失败则等下轮
                self._reconnect()
                if self._conn is None:
                    continue
            try:
                with self._conn.cursor() as cur:
                    cur.execute(sql, args)
                    renewed = cur.rowcount == 1
                self._conn.commit()
            except Exception as exc:
                # 连接/语句异常≠失权：打日志、重建连接，下轮重试
                logger.warning("[heartbeat task %s] error (will retry): %s",
                               self.task_id, exc)
                self._reconnect()
                continue
            if not renewed:
                # rowcount==0：(claimed_by, epoch, status) 围栏不匹配——
                # 任务已被回收/易主/进终态，这是确定性失权信号
                logger.warning("[heartbeat task %s] fenced: heartbeat matched 0 rows",
                               self.task_id)
                self.fenced.set()
                return

    def stop(self):
        """停跳并回收连接。幂等：重复调用直接返回。
        join 后连接关两次（二次兜底，均幂等）：防 join 等待窗口内
        心跳线程刚重建的连接无人认领（配合 _reconnect 的 stop 前置检查）。"""
        if self._stopped:
            return
        self._stopped = True
        self._stop_event.set()
        try:
            if self._thread.is_alive():
                self._thread.join(timeout=5)
        finally:
            self._close_conn()
            # 二次兜底：若 join 超时后线程仍在收尾瞬间新建了连接，再幂等关一次
            self._close_conn()


def load_task(conn, task_id):
    """读取任务上下文：base_params、组级 override、按序的 step override 列表。"""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.base_params,
                   COALESCE(g.override_params, '{}'::jsonb) AS group_params
              FROM tasks t
              LEFT JOIN task_groups g ON g.id = t.group_id
             WHERE t.id = %s
            """,
            (task_id,),
        )
        task_row = cur.fetchone()
        cur.execute(
            """
            SELECT step_index, override_params
              FROM steps WHERE task_id = %s
             ORDER BY step_index
            """,
            (task_id,),
        )
        step_rows = cur.fetchall()
    if task_row is None:
        # 认领后、执行前任务凭空消失（如被外部删除）：立即抛错走统一异常路径，
        # 绝不拿 None 硬凑继续跑 —— 否则错误会被吞成静默的幽灵任务。
        raise LookupError(f"task {task_id} vanished")
    base = task_row[0] if task_row[0] is not None else {}
    group = task_row[1] if task_row[1] is not None else {}
    # 返回 (step_index, override) 对：step_index 不保证从 1 连续，
    # 后续上报与推进必须用真实编号（与 api.py 末步判定口径统一）
    step_defs = [(r[0], r[1] or {}) for r in step_rows]
    return base, group, step_defs


def _safe_rollback(conn):
    """尽力回滚；连接已断时吞掉异常。返回连接是否仍然可用。"""
    try:
        conn.rollback()
        return True
    except Exception:
        # 吞异常但留痕：连接断开细节只进 DEBUG，不改变“返回 False”控制流
        logger.debug("rollback failed (connection likely broken)", exc_info=True)
        return False


def _still_owner(conn, task_id, worker_id, claim_epoch=None):
    """持有者探针：我此刻是否仍是该任务的合法持有者？

    为什么需要它：report_step 返回 False 有【两种】含义——
      ① 主键冲突（幂等重复）：含回收重跑时新主人对旧 step 的合法重报，
         此时应继续执行，"False 即中止"会错杀合法路径；
      ② 围栏拦下（已失权）：任务被回收易主，继续执行就是僵尸写。
    探针用与心跳同口径的围栏条件 SELECT 一次消歧（谓词统一经
    claim._fence_where(active_status=True, status_last=True) 拼装，与心跳
    逐字同形）：仍在 → True（幂等重复，继续）；不在 → False（失权，中止）。
    探针自身失败按失权处理：连持有状态都无法确认时继续执行只会扩大越权写。
    """
    frag, params = claim._fence_where(worker_id, claim_epoch,
                                      active_status=True, status_last=True)
    sql = "SELECT 1 FROM tasks WHERE id=%s" + frag
    args = [task_id] + params
    try:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(args))
            still = cur.fetchone() is not None
        conn.rollback()  # 纯读探针：回滚释放快照，不留事务痕迹
        return still
    except Exception:
        # 探针自身异常按失权处理（见函数 docstring）；异常现场补 DEBUG 留痕，
        # 排查“为什么被误判失权”时可见真实原因
        logger.debug("ownership probe failed for task %s (treating as fenced)",
                     task_id, exc_info=True)
        _safe_rollback(conn)
        return False


def _mark_failed(conn, task_id, worker_id, step_idx, reason, claim_epoch=None, duration_ms=None):
    """异常路径统一处理：先上报失败日志，再翻转状态。

    顺序有讲究：先 rollback 清掉中止的事务，再对出错的 step 调用
    report_step(success=False)——让"失败日志"有真实产生路径；
    然后 running→failed，不成功再兜底 claimed→failed（覆盖
    尚未翻到 running 就抛异常的场景）。每一步都单独兜异常，
    连接不可用时直接放弃（由调用方 break 退出主循环）。
    step_idx=None 表示无 step 上下文（如 load_task 阶段抛异常）：
    此时【不写 step_logs】，避免给不存在的 step 0 留幽灵失败日志。

    claim_epoch（尾部可选参，向后兼容）：透传给 transition/report_step
    做代际双匹配围栏。duration_ms（尾部可选参，schema 版本 2 审计列）：
    该步开始到失败上报的耗时，由调用方在异常现场计量后传入；
    失败上报同样带 channel='worker'。注意失败路径 report_step 返回
    False 时沿用中性告警、【不做持有者探针】——失败路径语义不同：
    任务反正要标 failed，无需为"继续执行"消歧，写不进库就落日志。
    """
    if not _safe_rollback(conn):
        logger.error("[%s] task %s: connection lost, giving up", worker_id, task_id)
        return False
    if step_idx is not None:
        try:
            inserted = logs.report_step(
                conn, task_id, step_idx, False, worker_id,
                owner=worker_id, error_message=str(reason)[:logs.ERROR_MESSAGE_MAX_LEN],
                owner_epoch=claim_epoch,
                channel='worker', epoch=claim_epoch, duration_ms=duration_ms,
            )
            if not inserted:
                # 中性归因：写不进库可能是重复（first-report-wins）或被围栏拦下，
                # 不再断言是哪一种；这条结构化 WARNING 是它最后的痕迹。
                logger.warning("failure log not written: task=%s step=%s worker=%s reason=fenced",
                               task_id, step_idx, worker_id)
        except Exception:
            # 失败日志写库异常：控制流不变（先兜底回滚再决定是否放弃），
            # 补 DEBUG 留痕，避免“失败上报为什么没写进去”无痕可查
            logger.debug("failure log report failed for task %s step %s",
                         task_id, step_idx, exc_info=True)
            if not _safe_rollback(conn):
                return False
    try:
        # 两次翻转都带 claimed_by（+epoch）围栏：被夺权的僵尸写影响行数为 0
        running_ok = claim.transition(conn, task_id, "running", "failed",
                                      claimed_by=worker_id, claim_epoch=claim_epoch)
        claimed_ok = True
        if not running_ok:
            claimed_ok = claim.transition(conn, task_id, "claimed", "failed",
                                          claimed_by=worker_id, claim_epoch=claim_epoch)
        if not running_ok and not claimed_ok:
            # 两条路都没走通：任务已被 reaper 回收或被其它路径改写，如实告知
            logger.warning("[%s] task %s: task left 'claimed/running' "
                           "by another path (likely reclaimed)", worker_id, task_id)
    except Exception:
        # 状态翻转异常：同样不改控制流，补 DEBUG 留痕后走兜底回滚判定
        logger.debug("failed transition error for task %s", task_id, exc_info=True)
        if not _safe_rollback(conn):
            return False
    step_label = step_idx if step_idx is not None else "-"
    logger.error("[%s] task %s failed at step %s: %s", worker_id, task_id, step_label, reason)
    return True


def run_task(conn, task_id, worker_id, claim_epoch=None):
    """执行一个已认领的任务。返回 True=done，False=failed/跳过/失权中止。

    claim_epoch（尾部可选参，向后兼容）：本次认领的代际令牌，
    透传给 transition/report_step/步推进/心跳做 (claimed_by, claim_epoch)
    双匹配围栏；None 时各 SQL 省略该条件，与 claim/logs 口径一致。
    """
    # claimed -> running：单次条件翻转（删除了旧 5 次重试循环）。
    # 为什么不再重试：认领是原子短事务，认领成功即已持有；翻转失败只可能是
    # 状态已被其它路径改写（如刚被回收）——重试改变不了结果，及时 release+skip
    # 才是正解。翻转带 claimed_by+epoch 双围栏：被夺权的旧 worker CAS 影响 0 行。
    try:
        started = claim.transition(conn, task_id, "claimed", "running",
                                   claimed_by=worker_id, claim_epoch=claim_epoch)
    except Exception:
        if not _safe_rollback(conn):
            return False
        started = False
    if not started:
        # 翻转失败：跳过任务前先把任务归还 pending（比等租约到期更及时）；
        # release 带 id+status='claimed'+claimed_by（+epoch）条件，只有当前持有者能释放。
        try:
            released = claim.release(conn, task_id, worker_id, claim_epoch=claim_epoch)
            logger.info("[%s] task %s: released back to pending: %s", worker_id, task_id, released)
        except Exception as exc:
            logger.error("[%s] task %s: release failed: %s", worker_id, task_id, exc)
            _safe_rollback(conn)
        logger.warning("[%s] task %s: claimed->running transition failed, skipping",
                       worker_id, task_id)
        return False

    base, group, step_defs = load_task(conn, task_id)
    # 一次性解析出全部 step 的生效参数快照（纯函数，与执行解耦）
    snapshots = params.resolve(base, group, [ov for _, ov in step_defs])
    indices = [si for si, _ in step_defs]
    if not snapshots:
        # 取舍（替代旧幽灵步方案）：steps 表是唯一事实源。无 steps 的任务
        # 直接 running→done，不伪造“幽灵 step 1”——与 api.py 的 report 404
        # 校验（step 不在 steps 表）和前端展示口径统一：没有步就是空任务，不硬造。
        # 检查 transition 返回值：快进前若已被夺权，如实报失败而非谎报成功。
        # 刻意【不启心跳】：无步任务瞬时就完成，不值得为它开第二条数据库连接。
        if claim.transition(conn, task_id, "running", "done",
                            claimed_by=worker_id, claim_epoch=claim_epoch):
            logger.info("[%s] task %s: no steps, fast-forwarded to done", worker_id, task_id)
            return True
        logger.warning("[%s] task %s: ownership lost before fast-forward "
                       "(likely reclaimed)", worker_id, task_id)
        return False

    # 翻到 running 后先把 current_step 指向即将执行的真实步号：
    # 纯围栏 CAS（claim.advance_current_step），与步推进同口径；
    # 返回 False 即确定性失权，记录后中止（不启心跳、不写失败日志——
    # 失权后任何状态写都是越权）。
    if not claim.advance_current_step(conn, task_id, indices[0], worker_id,
                                      claim_epoch=claim_epoch):
        logger.warning("[%s] task %s: fenced out, aborting", worker_id, task_id)
        return False

    # 有 steps 的任务：启动心跳独立续租（续租单一职责，见文件头心跳模型）。
    # try/finally 包住整个 step 循环：无论正常完成、失权中止还是异常，
    # 心跳线程与它的独立连接都必须被回收。
    hb = Heartbeat(task_id, worker_id, claim_epoch=claim_epoch)
    hb.start()
    try:
        for pos, (idx, effective) in enumerate(zip(indices, snapshots)):
            # 检查点①a：每步开始前检查心跳围栏——上一轮执行期间若已失权，
            # 不进入新的一步（确定性失权，无需探针）。
            if hb.fenced.is_set():
                logger.warning("[%s] task %s: fenced out, aborting", worker_id, task_id)
                return False
            # 步耗时计量（schema 版本 2 审计列 duration_ms）：从步开始
            # （围栏检查通过后）到上报时刻的时间差，毫秒 int；
            # 成功/失败两条上报路径共用同一计时起点。
            step_started = time.monotonic()
            try:
                logger.info("[%s] task %s step %s params=%s", worker_id, task_id, idx,
                            json.dumps(effective, ensure_ascii=False, sort_keys=True, allow_nan=False))
                time.sleep(random.uniform(0.3, 1.0))  # 模拟执行耗时
                duration_ms = int((time.monotonic() - step_started) * 1000)
                inserted = logs.report_step(conn, task_id, idx, True, worker_id,
                                            owner=worker_id, owner_epoch=claim_epoch,
                                            channel='worker', epoch=claim_epoch,
                                            duration_ms=duration_ms)
                # 检查点②：report_step 返回 False 禁止"False 即中止"——
                # 用持有者探针消歧：仍是持有者→幂等重复（含回收重跑的合法
                # 重报），继续执行；失权→中止（失权后任何状态写都是越权，
                # 不调 _mark_failed）。
                if not inserted:
                    if _still_owner(conn, task_id, worker_id, claim_epoch):
                        # 幂等重复上报降级 DEBUG：默认级别不刷屏，事件仍留痕
                        logger.debug("[%s] task %s step %s: report duplicate "
                                     "(idempotent), continuing", worker_id, task_id, idx)
                    else:
                        logger.warning("[%s] task %s: fenced out, aborting", worker_id, task_id)
                        return False
                # 检查点①b：上报后、推进前再查一次围栏，缩短失权后的越权窗口
                if hb.fenced.is_set():
                    logger.warning("[%s] task %s: fenced out, aborting", worker_id, task_id)
                    return False
                # 检查点③ 推进 current_step：指向下一个待执行 step 的真实编号；
                # 末步完成后保持末步编号（clamp，避免看板出现 4/3 越界显示）。
                # WHERE status='running' + claimed_by（+epoch）围栏：任务被夺权后
                # 不再覆盖写；【刻意删除 claimed_at=now()】——续租单一职责归心跳，
                # 推进退化为纯围栏 CAS。rowcount==0 是确定性失权（无需探针）：
                # 不走 _mark_failed（失权后任何状态写都是越权），直接中止。
                next_idx = indices[pos + 1] if pos + 1 < len(indices) else idx
                advanced = claim.advance_current_step(conn, task_id, next_idx, worker_id,
                                                      claim_epoch=claim_epoch)
                if not advanced:
                    logger.warning("[%s] task %s: fenced out, aborting", worker_id, task_id)
                    return False
            except Exception as exc:
                # 异常路径：rollback → 报失败日志（带耗时审计）→ 翻转 failed（带 epoch 围栏）。
                # 注：推进 rowcount==0 的失权路径已在上方 return，不会进到这里。
                _mark_failed(conn, task_id, worker_id, idx, exc, claim_epoch=claim_epoch,
                             duration_ms=int((time.monotonic() - step_started) * 1000))
                return False
    finally:
        hb.stop()

    # running -> done；CAS 失败说明状态已被其它路径改写（如被回收），
    # 如实返回 False（符合文件头契约：True 仅当任务真正翻到 done）
    if claim.transition(conn, task_id, "running", "done",
                        claimed_by=worker_id, claim_epoch=claim_epoch):
        logger.info("[%s] task %s: done", worker_id, task_id)
        return True
    logger.warning("[%s] task %s: already left 'running' before done transition",
                   worker_id, task_id)
    return False


def main():
    logconf.setup("board.worker")  # 幂等：模块导入时已配置过一次
    parser = argparse.ArgumentParser(description="taskboard worker")
    parser.add_argument("--id", required=True, help="worker id, e.g. W1")
    args = parser.parse_args()
    worker_id = args.id

    conn = db.connect()  # 每个进程自己建连接，绝不跨进程共享
    logger.info("[%s] started", worker_id)
    last_reap = 0.0  # 初始为 0：首轮循环即触发一次回收
    try:
        while True:
            # reaper 定期化（评审修复）：不再只在队列空时回收。
            # 用 time.monotonic 节流：距上次回收 ≥ REAP_INTERVAL_SECONDS 即执行
            # 一次 reclaim_expired。单语句短事务成本可忽略，队列繁忙时
            # 也能及时回收过期租约。reaper 自身异常与 claim_next 同样兜底：
            # rollback 后继续；连接不可用则退出。
            if time.monotonic() - last_reap >= REAP_INTERVAL_SECONDS:
                try:
                    recovered = claim.reclaim_expired(conn, LEASE_SECONDS)
                    last_reap = time.monotonic()
                    if recovered:
                        logger.info("[%s] reclaimed expired tasks: %s", worker_id, recovered)
                except Exception as exc:
                    logger.warning("[%s] reclaim_expired error: %s", worker_id, exc)
                    if not _safe_rollback(conn):
                        logger.error("[%s] connection lost, exiting", worker_id)
                        return 1  # 连接丢失：非正常退出码，供进程管理器识别
                    time.sleep(1)
                    continue
            # 主循环健壮性：claim_next 异常不让进程整体崩溃，
            # rollback 后 sleep 1s 继续；连接彻底不可用时才退出。
            try:
                # claim_next 返回 (task_id, claim_epoch) 元组：epoch 是本次认领的
                # 代际令牌，向下透传给 run_task → transition/report_step/心跳围栏
                claimed = claim.claim_next(conn, worker_id)
            except Exception as exc:
                logger.warning("[%s] claim_next error: %s", worker_id, exc)
                if not _safe_rollback(conn):
                    logger.error("[%s] connection lost, exiting", worker_id)
                    return 1
                time.sleep(1)
                continue
            if claimed is None:
                # 队列空：短睡后继续轮询（回收已由上方定期 reaper 承担）
                time.sleep(1)
                continue
            task_id, epoch = claimed
            logger.info("[%s] claimed task %s (epoch %s)", worker_id, task_id, epoch)
            try:
                run_task(conn, task_id, worker_id, claim_epoch=epoch)
            except Exception as exc:
                # run_task 外围兜底（如 load_task 抛异常）：
                # rollback → 翻转 failed（无 step 上下文，不写 step_logs）；连接不可用则退出
                logger.error("[%s] task %s unexpected error: %s", worker_id, task_id, exc)
                if not _mark_failed(conn, task_id, worker_id, None, exc, claim_epoch=epoch):
                    logger.error("[%s] connection lost, exiting", worker_id)
                    return 1
    except KeyboardInterrupt:
        # 刻意不强改在跑任务的状态：正在执行的任务留在 running，
        # 由租约到期 + reaper 兜底回收——比退出时“紧急标 failed”更稳，
        # 避免与数据库断开时的最后一次写操作竞争。
        logger.info("[%s] interrupted, exiting", worker_id)
        return 0
    finally:
        conn.close()
    return 0  # 正常结束


if __name__ == "__main__":
    sys.exit(main())
