# board/claim.py —— 任务认领与状态流转
#
# 核心设计（答辩重点）：
#   claim_next 用【单条 UPDATE + 子查询 FOR UPDATE SKIP LOCKED】完成认领，
#   这是原子的：数据库保证一行在同一时刻只能被一个事务锁定。
#   - FOR UPDATE SKIP LOCKED：并发的 worker 遇到已被锁定的候选行直接跳过，
#     不等待、不阻塞、更不会选到同一行 → 结构性地杜绝重复认领。
#   - 单条语句 + 立即 commit：短事务，持锁时间≈一条 UPDATE 的执行时间；
#     认领事务内绝不允许 sleep / IO（那样会拉长锁持有时间，拖垮吞吐）。
#   - 不用"先 SELECT 再 UPDATE"两步走：两步之间存在竞态窗口，
#     两个 worker 可能读到同一行 pending 任务，靠应用层加锁是下策。

# reaper 事务级 advisory 锁的固定键：pg_try_advisory_xact_lock 选主，
# 把 N 个 worker 的重复回收扫描收敛为 1 份（详见 reclaim_expired 注释）。
REAPER_LOCK_KEY = 7701

# 状态机白名单：transition 只允许这些 (from_status, to_status) 翻转。
# 非法翻转在参数校验层直接抛 ValueError，早于任何 SQL —— 结构上杜绝
# 用 CAS 碰运气试探出不在状态机里的路径。
ALLOWED_TRANSITIONS = frozenset({
    ("claimed", "running"),
    ("running", "done"),
    ("running", "failed"),
    ("claimed", "failed"),
})


# 活跃状态围栏片段：心跳/持有者探针/围栏上报共用的 claimed/running 过滤。
ACTIVE_STATUS_FRAG = " AND status IN ('claimed','running')"


def _fence_where(claimed_by, claim_epoch, active_status=False, status_last=False):
    """单一围栏谓词构造器：统一拼装 WHERE 尾巴上的持有者/代际/活跃状态
    围栏片段。logs.report_step 围栏分支、worker 的 _heartbeat_sql 与
    _still_owner 均经此复用同一口径。

    返回 (sql_fragment, params)：
      claimed_by 非 None → 追加 " AND claimed_by=%s"；
      active_status → 追加 ACTIVE_STATUS_FRAG（位置见下）；
      claim_epoch 非 None → 追加 " AND claim_epoch=%s"；
      全关/全 None → 返回 ("", [])，与存量无条件路径逐字等价。
    status_last 控制活跃状态片段的位置：False → 紧跟 claimed_by 之后
    （logs 围栏上报的存量语序）；True → 置于 claim_epoch 之后
    （心跳/持有者探针的存量语序）。

    收敛边界留档（评审修复）：两种语序并存是为【逐字等价】于存量 SQL
    （存量 fencing 用例与 reaper_demo 是等价性门禁）；AND 谓词逻辑上可
    交换，两种语序语义完全一致，仅为字符级保真。若后续放宽逐字等价
    约束，可合并为单一语序。
    transition/release/advance_current_step 一律经此拼装，围栏口径
    单点收敛：改围栏语义只需动这一处。
    """
    frag = ""
    params = []
    if claimed_by is not None:
        frag += " AND claimed_by=%s"
        params.append(claimed_by)
    if active_status and not status_last:
        frag += ACTIVE_STATUS_FRAG
    if claim_epoch is not None:
        frag += " AND claim_epoch=%s"
        params.append(claim_epoch)
    if active_status and status_last:
        frag += ACTIVE_STATUS_FRAG
    return frag, params


def claim_next(conn, worker_id):
    """原子认领一个 pending 任务。返回 (task_id, claim_epoch) 元组；
    无任务返回 None。

    claim_epoch 语义：单调认领代数，每次认领 +1、回收不清零。
    调用方拿到本次认领的 epoch 后，后续 transition/report_step 用它
    做 (claimed_by, claim_epoch) 双匹配围栏 —— 即使任务中途被回收
    后又被（同一个或别的）worker 重新认领，旧代 epoch 也永远对不上。

    语义拆解：
      子查询：在 status='pending' 的任务里按 id 升序取第一条，
              FOR UPDATE 锁行、SKIP LOCKED 跳过已被他人锁住的行；
      外层 UPDATE：仅作用于子查询选中的那一行，写入认领信息并
              把 claim_epoch 递增 1（新代开始）；
      RETURNING id, claim_epoch：把认领到的任务 id 与新一代 epoch 带回来；
      commit：立即提交释放行锁 —— 整个临界区只有这一条语句。
    """
    sql = """
        UPDATE tasks
           SET status='claimed', claimed_by=%s, claimed_at=now(),
               claim_epoch = tasks.claim_epoch + 1
         WHERE id = (
               SELECT id FROM tasks
                WHERE status='pending'
                ORDER BY id
                LIMIT 1
                FOR UPDATE SKIP LOCKED
           )
        RETURNING id, claim_epoch
    """
    with conn.cursor() as cur:
        cur.execute(sql, (worker_id,))
        row = cur.fetchone()
    conn.commit()  # 短事务：拿到结果立即提交，无论是否认领成功
    return (row[0], row[1]) if row else None


def transition(conn, task_id, from_status, to_status, claimed_by=None, claim_epoch=None):
    """条件状态翻转：仅当当前状态 == from_status 时才推进到 to_status。

    单行 UPDATE 带 WHERE status=%s —— 天然 CAS（比较并交换）：
    如果别的 worker 已把状态改掉，本条 UPDATE 影响行数为 0，返回 False。
    这避免了"先查状态再改"的两步竞态。

    claimed_by 围栏（fencing，默认 None 时行为向后兼容）：
    传入 worker_id 后 WHERE 追加 AND claimed_by=%s —— 被 reaper 夺权
    （status 已回 pending、claimed_by 已置 NULL）的旧 worker，后续
    一切 CAS 翻转影响行数都是 0，结构性地拦死僵尸写。

    claim_epoch 双匹配（默认 None 保持存量行为）：非 None 时 WHERE
    追加 AND claim_epoch=%s，与 claimed_by 同口径 —— 堵死"回收后
    重认领同一任务使 claimed_by 再次匹配"的围栏失效漏洞：旧 worker
    手里的过期 epoch 与新代编号对不上，翻转影响行数仍为 0。
    终态翻转（done/failed）顺带打点 finished_at：预留终态打点，
    供排查/审计查询，当前看板未展示。

    状态机白名单：(from_status, to_status) 必须在 ALLOWED_TRANSITIONS
    内，否则参数校验层直接抛 ValueError（消息含 from/to），早于 SQL。
    """
    if (from_status, to_status) not in ALLOWED_TRANSITIONS:
        raise ValueError(
            f"illegal transition: from_status={from_status!r} -> "
            f"to_status={to_status!r} not in allowed state machine"
        )
    sql = """
        UPDATE tasks
           SET status=%s,
               finished_at = CASE WHEN %s IN ('done','failed') THEN now() ELSE finished_at END
         WHERE id=%s AND status=%s
    """
    params = [to_status, to_status, task_id, from_status]
    fence_frag, fence_params = _fence_where(claimed_by, claim_epoch)
    sql += fence_frag
    params.extend(fence_params)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        ok = cur.rowcount == 1
    conn.commit()
    return ok


def reclaim_expired(conn, lease_seconds):
    """reaper：回收租约过期的任务，回到 pending 重新排队（达重试上限则进死信）。

    单条原子 UPDATE：claimed_at 过期（或 claimed_at=NULL 的脏数据）的
    claimed/running 任务清空认领信息并置回 pending；但重试计数达上限的
    任务不再重排队，而是置 failed 进入死信。

    死信语义（schema v3，retry_count/max_retries）：
      - 每次回收 retry_count 累加 1（重试记账，与 claim_epoch 同为回收不清零）；
      - retry_count + 1 >= max_retries → status 置 'failed' 并打点
        finished_at（终态口径与 transition 一致）：必然失败的任务
        不再被无限重试霸占队列，留待人工排查；
      - 未达上限 → status 置 'pending'，finished_at 保持列不动
        （claimed/running 态本无终态打点，语义上恒为 NULL）。
      - RETURNING id 带回全部被回收的任务（含进死信的），语义不变。

    注意【回收不清 claim_epoch】：SET 只清 status/claimed_by/claimed_at，
    epoch 保留累加值 —— 否则"回收后重认领"会让旧 worker 的过期 epoch
    与新代碰巧一致，围栏失效。

    正确性与规模化论证：
      - 行锁天然互斥保证正确性：多个 worker 并发跑 reaper 时，同一行
        只会被一个事务的 UPDATE 改写，其余事务重读时该行已不再命中
        WHERE（status 已变）——即使全员同时回收也不会错。
      - advisory 选主只是收敛浪费：pg_try_advisory_xact_lock 抢不到
        直接返回，把 N 个 worker 的重复回收扫描收敛为 1 份 ——
        规模化后的暴民防护（每个 worker 主循环空闲都会尝试 reaper，
        无锁时是 N 份全表扫描的重复劳动）。
      - 事务级锁随 commit/rollback 自动释放，无清理逻辑、无锁泄漏负担。
      - 回收后任务整体重跑：已上报的 step 由 step_logs 主键幂等挡下，
        不会重复写入；未完成的 step 由新 worker 继续跑。
      - 配合 transition/report_step 的 (claimed_by, claim_epoch) 双匹配
        围栏：被夺权的旧 worker 醒来后一切写操作影响行数为 0，
        即使它重新认领同一任务也持有新代 epoch，无法冒充旧代。
    谓词与函数式索引同形（评审修复）：WHERE 改用
    COALESCE(claimed_at, '-infinity'::timestamptz) < now() - make_interval(...)，
    与 idx_tasks_lease 的表达式索引逐字同形，可被索引 seek（小表下
    规划器仍可能选 Seq Scan，属成本模型预期行为）。语义与旧谓词
    (claimed_at IS NULL OR claimed_at < ...) 完全等价：NULL 脏行映射到
    -infinity 必然命中回收条件（test_recovery 用例⑧是回归门禁）。
    RETURNING id 带回被回收的任务列表；commit 后生效。
    """
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_xact_lock(%s)", (REAPER_LOCK_KEY,))
        elected = cur.fetchone()[0]
    if not elected:
        # 未当选：别的 worker 正在回收。事务级锁随本次 commit 自动释放，
        # 无需清理；直接空手返回。
        conn.commit()
        return []
    sql = """
        UPDATE tasks
           SET retry_count = tasks.retry_count + 1,
               status = CASE WHEN tasks.retry_count + 1 >= tasks.max_retries
                             THEN 'failed' ELSE 'pending' END,
               finished_at = CASE WHEN tasks.retry_count + 1 >= tasks.max_retries
                                  THEN now() ELSE finished_at END,
               claimed_by=NULL, claimed_at=NULL
         WHERE status IN ('claimed','running')
           AND COALESCE(claimed_at, '-infinity'::timestamptz)
               < now() - make_interval(secs => %s)
        RETURNING id
    """
    with conn.cursor() as cur:
        cur.execute(sql, (lease_seconds,))
        ids = [r[0] for r in cur.fetchall()]
    conn.commit()
    return ids


def release(conn, task_id, worker_id, claim_epoch=None):
    """主动释放：worker 把自己刚认领、尚未跑起来的任务退回 pending。

    WHERE 三重条件（id + status='claimed' + claimed_by=%s）：只允许
    当前持有者释放，且任务一旦进入 running（开始干活）就不应再走这条路。
    claim_epoch 双匹配（默认 None 保持存量行为）：非 None 时 WHERE
    追加 AND claim_epoch=%s，与 claimed_by 同口径，防止持有过期代
    令牌的僵尸 worker 误释放新代的任务。
    返回是否真的释放成功（False 表示任务已不在本 worker 手里）。
    """
    sql = """
        UPDATE tasks
           SET status='pending', claimed_by=NULL, claimed_at=NULL
         WHERE id=%s AND status='claimed'
    """
    params = [task_id]
    fence_frag, fence_params = _fence_where(worker_id, claim_epoch)
    sql += fence_frag
    params.extend(fence_params)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        ok = cur.rowcount == 1
    conn.commit()
    return ok


def advance_current_step(conn, task_id, value, worker_id, claim_epoch=None):
    """纯围栏 CAS 推进 current_step：仅当前持有者（+代际）在 running 态可写。

    UPDATE tasks SET current_step=%s
     WHERE id=%s AND status='running' AND claimed_by=%s [AND claim_epoch=%s]

    commit 后返回 bool（rowcount==1）：False 是确定性失权信号，
    调用方应中止任务（无需探针 —— 围栏全条件不匹配没有第二种解释）。
    刻意不含 claimed_at=now()：续租单一职责归心跳线程，与步推进
    现有口径一致（推进退化为纯围栏 CAS）。
    """
    sql = ("UPDATE tasks SET current_step=%s "
           "WHERE id=%s AND status='running'")
    params = [value, task_id]
    fence_frag, fence_params = _fence_where(worker_id, claim_epoch)
    sql += fence_frag
    params.extend(fence_params)
    with conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        ok = cur.rowcount == 1
    conn.commit()
    return ok
