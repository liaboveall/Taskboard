# board/logs.py —— 步骤结果上报（幂等，first-report-wins）
#
# 核心设计：
#   step_logs 的主键是 (task_id, step_index) —— 每个 step 的日志结构性地
#   至多一行。用 INSERT ... ON CONFLICT DO NOTHING 实现幂等上报：
#   - 首次写入成功：返回 inserted=True；
#   - 重复上报（无论 success 是真是假）：被唯一约束挡下，什么都不做，
#     返回 inserted=False。
#   "first-report-wins"：一条 success=True 的记录【结构性地】不可能被
#   后续的 success=False 覆盖 —— 因为根本不存在 UPDATE/覆盖路径，
#   冲突时唯一的动作就是 DO NOTHING。这不需要任何应用层判断。
#
#   inserted 的判据：RETURNING 只回报【实际写入】的行。首次插入返回
#   一行（值恒为 1）；冲突命中 DO NOTHING 时【不返回任何行】，
#   fetchone() 得到 None —— 这就是重复上报的唯一信号。
#   （注：常见写法 RETURNING (xmax = 0) 是给 ON CONFLICT DO UPDATE 用的：
#   DO UPDATE 冲突时会返回被更新的行，靠 xmax 区分"新插入/更新而来"；
#   在 DO NOTHING 下冲突根本不返回行，那个表达式永远求值不到，
#   属于死代码——这里直接 RETURNING 1。）

# 失败上报异常摘要的入库截断长度（单一事实源：worker 失败路径同样引用本常量）
ERROR_MESSAGE_MAX_LEN = 500

# 围栏谓词单点收敛（评审修复）：围栏分支的 WHERE 尾巴与 worker 心跳/
# 持有者探针复用 claim._fence_where 同一构造器（active_status=True 的
# logs 存量语序：status 紧跟 claimed_by，逐字等价于存量 SQL）。
from board.claim import _fence_where  # noqa: E402（claim 不反向依赖 logs，无环）


def report_step(conn, task_id, step_index, success, worker_id, owner=None, error_message=None, owner_epoch=None,
                channel=None, epoch=None, duration_ms=None):
    """上报某 step 的执行结果。返回 True 表示本次是首次写入。

    重复调用（哪怕带着相反的 success 值）不会改变已有记录。

    owner（claimed_by 围栏，默认 None 行为向后兼容）：
      - owner=None：直插 VALUES，API 手动演示通道用（此时没有 worker
        持有关系可言，无需围栏），仅多写 error_message 列。
      - owner 非 None：改为 INSERT ... SELECT FROM tasks WHERE
        id=%s AND claimed_by=%s AND status IN ('claimed','running')
        —— 被 reaper 夺权的僵尸 worker（任务已回 pending 且
        claimed_by=NULL，或已换了新主人）连日志都写不进：SELECT 查
        不到匹配行，INSERT 自然没有输入，影响 0 行。
        status 过滤是补 owner 围栏的漏洞：终态（done/failed）任务
        的 claimed_by 不再清理，若只校验 claimed_by，终态后到达的
        迟到写会因 claimed_by 仍匹配而误插入；限定活跃状态后堵死。
      - owner_epoch（可选，默认 None 保持存量行为）：非 None 时
        WHERE 追加 AND claim_epoch=%s，与 owner 同口径双匹配——
        堵死"回收后重认领同一任务使 claimed_by 再次匹配"的迟到写。
        数据源 SELECT 带 FOR UPDATE（PostgreSQL 允许 INSERT ... SELECT ... FOR UPDATE）：
        使围栏读与 reaper 回收串行化 —— READ COMMITTED 下若不加锁，
        本事务快照可能先于 reaper 提交，僵尸写入窗口残留；
        加锁后 CAS 与围栏两条路径的竞态强度对齐。

    error_message：失败上报的异常摘要，统一截断 ERROR_MESSAGE_MAX_LEN 字符入库。

    审计三列（尾部可选参，schema 版本 2；全部默认 None = 写 NULL，
    既有位置参/默认行为零变化，ON CONFLICT DO NOTHING 语义不变）：
      - channel：上报通道（'worker'/'manual'）；
      - epoch：worker 上报时持有的 claim_epoch（手动通道无持有关系）；
      - duration_ms：该步执行到上报的耗时（毫秒 int）。
    """
    err = (error_message or None) and str(error_message)[:ERROR_MESSAGE_MAX_LEN]
    # 审计三列统一在尾部拼接：None 直接写 NULL，与存量行为（列缺省）等价
    audit = (channel, epoch, duration_ms)
    if owner is None:
        sql = """
            INSERT INTO step_logs (task_id, step_index, success, reported_at, worker_id, error_message,
                                     channel, claim_epoch, duration_ms)
            VALUES (%s, %s, %s, now(), %s, %s, %s, %s, %s)
            ON CONFLICT (task_id, step_index) DO NOTHING
            RETURNING 1
        """
        args = (task_id, step_index, success, worker_id, err) + audit
    else:
        # 围栏 INSERT：数据来源是“任务此刻仍由本 worker 持有（活跃状态）”
        # 的那一行；被夺权/进终态后 SELECT 空集 → INSERT 空集，僵尸写被
        # 结构性拦死。围栏谓词统一经 claim._fence_where 拼装（与 worker
        # 心跳/_still_owner 同一口径；owner_epoch 非 None 时叠加代际双匹配）。
        # FOR UPDATE 使围栏读与 reaper 回收串行化，消除 READ COMMITTED 下
        # “快照先于 reaper 提交”的僵尸写入窗口；CAS 与围栏两条路径竞态强度对齐。
        fence_frag, fence_params = _fence_where(owner, owner_epoch, active_status=True)
        where = "id=%s" + fence_frag
        args = [task_id, step_index, success, worker_id, err] + list(audit) \
            + [task_id] + fence_params
        sql = f"""
            INSERT INTO step_logs (task_id, step_index, success, reported_at, worker_id, error_message,
                                   channel, claim_epoch, duration_ms)
            SELECT %s, %s, %s, now(), %s, %s, %s, %s, %s
              FROM tasks
             WHERE {where}
             FOR UPDATE
            ON CONFLICT (task_id, step_index) DO NOTHING
            RETURNING 1
        """
        args = tuple(args)
    with conn.cursor() as cur:
        cur.execute(sql, args)
        row = cur.fetchone()
    conn.commit()  # 短事务立即提交
    # 冲突（重复上报）或围栏拦下时 RETURNING 不返回行 → row is None
    return row is not None
