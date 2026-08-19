# scripts/e2e_prep.py —— 浏览器 E2E 静态演示窗口制造器（只读/只写【演示库】）
#
# 用途（T7 第 4 步）：为后续浏览器 E2E 验证制造一个不会被 worker 消费的
#   静态 running 任务——worker 不启动，任务将一直保持 running，供前端展示与
#   手动上报验证使用。
#
# 操作范围（严格约束）：
#   - 只动【一个】任务：演示库 status='pending' 且 id 最小的那一个；
#   - 翻转字段：status='running'、claimed_by='W-e2e'、claim_epoch+1、
#     claimed_at=now()、current_step=该任务最小 step_index；
#   - UPDATE 带 status='pending' 条件 CAS：重复执行幂等安全（已翻过则不再命中），
#     绝不影响第二个任务；
#   - 执行后回读并打印窗口明细（task id / claim_epoch / current_step /
#     step_index 列表），浏览器代理按此构造上报请求。
#
# 用法：.venv\Scripts\python.exe scripts\e2e_prep.py
import os
import sys
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parent.parent


def _read_demo_url():
    """演示库连接串：优先进程环境变量，再解析 taskboard/.env（与 board.db 同语义）。"""
    url = os.environ.get("DATABASE_URL")
    if url:
        return url
    env_file = ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            if key.strip() == "DATABASE_URL":
                return value.strip().strip('"').strip("'")
    raise RuntimeError("DATABASE_URL not set")


def main():
    url = _read_demo_url()
    if "taskboard_test" in url:
        print("REFUSING: e2e_prep targets the DEMO db, not taskboard_test")
        return 2
    conn = psycopg.connect(url, connect_timeout=5)
    try:
        with conn.cursor() as cur:
            # 窗口已存在（重复执行）→ 直接回读打印，不二次翻转
            cur.execute(
                "SELECT id FROM tasks WHERE claimed_by='W-e2e' AND status='running'"
            )
            row = cur.fetchone()
            if row is None:
                cur.execute(
                    "SELECT id FROM tasks WHERE status='pending' ORDER BY id LIMIT 1"
                )
                row = cur.fetchone()
                if row is None:
                    print("FATAL: demo db has no pending task to flip")
                    return 1
                tid = row[0]
                cur.execute(
                    "SELECT min(step_index) FROM steps WHERE task_id=%s", (tid,)
                )
                first_step = cur.fetchone()[0]
                if first_step is None:
                    print(f"FATAL: task {tid} has no steps; pick another candidate")
                    return 1
                cur.execute(
                    "UPDATE tasks SET status='running', claimed_by='W-e2e', "
                    "claim_epoch=claim_epoch+1, claimed_at=now(), current_step=%s "
                    "WHERE id=%s AND status='pending'",
                    (first_step, tid),
                )
                if cur.rowcount != 1:
                    conn.rollback()
                    print(f"FATAL: flip CAS missed for task {tid} (state changed)")
                    return 1
            conn.commit()

            # 回读窗口明细
            cur.execute(
                "SELECT id, status, claimed_by, claim_epoch, current_step "
                "FROM tasks WHERE claimed_by='W-e2e' AND status='running'"
            )
            tid, status, owner, epoch, current_step = cur.fetchone()
            cur.execute(
                "SELECT step_index FROM steps WHERE task_id=%s ORDER BY step_index",
                (tid,),
            )
            steps = [r[0] for r in cur.fetchall()]
            cur.execute(
                "SELECT step_index FROM step_logs WHERE task_id=%s ORDER BY step_index",
                (tid,),
            )
            logged = [r[0] for r in cur.fetchall()]
            cur.execute("SELECT count(*) FROM tasks WHERE status='pending'")
            pending_left = cur.fetchone()[0]

        print(f"E2E_STATIC_WINDOW_OK task_id={tid} status={status} "
              f"claimed_by={owner} claim_epoch={epoch} current_step={current_step}")
        print(f"E2E_STATIC_WINDOW_STEPS {steps}")
        print(f"E2E_STATIC_WINDOW_LOGGED {logged}")
        print(f"E2E_STATIC_WINDOW_PENDING_LEFT {pending_left}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
