# tests/reaper_demo.py —— "回收重跑 × 幂等"一键复现脚本（正式脚本，非 pytest 收集）
#
# 文件名刻意不带 test_ 前缀：pytest 不收集，可独立反复运行：
#   .venv\Scripts\python.exe tests\reaper_demo.py
#
# 只写隔离库 taskboard_test（绝不碰演示库 taskboard）：
#   连接串从进程环境 DATABASE_URL 或 taskboard/.env 推导，仅把 dbname 替换为
#   taskboard_test（其余连接参数复用）；库不存在则先建库并执行 schema.sql。
#
# 复现语义（复刻历史演示）：
#   1. 清理上一次演示残留（按 base_params.demo 标记），创建含 3 步的专用任务；
#   2. 以 LEASE_SECONDS=8 启动 W1 子进程（python -m board.worker --id W1）；
#   3. 轮询 DB 等 W1 提交完 step 1（step_logs 出现 step 1 行）后 kill W1；
#   4. 以同短租约启动 W2：其定期 reaper 在租约过期后回收任务（epoch 不清零），
#      再重认领接管（epoch+1），step 1 的重复上报被 first-report-wins 挡下；
#   5. 等任务 done 后 SQL 硬断言：
#      - tasks.status='done'；
#      - step_logs 恰 3 行：step1 仅 1 行且 worker_id='W1'（幂等挡下 W2 重报），
#        step2/step3 worker_id='W2'；
#      - claim_epoch >= 2（回收后重认领，代际递增）。
#
# 子进程管理：subprocess.Popen + 管道（独立守护线程抽吸 stdout，防管道缓冲死锁；
# 沙箱下避免 shell 后台进程）；try/finally 保证结束前杀掉全部子进程。
# 运行时长约 15-25s（短租约 8s + 轮询 DB 而非长 sleep），上限 1 分钟。
# 输出覆盖写入 evidence/reaper_epoch_demo.log，结尾打印 VERDICT: PASS/FAIL。
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import psycopg

ROOT = Path(__file__).resolve().parent.parent
TEST_DBNAME = "taskboard_test"
DEMO_MARK = "reaper-epoch-demo"     # base_params.demo 标记：定位/清理演示任务
LEASE_SECONDS = 8                   # 短租约：压缩"等过期"窗口
STEP1_WAIT_TIMEOUT = 20             # 等 W1 提交 step 1 的上限（秒）
DONE_WAIT_TIMEOUT = 45              # 等任务 done 的上限（秒）
EVIDENCE_PATH = ROOT / "evidence" / "reaper_epoch_demo.log"

_lines = []


def log(msg):
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    _lines.append(line)


# ---------- 连接串推导与隔离库供给（与 tests/conftest.py 同口径） ----------

def _read_source_url():
    """优先进程环境变量，再逐行解析 taskboard/.env（与 board.db.load_env 同语义）。"""
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
    return None


def _with_dbname(url, dbname):
    parts = urlsplit(url)
    return urlunsplit(parts._replace(path="/" + dbname))


def _ensure_test_db(source_url):
    """确保 taskboard_test 存在并已建表，返回测试库连接串。"""
    test_url = _with_dbname(source_url, TEST_DBNAME)
    maint_url = _with_dbname(source_url, "postgres")
    maint = psycopg.connect(maint_url, autocommit=True, connect_timeout=5)
    try:
        with maint.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (TEST_DBNAME,))
            exists = cur.fetchone() is not None
        if not exists:
            with maint.cursor() as cur:
                cur.execute(f'CREATE DATABASE "{TEST_DBNAME}"')
            log(f"created database {TEST_DBNAME}")
    finally:
        maint.close()
    schema_sql = (ROOT / "schema.sql").read_text(encoding="utf-8")
    tconn = psycopg.connect(test_url, autocommit=True, connect_timeout=5)
    try:
        with tconn.cursor() as cur:
            cur.execute(schema_sql)  # schema 幂等，可重复执行
    finally:
        tconn.close()
    return test_url


# ---------- 子进程管理 ----------

def _drain(proc, sink):
    """独立线程抽吸子进程管道：防缓冲写满导致子进程阻塞。"""
    try:
        for line in proc.stdout:
            sink.append(line.rstrip("\n"))
    except Exception:
        pass


def _spawn_worker(worker_id, test_url):
    """以短租约启动 worker 子进程：环境变量注入 DATABASE_URL（指向测试库）
    与 LEASE_SECONDS；进程环境变量优先于 .env（board.db.load_env setdefault 语义）。"""
    env = os.environ.copy()
    env["DATABASE_URL"] = test_url
    env["LEASE_SECONDS"] = str(LEASE_SECONDS)
    proc = subprocess.Popen(
        [sys.executable, "-m", "board.worker", "--id", worker_id],
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    sink = []
    thread = threading.Thread(target=_drain, args=(proc, sink), daemon=True)
    thread.start()
    return proc, sink, thread


def _kill(proc, name):
    """terminate → 等收线 → 兜底 kill；幂等，重复调用安全。"""
    if proc is None or proc.poll() is not None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
    except Exception:
        pass
    log(f"{name} killed")


# ---------- 演示主流程 ----------

def _cleanup_residue(conn):
    """可重复运行：先清上一次演示任务残留（按标记定位，外键顺序删）。"""
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM tasks WHERE base_params->>'demo' = %s", (DEMO_MARK,))
        old_ids = [r[0] for r in cur.fetchall()]
        for tid in old_ids:
            cur.execute("DELETE FROM step_logs WHERE task_id = %s", (tid,))
            cur.execute("DELETE FROM steps WHERE task_id = %s", (tid,))
        cur.execute("DELETE FROM tasks WHERE base_params->>'demo' = %s", (DEMO_MARK,))
    if old_ids:
        log(f"cleaned residue demo tasks: {old_ids}")


def _create_demo_task(conn):
    """创建专用 3 步任务（base_params 带标记，pending 入队）。"""
    with conn.cursor() as cur:
        # 显式 ::jsonb 转型：jsonb_build_object(VARIADIC "any") 会让 psycopg
        # 无法推断绑定参数类型（IndeterminateDatatype）
        cur.execute(
            "INSERT INTO tasks (base_params) VALUES (%s::jsonb) RETURNING id",
            (json.dumps({"demo": DEMO_MARK}, allow_nan=False),),
        )
        tid = cur.fetchone()[0]
        for si in (1, 2, 3):
            cur.execute(
                "INSERT INTO steps (task_id, step_index) VALUES (%s, %s)", (tid, si)
            )
    return tid


def _wait_for(check, timeout, desc, interval=0.05):
    """轮询 DB（而非长 sleep）直到 check() 为真或超时。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if check():
                return True
        except Exception:
            pass  # 瞬态连接抖动：继续轮询
        time.sleep(interval)
    log(f"TIMEOUT waiting for: {desc}")
    return False


def main():
    start = time.monotonic()
    verdict = "FAIL"
    w1 = w2 = None
    w1_sink = w2_sink = []
    w1_thread = w2_thread = None
    conn = None
    tid = None
    try:
        source_url = _read_source_url()
        if not source_url:
            log("FATAL: DATABASE_URL 未配置（环境变量与 .env 均无）")
            return 1
        test_url = _ensure_test_db(source_url)
        log(f"target db: {TEST_DBNAME}（演示库 taskboard 零写入）")

        # autocommit 轮询连接：每次查询独立快照，不被 worker 的写阻塞
        conn = psycopg.connect(test_url, autocommit=True, connect_timeout=5)

        _cleanup_residue(conn)
        tid = _create_demo_task(conn)
        log(f"demo task created: id={tid}, steps=1/2/3, LEASE_SECONDS={LEASE_SECONDS}")

        # —— 阶段 1：W1 认领并执行，等它提交完 step 1 后 kill（模拟僵尸） ——
        w1, w1_sink, w1_thread = _spawn_worker("W1", test_url)
        log("W1 started")

        def step1_committed():
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM step_logs WHERE task_id=%s AND step_index=1", (tid,)
                )
                return cur.fetchone() is not None

        if not _wait_for(step1_committed, STEP1_WAIT_TIMEOUT, "W1 commits step 1"):
            return 1
        log("W1 committed step 1 -> killing W1 (simulate zombie)")
        _kill(w1, "W1")

        # —— 阶段 2：W2 接管——reaper 回收过期租约 → 重认领 → 跑完 ——
        w2, w2_sink, w2_thread = _spawn_worker("W2", test_url)
        log("W2 started (waiting lease expiry -> reclaim -> rerun)")

        def task_done():
            with conn.cursor() as cur:
                cur.execute("SELECT status FROM tasks WHERE id=%s", (tid,))
                row = cur.fetchone()
                return row is not None and row[0] in ("done", "failed")

        if not _wait_for(task_done, DONE_WAIT_TIMEOUT, "task reaches terminal state"):
            return 1

        # —— 阶段 3：SQL 硬断言 ——
        with conn.cursor() as cur:
            cur.execute("SELECT status, claim_epoch FROM tasks WHERE id=%s", (tid,))
            status, epoch = cur.fetchone()
            cur.execute(
                "SELECT step_index, worker_id FROM step_logs "
                "WHERE task_id=%s ORDER BY step_index",
                (tid,),
            )
            log_rows = cur.fetchall()

        log(f"final: status={status}, claim_epoch={epoch}, step_logs={log_rows}")
        checks = [
            ("tasks.status == 'done'", status == "done"),
            ("step_logs 恰 3 行", len(log_rows) == 3),
            ("step 1 仅 1 行且 worker_id='W1'（幂等挡下 W2 重报）",
             [r for r in log_rows if r[0] == 1] == [(1, "W1")]),
            ("step 2 worker_id='W2'", (2, "W2") in log_rows),
            ("step 3 worker_id='W2'", (3, "W2") in log_rows),
            ("claim_epoch >= 2（回收后重认领，代际递增）", epoch >= 2),
        ]
        all_ok = True
        for name, ok in checks:
            log(f"  [{'OK ' if ok else 'FAIL'}] {name}")
            all_ok = all_ok and ok
        verdict = "PASS" if all_ok else "FAIL"
        log(f"elapsed: {time.monotonic() - start:.1f}s")
        return 0 if all_ok else 1
    except Exception as exc:
        log(f"EXCEPTION: {exc!r}")
        return 1
    finally:
        # 无论成败：杀掉全部子进程，不留长驻进程
        _kill(w1, "W1")
        _kill(w2, "W2")
        for thread in (w1_thread, w2_thread):
            if thread is not None:
                thread.join(timeout=2)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        # 证据落盘（覆盖旧文件）：主流程日志 + 两个 worker 的输出摘要
        EVIDENCE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with EVIDENCE_PATH.open("w", encoding="utf-8") as fh:
            fh.write("\n".join(_lines) + "\n")
            for name, sink in (("W1", w1_sink), ("W2", w2_sink)):
                fh.write(f"\n----- {name} worker output -----\n")
                fh.write("\n".join(sink) + "\n")
            fh.write(f"\nVERDICT: {verdict}\n")
        print(f"VERDICT: {verdict}")


if __name__ == "__main__":
    sys.exit(main())
