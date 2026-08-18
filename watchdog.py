# watchdog.py —— 演示环境守护进程（非常驻交付代码，仅为对抗环境收割）
#
# 职责边界：watchdog 只负责进程存活（子进程退出即按策略重拉），
# 不负责任何业务状态。被收割 worker 手里未完成任务的回收，由 worker 主循环
# 的租约 reaper（claim.reclaim_expired）负责——两者职责不重叠。
#
# 背景：本环境的后台进程会被周期性"收割"（evidence/api.err 中无任何崩溃栈，
# 进程最后一条日志还是 200 响应，随后直接消失——是外部 kill，不是代码 bug）。
# 因此用一个守护进程自愈：定期检查，子进程退出即重新拉起
# board.api / worker W1 / worker W2。守护进程本身建议用 WMI
# (Win32_Process.Create) 启动，使其脱离 shell 进程树，存活时间最长。
#
# 重启策略：
#   - 子进程 returncode==0 视为优雅退出：不重拉、不计崩溃退避，
#     该 job 置为停止态（后续循环不再拉起）。
#   - 非 0 退出走崩溃退避：每 job 维护连续失败计数——进程存活 <10 秒即退出
#     视为崩溃，重启前等待按 3s -> 6s -> 12s -> 30s 指数退避（封顶 30s）；
#     进程稳定存活 >30 秒后计数清零。避免配置性故障（如数据库不可用）
#     触发无限快速重启风暴。
#   - api 端口被外部占用（端口在监但非本 watchdog 所拉起）：不再静默跳过，
#     打印显式告警（含连续计数；每 PORT_BUSY_WARN_EVERY 次节流一次防刷屏）。
#
# 输出重定向：子进程 stdout/stderr 以 append 模式写入
# evidence/watchdog_<job>_<pid>.log（<pid> 为 watchdog 自身进程号：
# 防多实例守护同时运行时子进程日志交错写入同一文件；与既有
# api.log/w1.log 不冲突）。
#
# 优雅停机：注册 SIGTERM/SIGINT 处理——先 terminate 全部子进程并等待退出，
# 再退出 watchdog 本体（Windows 上 signal handler 由 Python 主线程投递，
# subprocess.terminate 等价于 TerminateProcess，语义可用）。
#
# Python 解释器：默认 .venv\Scripts\python.exe，可用环境变量 WATCHDOG_PYTHON 覆盖。
#
# 用法: .venv\Scripts\python.exe watchdog.py
import os
import signal
import socket
import subprocess
import time

from board import logconf

logger = logconf.setup("watchdog")

ROOT = os.path.dirname(os.path.abspath(__file__))
PY = os.environ.get("WATCHDOG_PYTHON") or os.path.join(ROOT, ".venv", "Scripts", "python.exe")
API_PORT = int(os.environ.get("PORT", "5000"))
JOBS = [
    [PY, "-m", "board.api"],
    [PY, "-m", "board.worker", "--id", "W1"],
    [PY, "-m", "board.worker", "--id", "W2"],
]
JOB_NAMES = ["api", "worker-W1", "worker-W2"]  # 与 JOBS 一一对应，用于日志文件名与告警

CRASH_WINDOW = 10.0            # 存活不足该秒数即退出 -> 记一次连续崩溃
STABLE_WINDOW = 30.0           # 稳定存活超过该秒数 -> 连续崩溃计数清零
BACKOFF_STEPS = [3, 6, 12, 30]  # 崩溃后重启等待（秒），按次数指数退避，封顶 30s
PORT_BUSY_WARN_EVERY = 10      # 端口被外部占用的告警节流：每 N 次循环打一条


def port_listening(port):
    """127.0.0.1:port 是否已有服务在监（用于避免 api 重启风暴）。"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", port)) == 0


def backoff_for(crashes):
    """连续崩溃 crashes 次后的重启等待秒数（至少 1 次崩溃才退避）。"""
    if crashes <= 0:
        return 0
    return BACKOFF_STEPS[min(crashes - 1, len(BACKOFF_STEPS) - 1)]


def open_job_log(job_name):
    """以 append 模式打开子进程输出日志 evidence/watchdog_<job>_<pid>.log
    （<pid> 为本 watchdog 进程号：多实例守护并存时各自独占文件，
    防子进程日志交错）。stdout 与 stderr 合流到同一文件
    （stderr 承接 logging 输出）。打开失败不致命：回退 DEVNULL，
    守护职责不因日志落盘问题中断。"""
    try:
        log_path = os.path.join(ROOT, "evidence",
                                f"watchdog_{job_name}_{os.getpid()}.log")
        return open(log_path, "ab", buffering=0)
    except OSError as exc:
        logger.warning("job %s: open log failed (%s), redirecting to DEVNULL", job_name, exc)
        return subprocess.DEVNULL


def main():
    # 每 job 状态：子进程句柄、启动时刻（monotonic）、连续崩溃计数、最早可重启时刻、
    # stopped（优雅退出后的停止态，不再拉起）、port_busy（端口占用连续计数）
    state = [
        {"proc": None, "started_at": 0.0, "crashes": 0, "next_start": 0.0,
         "stopped": False, "port_busy": 0}
        for _ in JOBS
    ]

    # —— 优雅停机：先 terminate 全部子进程、等待退出，再退出 watchdog 本体 ——
    # Windows 上 handler 由 Ctrl+C / TerminateProcess 事件经 Python 主线程投递，
    # 此时不处于阻塞调用中，直接在此收尾即可；handler 内只做置旗标这类轻量动作，
    # 实际收尾在主循环检测旗标后执行，避免在信号上下文里做重活。
    shutdown_requested = []

    def request_shutdown(signum, _frame):
        if not shutdown_requested:  # 重复信号不重复打印
            logger.info("received signal %s, stopping children then exiting", signum)
        shutdown_requested.append(signum)

    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)

    def stop_all_children():
        for i, st in enumerate(state):
            p = st["proc"]
            if p is not None and p.poll() is None:
                logger.info("terminating job %s ...", JOB_NAMES[i])
                try:
                    p.terminate()
                except OSError:
                    pass
        for i, st in enumerate(state):
            p = st["proc"]
            if p is not None and p.poll() is None:
                try:
                    p.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    logger.warning("job %s did not exit in 10s, killing", JOB_NAMES[i])
                    try:
                        p.kill()
                        p.wait(timeout=5)
                    except Exception:
                        pass
            st["proc"] = None

    while True:
        if shutdown_requested:
            stop_all_children()
            logger.info("all children stopped, watchdog exiting")
            return 0
        now = time.monotonic()
        for i, cmd in enumerate(JOBS):
            st = state[i]
            if st["stopped"]:
                continue  # 优雅退出过的 job：停止态，不再拉起
            p = st["proc"]
            if p is not None:
                rc = p.poll()
                if rc is None:
                    # 进程仍在运行：稳定存活超过阈值即清零连续崩溃计数
                    if now - st["started_at"] >= STABLE_WINDOW:
                        st["crashes"] = 0
                    continue
                # poll() 非 None → 子进程已退出（优雅退出/被收割/崩溃）
                alive = now - st["started_at"]
                st["proc"] = None
                if rc == 0:
                    # 优雅退出：不重拉、不计崩溃退避，置停止态
                    st["stopped"] = True
                    logger.info("job %s 优雅退出（存活 %.1fs，rc=0），停止该 job，不再拉起",
                                JOB_NAMES[i], alive)
                    continue
                if alive < CRASH_WINDOW:
                    st["crashes"] += 1
                    delay = backoff_for(st["crashes"])
                    st["next_start"] = now + delay
                    logger.warning("job %s 存活 %.1fs 即退出（rc=%s，连续崩溃 #%s），%ss 后重启",
                                   JOB_NAMES[i], alive, rc, st["crashes"], delay)
                else:
                    # 存活较久后非 0 退出（如被外部收割）：不视为崩溃，立即重拉
                    st["crashes"] = 0
                    st["next_start"] = now
                    logger.warning("job %s 退出（rc=%s，存活 %.1fs），立即重启",
                                   JOB_NAMES[i], rc, alive)
            # 需要（重新）拉起：遵守退避时刻；api 端口已被占用时跳过，
            # 否则会陷入"绑定失败→退出→再拉起"的无限重启风暴。
            if st["proc"] is None and now >= st["next_start"]:
                if i == 0 and port_listening(API_PORT):
                    # 端口在监但本 watchdog 未持有该进程：显式告警（节流防刷屏）
                    st["port_busy"] += 1
                    if st["port_busy"] == 1 or st["port_busy"] % PORT_BUSY_WARN_EVERY == 0:
                        logger.warning(
                            "api 端口 %s 已被外部进程占用（非本 watchdog 拉起），"
                            "跳过启动（已连续 %s 次检测到；请停掉外部占用者或换 PORT）",
                            API_PORT, st["port_busy"])
                    continue
                st["port_busy"] = 0
                log_file = open_job_log(JOB_NAMES[i])
                try:
                    st["proc"] = subprocess.Popen(
                        cmd, cwd=ROOT,
                        stdout=log_file,
                        stderr=subprocess.STDOUT,
                    )
                except Exception as exc:
                    # Popen 失败（句柄耗尽/路径失效/系统资源枯竭等）不拖垮
                    # 守护本体：按既有崩溃退避口径记 crashes 与 next_start
                    # 后 continue 照看其它 job；失败路径关闭已打开的日志句柄
                    # （Popen 未接管 fd，不关即泄漏）。
                    if log_file is not subprocess.DEVNULL:
                        try:
                            log_file.close()
                        except OSError:
                            pass
                    st["crashes"] += 1
                    delay = backoff_for(st["crashes"])
                    st["next_start"] = time.monotonic() + delay
                    logger.error(
                        "job %s Popen 失败: %s；按崩溃退避 %ss 后重试（连续 #%s）",
                        JOB_NAMES[i], exc, delay, st["crashes"])
                    continue
                # Popen 已把 fd 复制给子进程：watchdog 侧句柄及时关闭，防泄漏
                if log_file is not subprocess.DEVNULL:
                    log_file.close()
                st["started_at"] = time.monotonic()
                logger.info("job %s started (pid=%s)", JOB_NAMES[i], st["proc"].pid)
        time.sleep(1)


if __name__ == "__main__":
    raise SystemExit(main())
