# tests/test_watchdog.py —— watchdog 崩溃退避纯函数边界用例（无需数据库）
#
# backoff_for 口径（与 watchdog.py 头注释一致）：
#   0 次崩溃不退避（返回 0）；之后按 BACKOFF_STEPS=[3,6,12,30] 指数退避，
#   封顶 30s。本文件只锁纯函数边界，不拉起任何子进程。
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import watchdog  # noqa: E402  模块导入仅配置 logging 与读环境变量，无子进程副作用


def test_no_crash_means_no_backoff():
    # 首次（尚无崩溃）不退避；负值脏输入同口径兜底为 0
    assert watchdog.backoff_for(0) == 0
    assert watchdog.backoff_for(-1) == 0


def test_backoff_steps_increase():
    # 连续崩溃 1..4 次：3s -> 6s -> 12s -> 30s 指数递增
    assert watchdog.backoff_for(1) == 3
    assert watchdog.backoff_for(2) == 6
    assert watchdog.backoff_for(3) == 12
    assert watchdog.backoff_for(4) == 30


def test_backoff_capped_at_last_step():
    # 封顶：超过退避阶梯长度后不再增长（防配置性故障触发重启风暴，
    # 也绝不无限拉长等待）
    assert watchdog.backoff_for(5) == 30
    assert watchdog.backoff_for(1000) == 30
    assert watchdog.backoff_for(1000) == watchdog.BACKOFF_STEPS[-1]
