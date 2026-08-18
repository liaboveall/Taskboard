# board/logconf.py —— 统一 logging 配置（零新依赖，纯 stdlib）
#
# 设计要点：
#   - setup(name) 幂等：重复调用直接返回，worker/api/watchdog 各自入口调用即可。
#   - 格式含 ISO 时间戳（asctime 本身即 ISO 8601，毫秒 %03d）/级别/模块名。
#   - handler 挂 root，level INFO；basicConfig 自带"root 已有 handler 则跳过"
#     语义，双重保险防重复挂载。
#   - 刻意不 import board 包内任何模块：本模块可能在包初始化最早期被引用，
#     只依赖标准库可杜绝导入环。
import logging
import sys

LOG_FORMAT = "%(asctime)s.%(msecs)03d %(levelname)s %(name)s: %(message)s"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"

_configured = False


def setup(name, level=logging.INFO):
    """配置进程级 logging 并返回命名 logger。幂等：重复调用不重复挂 handler。

    输出到 stderr：与子进程的 stdout 数据流分离（watchdog 重定向、
    管道消费场景下日志不污染业务输出）。"""
    global _configured
    if not _configured:
        logging.basicConfig(
            level=level,
            format=LOG_FORMAT,
            datefmt=DATE_FORMAT,
            stream=sys.stderr,
        )
        _configured = True
    return logging.getLogger(name)
