# board/logconf.py —— 统一 logging 配置（零新依赖，纯 stdlib）
#
# 设计要点：
#   - setup(name) 幂等：重复调用直接返回，worker/api/watchdog 各自入口调用即可。
#   - 格式含 ISO 时间戳（asctime 本身即 ISO 8601，毫秒 %03d）/级别/模块名/
#     request_id（flask 请求上下文内为请求跟踪 id，无上下文时 "-"）。
#   - handler 挂 root，level INFO；basicConfig 自带"root 已有 handler 则跳过"
#     语义，双重保险防重复挂载。request_id Filter 挂在 handler 上（而非
#     logger）：logger 级 filter 只对直接经该 logger 打的日志生效，
#     子 logger 传播上来的记录不会被过滤，handler 级才能全覆盖。
#   - 可选 TB_LOG_FILE 环境变量：设置时额外挂 RotatingFileHandler
#     （10MB×5）追加落盘，stderr 输出保持不变（双写而非切换，
#     watchdog 重定向的子进程 stderr 链路不受影响）。
#   - 刻意不 import board 包内任何模块：本模块可能在包初始化最早期被引用，
#     只依赖标准库可杜绝导入环；flask 在 Filter 内惰性导入，
#     worker/watchdog 等非 API 进程不硬依赖 flask。
import logging
import os
import sys
from logging.handlers import RotatingFileHandler

LOG_FORMAT = ("%(asctime)s.%(msecs)03d %(levelname)s %(name)s "
              "[%(request_id)s]: %(message)s")
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"

_configured = False


class RequestIdFilter(logging.Filter):
    """给每条日志记录注入 request_id 字段。

    flask 请求上下文内取 flask.g.request_id（由 board.api 的 before_request
    生成 uuid4）；无上下文（worker 主循环/心跳/watchdog/CLI）或
    flask 不可用时一律 "-"，保证格式串 %(request_id)s 永远有值。
    flask 惰性导入：失败/缺失也回退 "-"，不让日志链路因依赖问题断掉。"""

    def filter(self, record):
        request_id = "-"
        try:
            from flask import g, has_request_context
            if has_request_context():
                request_id = getattr(g, "request_id", None) or "-"
        except Exception:
            pass  # 非 flask 进程/无请求上下文：保持 "-"
        record.request_id = request_id
        return True


def setup(name, level=logging.INFO):
    """配置进程级 logging 并返回命名 logger。幂等：重复调用不重复挂 handler。

    输出到 stderr：与子进程的 stdout 数据流分离（watchdog 重定向、
    管道消费场景下日志不污染业务输出）。TB_LOG_FILE 设置时额外
    以 RotatingFileHandler（10MB×5）追加落盘，stderr 不变。"""
    global _configured
    if not _configured:
        handlers = [logging.StreamHandler(sys.stderr)]
        log_file = os.environ.get("TB_LOG_FILE")
        if log_file:
            handlers.append(RotatingFileHandler(
                log_file, maxBytes=10 * 1024 * 1024, backupCount=5,
                encoding="utf-8",
            ))
        logging.basicConfig(
            level=level,
            format=LOG_FORMAT,
            datefmt=DATE_FORMAT,
            handlers=handlers,
        )
        # request_id 注入挂 handler：覆盖所有经 root 输出的记录
        rid_filter = RequestIdFilter()
        for handler in logging.getLogger().handlers:
            handler.addFilter(rid_filter)
        _configured = True
    return logging.getLogger(name)
