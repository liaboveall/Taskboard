# board/db.py —— 数据库连接工厂
# 设计要点：
#   1. 不引入 python-dotenv 依赖，手写极简 .env 解析（只支持 KEY=VALUE 行）。
#   2. 进程环境变量优先于 .env 文件 —— 方便测试/部署时用环境变量覆盖。
#   3. 保持 READ COMMITTED（psycopg/PG 默认），不改隔离级别；
#      并发正确性靠 FOR UPDATE SKIP LOCKED 和唯一约束，不靠提升隔离级别。
#   4. load_env 每进程只解析一次 .env（_ENV_LOADED 门闩），database_url 结果
#      模块级缓存：.env 解析是纯重复劳动，连接热路径上不该反复读文件。
import contextlib
import os
import threading
import time
from pathlib import Path

import psycopg
# psycopg_pool 惰性导入：移入 get_pool() 内部 —— worker/seed/测试直连
# （connect()）入口不再硬依赖池库，只有真正建池时才 import。

# 项目根目录（board/ 的上一级），.env 和 schema.sql 都在这里
ROOT = Path(__file__).resolve().parent.parent

# 进程内只解析一次 .env；DATABASE_URL 首次确定后缓存复用
_ENV_LOADED = False
_DATABASE_URL = None

# —— 连接韧性默认值 ——
# 仅作默认兜底；可被同名 TB_ 前缀环境变量在 connect 调用时覆盖。
# 刻意带 TB_ 前缀：与业务配置键（如 LEASE_SECONDS/PORT）隔离，避免命名冲突。
CONNECT_TIMEOUT = 5            # 建连超时（秒），DB 黑洞时不无限挂起
LOCK_TIMEOUT_MS = 5000         # 等行锁超时（毫秒），行锁等待有上限
STATEMENT_TIMEOUT_MS = 30000   # 单语句执行超时（毫秒）

# 建连重试策略：仅覆盖“连接建立”阶段（见 connect 内注释）
_MAX_RETRIES = 2               # 首次 + 最多 2 次重试 = 共 3 次尝试
_RETRY_BACKOFF = (0.2, 0.5)    # 每次重试前的退避秒数（与尝试序号对应）

# —— 连接池默认值（API 读路径/上报路径专用；worker/seed/测试仍走 connect()）——
POOL_MIN_SIZE = 1              # 空闲保底连接数
POOL_MAX_SIZE = 8              # 并发上限：看板轮询 + 手动上报量级足够
POOL_TIMEOUT = 5.0             # getconn 排队等待上限（秒）
POOL_MAX_LIFETIME = 1800.0     # 连接最长寿命（秒），到期换新建连


def load_env(path=None):
    """极简 .env 解析：逐行读 KEY=VALUE，忽略空行与 # 注释，
    已存在于 os.environ 的 key 不覆盖（进程环境变量优先）。
    _ENV_LOADED 保证每进程只解析一次，重复调用直接返回。"""
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    if path is None:
        path = ROOT / ".env"
    path = Path(path)
    if not path.exists():
        _ENV_LOADED = True
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)  # 不覆盖已有环境变量
    _ENV_LOADED = True


def database_url():
    """取 DATABASE_URL：先看进程环境变量，再兜底读 .env。
    结果模块级缓存（_DATABASE_URL）：运行期改 .env 不会被拾起，
    这是有意取舍——配置应在进程启动前确定，热路径零文件 IO。"""
    global _DATABASE_URL
    if _DATABASE_URL is not None:
        return _DATABASE_URL
    load_env()
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL not set (neither env var nor .env)")
    _DATABASE_URL = url
    return url


def to_int(value, default):
    """容错转 int：空值/非法值回退 default。
    为什么：超时是韧性配置，不能因一条脏环境变量把建连整体炸掉。"""
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


# 内部别名：公开名 to_int 前的存量引用保持不破坏
_to_int = to_int


def env_int(key, default):
    """容错读取整型环境变量。
    日志分级口径：变量缺失是常态（默认值存在的意义），降为 DEBUG
    （默认级别下静默）；仅"变量存在但非法"才打 WARNING（显式告警，
    避免脏配置被静默吞掉）。合法值原样返回。
    logconf 惰性导入：本模块在包初始化最早期被导入，避免任何导入环风险。"""
    import logging

    raw = os.environ.get(key)
    if raw is None or raw == "":
        logging.debug("env %s missing, falling back to default %s", key, default)
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        logging.warning("env %s=%r is not an integer, falling back to default %s",
                        key, raw, default)
        return default


def _effective_timeouts():
    """在 connect 调用时读取环境变量覆盖默认超时。
    为什么刻意不在模块导入时读取：避免重蹈时序倒挂——
    模块导入时 .env 可能尚未加载，os.environ 拿不到配置；
    调用时读取才能保证环境变量/运行时覆盖始终生效。"""
    connect_timeout = to_int(os.environ.get("TB_CONNECT_TIMEOUT"), CONNECT_TIMEOUT)
    lock_timeout = to_int(os.environ.get("TB_LOCK_TIMEOUT_MS"), LOCK_TIMEOUT_MS)
    statement_timeout = to_int(
        os.environ.get("TB_STATEMENT_TIMEOUT_MS"), STATEMENT_TIMEOUT_MS
    )
    return connect_timeout, lock_timeout, statement_timeout


def connect():
    """新建一条 psycopg 连接（每次调用都是新连接，绝不跨进程/线程共享）。
    autocommit=False 是默认值：短事务里执行完一条语句就立即 commit。

    韧性加固（签名与返回语义保持不变）：
      - connect_timeout 限制建连耗时，DB 黑洞时不无限挂起；
      - lock_timeout / statement_timeout 经 conninfo 的 options 一次性下发，
        行锁等待与单语句执行都有上限，免每连接额外 SET 往返；
      - 重试仅覆盖“连接建立”阶段：只捕获 psycopg.OperationalError，
        最多重试 2 次、退避 0.2s/0.5s，耗尽后原样抛出最后一次异常。
        语句执行类错误（约束/语法等）不在此处，不重试、直接向上抛。"""
    connect_timeout, lock_timeout, statement_timeout = _effective_timeouts()
    options = f"-c lock_timeout={lock_timeout} -c statement_timeout={statement_timeout}"
    last_exc = None
    for attempt in range(_MAX_RETRIES + 1):
        try:
            return psycopg.connect(
                database_url(),
                connect_timeout=connect_timeout,
                options=options,
            )
        except psycopg.OperationalError as exc:
            # 仅“建连失败”才进入重试；重试耗尽后原样抛出最后一次异常
            last_exc = exc
            if attempt < _MAX_RETRIES:
                time.sleep(_RETRY_BACKOFF[attempt])
    raise last_exc


# ---------- 连接池（API 层专用） ----------
# 为什么懒初始化而不是模块导入时建池：
#   1. database_url() 首次调用才缓存，测试由 conftest.pytest_configure 在
#      收集前改写 DATABASE_URL 指向隔离库——导入期建池会把池焊死在错误库上；
#   2. worker/seed 进程根本不用池，懒初始化避免给它们白养一池连接。
_POOL = None
_POOL_LOCK = threading.Lock()


def get_pool():
    """懒初始化并返回进程级唯一 psycopg_pool.ConnectionPool（双重检查加锁）。

    min/max 可用 TB_POOL_MIN/TB_POOL_MAX 环境变量覆盖（env_int 容错解析）。
    超时韧性（评审修复）：池内连接与 connect() 同口径下发 lock_timeout /
    statement_timeout —— 复用 _effective_timeouts() 的同一组常量与
    TB_ 覆盖口径。options 走 kwargs 下发：psycopg_pool 的 conninfo 直传
    libpq，URI 与 key=value 两种 conninfo 形式不可混写，而 kwargs 会
    逐字转发给 psycopg.connect（与 connect() 的 options= 参数等价）。
    reset 用池默认行为（reset=None）：归还时自动 rollback 未提交事务并复位
    会话级参数（SET 过的隔离级别/search_path 等一并还原），配合 acquire()
    异常路径的显式 rollback，保证池内连接复用前状态干净。
    open=True 显式开池，规避 3.2+ 的弃用警告与隐式语义。"""
    import psycopg_pool  # 惰性导入：无池入口（worker/seed）不硬依赖本库

    global _POOL
    if _POOL is None:
        with _POOL_LOCK:
            if _POOL is None:
                # 与 connect() 同一组韧性超时（调用时读取，支持 TB_ 覆盖）
                _connect_timeout, lock_timeout, statement_timeout = _effective_timeouts()
                options = (f"-c lock_timeout={lock_timeout} "
                           f"-c statement_timeout={statement_timeout}")
                _POOL = psycopg_pool.ConnectionPool(
                    conninfo=database_url(),
                    min_size=env_int("TB_POOL_MIN", POOL_MIN_SIZE),
                    max_size=env_int("TB_POOL_MAX", POOL_MAX_SIZE),
                    timeout=POOL_TIMEOUT,
                    max_lifetime=POOL_MAX_LIFETIME,
                    open=True,
                    kwargs={"options": options},
                )
    return _POOL


@contextlib.contextmanager
def acquire():
    """从池取一条连接的上下文管理器。

    语义契约（与 report() 的行锁时序强相关，不可擅改）：
      - 正常路径【不】commit 也不 rollback——FOR UPDATE 行锁必须持有到
        调用方（如 logs.report_step）内部 commit 才释放，提前结束事务
        会把“读状态时合法、写入前被夺权”的窗口重新敞开；
      - 异常路径显式 rollback，避免把报错事务带回池；
      - finally 归还：池默认 reset 负责 rollback 残余事务与复位会话参数。"""
    pool = get_pool()
    conn = pool.getconn()
    try:
        yield conn
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass  # 连接可能已断；归还时池的 check/reset 会自行处置
        raise
    finally:
        pool.putconn(conn)


def close_pool():
    """关闭并丢弃进程级连接池（进程退出/测试收尾用）；未建池时静默无操作。"""
    global _POOL
    with _POOL_LOCK:
        if _POOL is not None:
            _POOL.close()
            _POOL = None
