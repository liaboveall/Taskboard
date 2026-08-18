# tests/conftest.py —— 测试隔离基座（T2）：全部 DB 用例命中独立库 taskboard_test
#
# 注入时机设计（为什么用 pytest_configure 钩子而非 session fixture）：
#   board.db.database_url() 的语义是"进程环境变量优先 + 首次调用即模块级缓存"，
#   而各 DB 测试文件在【模块导入期】就调用 _db_available() → db.connect()，
#   第一次连接就会把 URL 缓存定型。pytest_configure 在收集（即测试模块导入）
#   之前执行，是"任何 board.db 缓存形成之前"的最早可靠时点——比 session 级
#   autouse fixture 更早（fixture 在首个用例实例化时才运行，收集早已完成）。
#   因此本文件不导入 board 包、不用 board.db 的任何函数（避免提前触发缓存），
#   全部用裸 psycopg 完成建库建表，再改写 os.environ["DATABASE_URL"]。
#
# 失败策略（评审修复版，不再让建库故障杀死纯逻辑用例）：
#   - TB_TEST_DB=off（大小写不敏感）→ 跳过隔离供给，退回原 skipif 语义：
#     不改写 DATABASE_URL，测试直接命中已配置的库（注意：该模式下 DB 用例
#     写的是配置指向的库，演示环境慎用）。
#   - PG 可达但 CREATE DATABASE / 建表失败 → 不再 pytest.fail 中止全会话：
#     把 DATABASE_URL 指向一个不存在的库名，各文件既有 _db_available()
#     skipif 自然触发 → DB 用例集体 skip、纯逻辑用例照跑，
#     pytest_terminal_summary 打印醒目降级警告。
#     仅当 TB_STRICT_DB=1 时才大声 pytest.fail（CI 等不容忍降级的环境）。
#   - PG 完全不可达 → 允许 skip（各文件既有 skipif 自然生效），
#     但 pytest_terminal_summary 打印醒目"假绿警告"。
#
# session 结束保留 taskboard_test（可重复执行），不 drop。
import os
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import pytest

ROOT = Path(__file__).resolve().parent.parent
TEST_DBNAME = "taskboard_test"
# 供给失败降级时指向的虚构库名：必然连不上 → 各文件 skipif 自然触发
_UNPROVISIONED_DBNAME = "taskboard_test_unprovisioned"

_TEST_URL = None          # 供给成功 = 指向 taskboard_test 的连接串
_SETUP_ERROR = None       # PG 可达但建库/建表失败的原因
_PG_NOTE = ""             # PG 不可达原因（终端摘要的假绿警告用）
_TB_TEST_DB_OFF = False   # TB_TEST_DB=off：跳过隔离供给，退回原 skipif 语义


def _read_source_url():
    """取演示库连接串：优先进程环境变量，再逐行解析 taskboard/.env。
    语义与 board.db.load_env 对齐，但刻意不调用它——
    在注入完成前绝不触碰 board 包，防止 URL 缓存提前定型。"""
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
    """保留 user/password/host/port 等全部连接参数，仅替换 dbname。"""
    parts = urlsplit(url)
    return urlunsplit(parts._replace(path="/" + dbname))


def _provision_test_db():
    """连 maintenance 库（dbname=postgres）确保 taskboard_test 存在，
    注入进程环境变量，再对测试库幂等执行 schema.sql。"""
    global _TEST_URL, _SETUP_ERROR, _PG_NOTE
    import psycopg

    source_url = _read_source_url()
    if not source_url:
        _PG_NOTE = "环境变量与 .env 均未配置 DATABASE_URL"
        return source_url

    test_url = _with_dbname(source_url, TEST_DBNAME)
    maint_url = _with_dbname(source_url, "postgres")

    try:
        # CREATE DATABASE 不能在事务块内执行 → autocommit 连接
        maint = psycopg.connect(maint_url, autocommit=True, connect_timeout=5)
    except Exception as exc:
        _PG_NOTE = f"PostgreSQL 不可达: {exc}"
        return source_url

    try:
        with maint.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (TEST_DBNAME,))
            exists = cur.fetchone() is not None
        if not exists:
            with maint.cursor() as cur:
                cur.execute(f'CREATE DATABASE "{TEST_DBNAME}"')
    except Exception as exc:
        # PG 可达却建不了库（权限等问题）：记录原因，由 pytest_configure
        # 按 TB_STRICT_DB 口径决定大声 fail 还是降级 skip
        _SETUP_ERROR = exc
        return source_url
    finally:
        maint.close()

    # 【注入点】改写进程环境变量 → 指向隔离库。
    # board.db.database_url() 进程环境变量优先且首次调用才缓存，
    # 此后任何测试文件的第一次建连必然命中 taskboard_test。
    os.environ["DATABASE_URL"] = test_url
    _TEST_URL = test_url

    # 对测试库执行 schema.sql 建表（文件本身全部 IF NOT EXISTS，幂等可重复）
    schema_sql = (ROOT / "schema.sql").read_text(encoding="utf-8")
    try:
        tconn = psycopg.connect(test_url, autocommit=True, connect_timeout=5)
    except Exception as exc:
        _SETUP_ERROR = f"taskboard_test 已建但连不上: {exc}"
        return source_url
    try:
        with tconn.cursor() as cur:
            cur.execute(schema_sql)
    except Exception as exc:
        _SETUP_ERROR = f"schema.sql 执行失败: {exc}"
    finally:
        tconn.close()
    return source_url


def pytest_configure(config):
    """在收集（测试模块导入）之前完成隔离库供给与 URL 注入。"""
    global _TB_TEST_DB_OFF
    if os.environ.get("TB_TEST_DB", "").strip().lower() == "off":
        # 显式关闭隔离供给：退回原 skipif 语义（不改写 DATABASE_URL，
        # 测试直接命中已配置的库）。纯逻辑用例不受影响。
        _TB_TEST_DB_OFF = True
        return

    source_url = _provision_test_db()
    if _SETUP_ERROR is None:
        return

    # PG 可达但建库/建表失败 ——
    # 严格模式（TB_STRICT_DB=1）：大声失败，不装绿也不降级。
    if os.environ.get("TB_STRICT_DB") == "1":
        pytest.fail(
            "测试隔离基座供给失败（PG 可达，但 taskboard_test 建库/建表失败）: "
            f"{_SETUP_ERROR}",
            pytrace=False,
        )
    # 降级模式：不中止全会话 —— 把 DATABASE_URL 指向必然不存在的库，
    # 各测试文件模块导入期的 _db_available() 自然返回 False → DB 用例
    # 集体 skip，纯逻辑用例照跑；pytest_terminal_summary 打印醒目警告。
    if source_url:
        os.environ["DATABASE_URL"] = _with_dbname(source_url, _UNPROVISIONED_DBNAME)
    else:
        # 理论上到不了这里（无 source_url 时只会置 _PG_NOTE），双保险
        os.environ["DATABASE_URL"] = (
            f"postgresql://localhost/{_UNPROVISIONED_DBNAME}"
        )


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    if _TB_TEST_DB_OFF:
        terminalreporter.write_sep("=", "测试库隔离核对")
        terminalreporter.write_line(
            "TB_TEST_DB=off：已跳过隔离供给（退回原 skipif 语义），"
            "DB 用例直接命中已配置的 DATABASE_URL —— 请自行确认未指向演示库。"
        )
    elif _TEST_URL is not None:
        terminalreporter.write_sep("=", "测试库隔离核对")
        terminalreporter.write_line(
            f"全部 DB 用例命中隔离库 {TEST_DBNAME}（演示库 taskboard 零写入）。"
        )
    elif _SETUP_ERROR is not None:
        terminalreporter.write_sep("!", "隔离供给失败 —— DB 用例已整体跳过")
        terminalreporter.write_line(
            f"PG 可达但 {TEST_DBNAME} 建库/建表失败：{_SETUP_ERROR}"
        )
        terminalreporter.write_line(
            "全部 DB 用例被跳过、仅纯逻辑用例参与本次结果 —— 当前绿色【不代表】"
            "并发正确性/幂等/围栏已验证！"
        )
        terminalreporter.write_line(
            "修复环境后重跑；需要强制大声失败请设 TB_STRICT_DB=1。"
        )
    else:
        terminalreporter.write_sep("!", "假绿警告 FALSE-GREEN WARNING")
        terminalreporter.write_line(
            f"PostgreSQL 不可达（{_PG_NOTE or '未知原因'}），全部 DB 用例被跳过。"
        )
        terminalreporter.write_line(
            "当前结果【不代表】并发正确性/幂等/围栏已验证——绿是假绿！"
        )
