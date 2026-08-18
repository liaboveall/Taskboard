# tests/test_db.py —— board/db.py 的 load_env 与 database_url 单测
#
# 隔离要点（防污染其它 DB 用例的连接目标）：
#   autouse fixture 在每个用例前后保存/还原 db._ENV_LOADED、db._DATABASE_URL
#   与 os.environ 的 DATABASE_URL；用例内的临时键一律走 monkeypatch，
#   由 monkeypatch 在用例结束时自动还原。
import os

import pytest

from board import db

# 用例专用的环境变量键名（加 TBTEST_ 前缀，避免撞上真实配置）
_KEY_A = "TBTEST_A"
_KEY_B = "TBTEST_B"
_KEY_C = "TBTEST_C"
_KEY_PRIO = "TBTEST_PRIO"


@pytest.fixture(autouse=True)
def isolate_db_state(monkeypatch):
    """记录并锁定 db 模块的可变状态与 DATABASE_URL，用例结束后自动还原。

    monkeypatch 在 setenv/delenv/setattr 时记录【调用时刻】的原值，
    用例结束时还原回去——即使本用例把状态改得面目全非，
    后续 DB 用例看到的仍是进入本用例前的原始状态。
    """
    monkeypatch.setattr(db, "_ENV_LOADED", db._ENV_LOADED)
    monkeypatch.setattr(db, "_DATABASE_URL", db._DATABASE_URL)
    if os.environ.get("DATABASE_URL") is None:
        monkeypatch.delenv("DATABASE_URL", raising=False)
    else:
        monkeypatch.setenv("DATABASE_URL", os.environ["DATABASE_URL"])
    # 预清理用例专用键，保证起点干净（用例内再按需 setenv）
    for key in (_KEY_A, _KEY_B, _KEY_C, _KEY_PRIO):
        monkeypatch.delenv(key, raising=False)


def _write_env(tmp_path, content):
    p = tmp_path / ".env"
    p.write_text(content, encoding="utf-8")
    return p


# ---------- load_env：基础解析 ----------
def test_load_env_parses_kv_and_skips_noise(tmp_path):
    """KEY=VALUE 正常解析；注释行、空行、无 = 行跳过；引号剥离。"""
    env_file = _write_env(tmp_path, "\n".join([
        "# 这是注释行，应被跳过",
        "",
        f"{_KEY_A}=plain-value",
        "没有等号的行也应跳过",
        f'{_KEY_B}="double quoted"',
        f"{_KEY_C}='single quoted'",
    ]))
    db._ENV_LOADED = False

    db.load_env(path=env_file)

    assert os.environ[_KEY_A] == "plain-value"
    assert os.environ[_KEY_B] == "double quoted"   # 双引号被剥离
    assert os.environ[_KEY_C] == "single quoted"   # 单引号被剥离
    assert db._ENV_LOADED is True                  # 解析后上锁


# ---------- load_env：进程环境变量优先 ----------
def test_load_env_does_not_override_existing_env(tmp_path):
    """setdefault 语义：os.environ 已有的值不被 .env 覆盖。"""
    os.environ[_KEY_PRIO] = "from-os-environ"
    env_file = _write_env(tmp_path, f"{_KEY_PRIO}=from-dotenv\n")
    db._ENV_LOADED = False

    db.load_env(path=env_file)

    assert os.environ[_KEY_PRIO] == "from-os-environ"


def test_load_env_latch_prevents_second_parse(tmp_path):
    """_ENV_LOADED 门闩：已加载过则直接返回，不再读文件。"""
    env_file = _write_env(tmp_path, f"{_KEY_A}=first\n")
    db._ENV_LOADED = False
    db.load_env(path=env_file)
    assert os.environ[_KEY_A] == "first"

    # 改写文件内容后再次调用：门闩已闭，新值不应被拾起
    _write_env(tmp_path, f"{_KEY_A}=second\n")
    db.load_env(path=env_file)
    assert os.environ[_KEY_A] == "first"


# ---------- load_env：.env 缺失兜底 ----------
def test_load_env_missing_file_is_noop(tmp_path):
    """文件不存在时静默返回不报错，并把门闩置位（避免反复探测磁盘）。"""
    db._ENV_LOADED = False

    db.load_env(path=tmp_path / "no-such-.env")   # 不应抛异常

    assert db._ENV_LOADED is True


# ---------- database_url：缓存语义 ----------
def test_database_url_caches_first_result(monkeypatch):
    """首次取值后模块级缓存：之后改环境变量不再被拾起（热路径零文件 IO）。"""
    monkeypatch.setattr(db, "_ENV_LOADED", True)   # 不去读真实 .env
    monkeypatch.setenv("DATABASE_URL", "postgresql://cache-test/first")
    db._DATABASE_URL = None

    assert db.database_url() == "postgresql://cache-test/first"

    # 运行期改动环境变量：缓存命中，结果不变
    monkeypatch.setenv("DATABASE_URL", "postgresql://cache-test/second")
    assert db.database_url() == "postgresql://cache-test/first"
    assert db._DATABASE_URL == "postgresql://cache-test/first"


def test_database_url_prefers_process_env(monkeypatch):
    """进程环境变量优先：DATABASE_URL 已在 os.environ 时直接采用，
    不被 load_env 从 .env 读到的任何同名键干扰（setdefault 语义的另一面）。"""
    monkeypatch.setattr(db, "_ENV_LOADED", True)   # 不去读真实 .env
    monkeypatch.setenv("DATABASE_URL", "postgresql://from-os-env/x")
    db._DATABASE_URL = None

    assert db.database_url() == "postgresql://from-os-env/x"


# ---------- database_url：未设置时抛错 ----------
def test_database_url_raises_when_unset(monkeypatch):
    """环境变量与 .env 都没有 DATABASE_URL 时显式抛 RuntimeError。"""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setattr(db, "_ENV_LOADED", True)   # 挡住 load_env 读真实 .env
    db._DATABASE_URL = None

    with pytest.raises(RuntimeError, match="DATABASE_URL not set"):
        db.database_url()
