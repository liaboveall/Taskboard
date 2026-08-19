# tests/test_seed.py —— seed 三路径用例：空库播种 / 非空跳过 / --force 重建
# （对真实 PostgreSQL，走 conftest 隔离库 taskboard_test）
#
# 场景自治口径：每个用例自行构造初始态（DELETE 清空全部数据行——
# 不 DROP 表，schema 保持就绪）再调 seed.main()，用例间互不依赖。
# seed.main 经 argparse 读 sys.argv，用 monkeypatch 注入；--force 免交互
# （board/seed.py argparse 既有语义），全程无需注入 input。
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from board import db, seed


def _db_available():
    """DB 不在（无 DATABASE_URL/.env 或服务未启动）时返回 False，
    让本模块 skip 而非 error，保证套件在无库环境可移植。"""
    try:
        c = db.connect()
    except Exception:
        return False
    c.close()
    return True


pytestmark = pytest.mark.skipif(
    not _db_available(), reason="PostgreSQL not reachable (set DATABASE_URL or .env)"
)


@pytest.fixture()
def conn():
    c = db.connect()
    yield c
    c.close()


@pytest.fixture()
def clean_db(conn):
    """清空全部数据行（按外键顺序 DELETE，不 DROP 表），
    用例结束后再清一次，避免演示数据残留影响其它用例的计数断言。"""
    def _wipe():
        with conn.cursor() as cur:
            cur.execute("DELETE FROM step_logs")
            cur.execute("DELETE FROM steps")
            cur.execute("DELETE FROM tasks")
            cur.execute("DELETE FROM task_groups")
        conn.commit()

    _wipe()
    yield conn
    _wipe()


def _task_count(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM tasks")
        return cur.fetchone()[0]


def _insert_sentinel(conn):
    """插入一个带标记的哨兵任务：用于构造非空库 / 验证 --force 重建后旧数据消失。"""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO tasks (base_params) "
            "VALUES ('{\"sentinel\": \"seed-test\"}'::jsonb)"
        )
    conn.commit()


def _sentinel_count(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM tasks "
            "WHERE base_params->>'sentinel' = 'seed-test'"
        )
        return cur.fetchone()[0]


# ---------- ① 空库播种 ----------

def test_seed_empty_db_seeds(clean_db, monkeypatch, capsys):
    """tasks 为空 → 默认路径执行 schema + 播种：打印 seeded 摘要，
    演示数据落库（1 group × 3 tasks × 3 steps）。"""
    monkeypatch.setattr(sys, "argv", ["board.seed"])
    seed.main()
    out = capsys.readouterr().out
    assert "seeded:" in out
    assert _task_count(clean_db) == 3


# ---------- ② 非空跳过 ----------

def test_seed_nonempty_skips(clean_db, monkeypatch, capsys):
    """tasks 非空 → 默认非破坏路径：打印 skip 摘要，任务数纹丝不动。"""
    _insert_sentinel(clean_db)
    assert _task_count(clean_db) == 1

    monkeypatch.setattr(sys, "argv", ["board.seed"])
    seed.main()
    out = capsys.readouterr().out
    assert "tasks not empty, seeding skipped" in out
    assert _task_count(clean_db) == 1          # 未播种也未破坏
    assert _sentinel_count(clean_db) == 1


# ---------- ③ --force 破坏性重建（免交互） ----------

def test_seed_force_rebuilds(clean_db, monkeypatch, capsys):
    """--force：跳过交互确认，DROP 四表 → schema → 播种；
    存量数据（哨兵任务）消失，重新播种出 3 个演示任务。"""
    _insert_sentinel(clean_db)

    monkeypatch.setattr(sys, "argv", ["board.seed", "--force"])
    seed.main()
    out = capsys.readouterr().out
    assert "seeded:" in out
    assert _sentinel_count(clean_db) == 0      # 旧数据已被重建清掉
    assert _task_count(clean_db) == 3
