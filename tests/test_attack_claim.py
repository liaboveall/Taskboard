# tests/test_attack_claim.py —— 多进程认领攻击脚本的 pytest 薄封装
#
# @pytest.mark.slow：10 进程 × 10 轮 × 100 任务 + reaper/report 组合轮，
# 分钟级耗时。pyproject addopts 默认 `-m "not slow"` 将其排除——
# 仅 CI 全量（显式 -m ""）或本地显式 `-m slow` 时执行。
# needs_db 门控与现有用例同风格：无库环境 skip 而非 fail。
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from board import db  # noqa: E402


def _db_available():
    """DB 不在（无 DATABASE_URL/.env 或服务未启动）时返回 False，
    让本模块 skip 而非 error，保证套件在无库环境可移植。"""
    try:
        c = db.connect()
    except Exception:
        return False
    c.close()
    return True


pytestmark = [
    pytest.mark.skipif(
        not _db_available(), reason="PostgreSQL not reachable (set DATABASE_URL or .env)"
    ),
    pytest.mark.slow,
]


def test_attack_claim_passes_end_to_end(tmp_path):
    """subprocess 调 scripts/attack_claim.py（独立解释器进程，复刻真实运行
    方式）：显式传 --truncate-ok 确认 TRUNCATE 护栏，--out 指向临时目录
    避免污染 evidence/。断言退出码 0 且 stdout 末行含 result=PASS。"""
    script = ROOT / "scripts" / "attack_claim.py"
    assert script.exists(), "scripts/attack_claim.py 不存在"
    proc = subprocess.run(
        [sys.executable, str(script),
         "--truncate-ok",
         "--out", str(tmp_path / "claim_attack_run.log")],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=900,  # 分钟级攻击轮，给足上限
    )
    stdout_lines = [ln for ln in proc.stdout.splitlines() if ln.strip()]
    last = stdout_lines[-1] if stdout_lines else ""
    assert proc.returncode == 0, (
        f"attack_claim 退出码 {proc.returncode}\n"
        f"--- stdout tail ---\n{''.join(stdout_lines[-10:])}\n"
        f"--- stderr tail ---\n{proc.stderr[-2000:]}"
    )
    assert "result=PASS" in last, f"stdout 末行未见 result=PASS: {last!r}"
