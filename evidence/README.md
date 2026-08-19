# evidence/ —— 交付证据目录

- `claim_attack_run.log`：多进程认领攻击实测日志（唯一入库的证据文件）。
  生成命令：`DATABASE_URL=<临时隔离库，如 taskboard_evidence> python scripts/attack_claim.py --truncate-ok`
  口径：10 workers × 10 rounds × 100 tasks + claim×reaper 组合轮 + report_step 洪泛轮×3；
  末行 `result=PASS, duplicate_claims=0`。生成日期：2026-08-19。
- 其余运行时产物（`evidence/_*`、`evidence/*.log` 其他文件、`evidence/*.png`）由 .gitignore 排除，不入库。
