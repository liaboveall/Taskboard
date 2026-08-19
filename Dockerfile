# Dockerfile —— taskboard 运行镜像（python:3.12-slim，单层非 root）
#
# 层序取舍：先 COPY requirements*.txt + pip install（依赖层可缓存），
# 再 COPY 代码 —— 改代码不触发依赖重装。
#
# 刻意取舍（笔试场景说明）：运行镜像【连 dev 依赖一起装】
# （pytest/pytest-cov/ruff，见 requirements-dev.txt）。原因是 compose 的
# testrunner 服务复用同一镜像在容器内跑全量测试（含 slow 攻击用例与覆盖率），
# 若镜像不带 pytest-cov 就得在 testrunner entrypoint 里现场 pip install
# （每次 run 都走网络、失败面更大）。代价是运行镜像多 ~10MB 测试工具链，
# 演示/笔试规模下可接受；生产化时应拆 builder 阶段只留运行依赖。
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# 依赖层：运行依赖 + dev 依赖（取舍见文件头注释）
COPY requirements.txt requirements-dev.txt ./
RUN pip install -r requirements.txt -r requirements-dev.txt

# 代码层：board/ 包、schema.sql、static/、scripts/、tests/ 一并入镜像。
# .dockerignore 已排除 .venv/.git/.env/evidence/ 等，宿主机 .env 绝不入镜像
# （容器内数据库配置一律由 compose 注入的环境变量提供）。
COPY . .

# 非 root 运行：useradd 建专用用户并移交 /app 属主
RUN useradd --create-home appuser \
    && chown -R appuser:appuser /app
USER appuser

EXPOSE 5000

# 缺省入口是 API；compose 各服务会覆写 command（seed/worker/testrunner）
CMD ["python", "-m", "board.api"]
