# board/__init__.py —— 包初始化：把配置加载提前到"包导入"时刻
# 为什么：worker 等子模块在模块导入时就读取 os.environ（如 LEASE_SECONDS），
# 若仍靠 database_url() 惰性触发 load_env，.env 里的配置永远赶不上模块级读取
# （时序倒挂）。这里在包导入即加载 .env，确保子模块模块级配置读取晚于配置加载。
# 无循环导入风险：db.py 只依赖标准库与 psycopg，不反向依赖 board 包。
from board import db

db.load_env()
