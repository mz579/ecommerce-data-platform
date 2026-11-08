"""电商数仓 Web UI 只读后端（FastAPI）。

入口：``app.main:app``。全部接口位于 ``/api`` 前缀下，前端静态资源由同一进程
托管（SPA fallback）。连接账号为只读的 ``webapp_ro``，后端代码不包含任何写库
语句。
"""

__version__ = "1.0.0"
