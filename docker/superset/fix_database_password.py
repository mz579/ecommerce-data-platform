"""Fix the read-only MySQL connection password in superset_meta (Phase 7).

Superset's dashboard/database export masks the SQLAlchemy URI password, so a
fresh `superset import-dashboards` lands with a dead connection. This script
rebuilds the connection with the real credentials from the environment
(docker-compose injects .env values) using the model's ``set_sqlalchemy_uri``:
the real password goes into the Fernet-encrypted ``dbs.password`` column and
the ``dbs.sqlalchemy_uri`` column only ever holds a masked URI (``XXXXX``),
so no plaintext password is at rest in the metadata DB.

Run from inside the apache/superset:4.1.2 image after `superset db upgrade`:

    python3 /app/scripts/fix_database_password.py
"""

from __future__ import annotations

import os
import sys
from urllib.parse import quote_plus

from superset.app import create_app
from superset.extensions import db

CONN_NAME = os.environ.get("SUPERSET_DB_CONN_NAME", "ecommerce_dw (Superset RO)")


def build_target_uri() -> str:
    user = os.environ["MYSQL_RO_USER"]
    password = os.environ["MYSQL_RO_PASSWORD"]
    host = os.environ.get("SUPERSET_MYSQL_HOST", "mysql")
    port = os.environ.get("SUPERSET_MYSQL_PORT", "3306")
    database = os.environ["MYSQL_DATABASE"]
    return f"mysql+pymysql://{user}:{quote_plus(password)}@{host}:{port}/{database}?charset=utf8mb4"


def main() -> int:
    if os.environ.get("SUPERSET_META_DB_URI", "").startswith("sqlite"):
        print("[fix-db] metadata db is sqlite (dev default), skipping")
        return 0
    uri = build_target_uri()
    app = create_app()
    with app.app_context():
        # 必须在 app 上下文内导入（superset.models.core 模块级代码需要 app）
        from superset.models.core import Database

        rows = db.session.query(Database).filter_by(database_name=CONN_NAME).all()
        if not rows:
            print(
                f"[fix-db] no database row named '{CONN_NAME}' yet "
                "(first bootstrap happens via scripts/superset_setup.py)"
            )
            return 0
        for row in rows:
            # 4.1.2 不能直接赋 sqlalchemy_uri（那会把明文口令写进 URI 列）。
            # 必须走 set_sqlalchemy_uri：真实口令进 Fernet 加密的 password 列，
            # sqlalchemy_uri 只存掩码 URI；连接时由 sqlalchemy_uri_decrypted
            # 用解密后的口令拼回想连接的 URI。
            row.set_sqlalchemy_uri(uri)
        db.session.commit()
        print(f"[fix-db] updated '{CONN_NAME}' URI host={uri.split('@')[-1].split('/')[0]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
