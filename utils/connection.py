import logging
import os

import psycopg2
from psycopg2.extensions import connection as BaseConnection

logger = logging.getLogger('sintesis_biocifras')


class _Result:
    def __init__(self, rows, rowcount):
        self._rows = rows
        self.rowcount = rowcount

    def fetchall(self):
        return self._rows


class PsycopgConnection(BaseConnection):
    def execute(self, sql, params=None):
        with self.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() if cur.description else []
            return _Result(rows=rows, rowcount=cur.rowcount)

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            return super().__exit__(exc_type, exc_value, traceback)
        finally:
            self.close()


class PsycopgEngine:
    def __init__(self, **conn_kwargs):
        self._conn_kwargs = conn_kwargs

    def connect(self):
        return psycopg2.connect(
            **self._conn_kwargs,
            connection_factory=PsycopgConnection,
        )

    def raw_connection(self):
        return psycopg2.connect(**self._conn_kwargs)

    def dispose(self):
        # Sin pool explícito: no hay recursos compartidos para liberar.
        return None


def get_db():
    return PsycopgEngine(
        user=os.getenv('DATABASE_USER'),
        password=os.getenv('DATABASE_PASS'),
        host=os.getenv('DATABASE_HOST'),
        port=os.getenv('DATABASE_PORT'),
        dbname=os.getenv('DATABASE_NAME'),
    )


def check_connection(db):
    try:
        with db.connect() as conn:
            conn.execute("SELECT 1")
            conn.execute(
                "SELECT has_database_privilege(current_user, current_database(), 'CREATE')"
            )
        logger.info(
            "Conexion exitosa a %s@%s:%s/%s",
            os.getenv('DATABASE_USER'),
            os.getenv('DATABASE_HOST'),
            os.getenv('DATABASE_PORT'),
            os.getenv('DATABASE_NAME'),
        )
        return True
    except Exception as e:
        logger.error("Fallo de conexion: %s", e)
        return False


def table_exists(db, table_name):
    with db.connect() as conn:
        result = conn.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = %(table_name)s
            )
            """,
            {'table_name': table_name},
        )
        row = result.fetchall()[0]
        return bool(row[0])
