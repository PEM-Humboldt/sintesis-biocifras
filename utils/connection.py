# Autor: Diego Moreno-Vargas (github.com/damorenov)
# Última modificación: 2026-04-30

"""
Este archivo contiene las funciones para la conexión a la base de datos PostgreSQL.
- get_db(): Crea el pool de conexiones de PostgreSQL.
- check_connection(): Verifica la conexión a la base de datos.
- table_exists(): Verifica si una tabla existe en la base de datos.
"""

import logging
import os

import psycopg2
from psycopg2.extensions import connection as BaseConnection

# Se conecta con el logger para tracking de eventos de conexión.
logger = logging.getLogger('sintesis_biocifras')


class _Result:
    def __init__(self, rows, rowcount):
        self._rows = rows
        self.rowcount = rowcount

    def fetchall(self):
        # Retorna todas las filas afectadas por execute.
        return self._rows


class PsycopgConnection(BaseConnection):
    def execute(self, sql, params=None):
        # Ejecuta SQL y retorna filas/rowcount para ser utilizado en funciones de utils.functions.py.
        with self.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() if cur.description else []
            return _Result(rows=rows, rowcount=cur.rowcount)

    def __exit__(self, exc_type, exc_value, traceback):
        # Cierra la conexión al salir del context manager.
        try:
            return super().__exit__(exc_type, exc_value, traceback)
        finally:
            self.close()


class PsycopgEngine:
    def __init__(self, **conn_kwargs):
        self._conn_kwargs = conn_kwargs

    def connect(self):
        # Crea conexión al pool de conexiones de PostgreSQL.
        return psycopg2.connect(
            **self._conn_kwargs,
            connection_factory=PsycopgConnection,
        )

    def raw_connection(self):
        # Se usa para operaciones especiales (ej. COPY, VACUUM autocommit).
        return psycopg2.connect(**self._conn_kwargs)

    def dispose(self):
        return None


def get_db():
    # Crea el pool de conexiones de PostgreSQL.
    # Parámetros:
    # - user: usuario de la base de datos definido en el .env.
    # - password: contraseña de la base de datos definida en el .env.
    # - host: host de la base de datos definida en el .env.
    # - port: puerto de la base de datos definida en el .env.
    # - dbname: nombre de la base de datos definida en el .env.
    # Retorna:
    # - PsycopgEngine: objeto con la conexión al pool de conexiones.
    return PsycopgEngine(
        user=os.getenv('DATABASE_USER'),
        password=os.getenv('DATABASE_PASS'),
        host=os.getenv('DATABASE_HOST'),
        port=os.getenv('DATABASE_PORT'),
        dbname=os.getenv('DATABASE_NAME'),
    )


def check_connection(db):
    # Función para verificar la conexión a la base de datos.
    # Parámetros:
    # - db: Conexión al pool de conexiones creado con get_db().
    # Retorna:
    # - True: si la conexión es exitosa.
    # - False: si la conexión falla.
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
    # Consulta information_schema para validar si una tabla existe en schema public.
    # Parámetros:
    # - db: Conexión al pool de conexiones creado con get_db().
    # - table_name: nombre de la tabla a verificar.
    # Retorna:
    # - True: si la tabla existe.
    # - False: si la tabla no existe.
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
