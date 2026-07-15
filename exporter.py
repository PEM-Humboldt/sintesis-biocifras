# Autor: Diego Moreno-Vargas (github.com/damorenov)

"""
Exporta las vistas materializadas de producto del portal a archivos TSV.

Reemplaza la salida de archivos del legacy generador.py. Cada MV se vuelca
con COPY (streaming, sin cargar todo en memoria) a <EXPORT_DIR>/<nombre_mv>.tsv
en UTF-8, separador tab y con encabezado.
"""

import os
import sys

from dotenv import load_dotenv

load_dotenv()

from utils.connection import get_db, check_connection
from utils.logger import setup_logger

# MVs de producto equivalentes a las salidas de generador.py.
_EXPORT_MVS = (
    'publicador',
    'region_publicador',
    'especie',
    'especie_meta',
    'especie_grupo',
    'especie_region',
    'especie_tematica',
    'cifras_totales',
    'region_tematica',
    'region_grupo',
)

logger = setup_logger(os.getenv('LOG_FILE_PATH'))


def matview_exists(conn, name):
    # Verifica que la MV exista en el schema public.
    rows = conn.execute(
        "SELECT 1 FROM pg_matviews WHERE schemaname = 'public' AND matviewname = %s",
        (name,),
    ).fetchall()
    return bool(rows)


def export_matview(raw_conn, name, out_dir):
    # Vuelca una MV a TSV con COPY. Formato csv + delimitador tab para usar
    # quoting mínimo (igual que pandas.to_csv(sep='\t') del legacy) y ser
    # robusto ante tabs o saltos de línea dentro de los datos.
    path = os.path.join(out_dir, f'{name}.tsv')
    copy_sql = (
        f'COPY (SELECT * FROM "{name}") TO STDOUT '
        "WITH (FORMAT csv, DELIMITER E'\\t', HEADER true)"
    )
    with raw_conn.cursor() as cur, open(path, 'w', encoding='utf-8', newline='') as f:
        cur.copy_expert(copy_sql, f)
    return path


def main():
    out_dir = os.getenv('EXPORT_DIR')
    if not out_dir:
        logger.error('Falta EXPORT_DIR en .env')
        sys.exit(1)
    os.makedirs(out_dir, exist_ok=True)

    db = get_db()
    if not check_connection(db):
        sys.exit(1)

    # Valida que todas las MVs existan antes de exportar.
    with db.connect() as conn:
        missing = [name for name in _EXPORT_MVS if not matview_exists(conn, name)]
    if missing:
        logger.error('MVs faltantes: %s', ', '.join(missing))
        sys.exit(1)

    raw_conn = db.raw_connection()
    try:
        for name in _EXPORT_MVS:
            path = export_matview(raw_conn, name, out_dir)
            logger.info('Exportado %s', path)
    finally:
        raw_conn.close()
        db.dispose()


if __name__ == '__main__':
    main()
