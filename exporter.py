# Autor: Diego Moreno-Vargas (github.com/damorenov)

"""
Exporta las vistas materializadas de producto del portal a archivos TSV.

Cada MV se lee con pandas/SqlAlchemy y se exporta a <EXPORT_DIR>/<nombre_mv>.tsv.
Los booleanos se escriben como TRUE/FALSE manteniendo el tipo de dato con script originales.
"""

import os
import sys

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

from utils.connection import get_db, check_connection
from utils.logger import setup_logger

# MVs de tablas a las salidas de generador.py.
_EXPORT_MVS = (
    'dato_relevante',
    'departamento',
    'municipio',
    'region',
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


def export_matview(conn, name, out_dir):
    # Lee la MV con cursor psycopg2 y la exporta a TSV.
    path = os.path.join(out_dir, f'{name}.tsv')
    with conn.cursor() as cur:
        cur.execute(f'SELECT * FROM "{name}"')
        df = pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])
    df = df.convert_dtypes()
    for col in df.select_dtypes(include=['bool', 'boolean']).columns:
        df[col] = df[col].map({True: 'TRUE', False: 'FALSE'})
    df.to_csv(path, sep='\t', index=False, encoding='utf-8')
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

    try:
        with db.connect() as conn:
            missing = [name for name in _EXPORT_MVS if not matview_exists(conn, name)]
            if missing:
                logger.error('MVs faltantes: %s', ', '.join(missing))
                sys.exit(1)
            for name in _EXPORT_MVS:
                path = export_matview(conn, name, out_dir)
                logger.info('Exportado %s', path)
    finally:
        db.dispose()


if __name__ == '__main__':
    main()
