# Autor: Diego Moreno-Vargas (github.com/damorenov)
"""
Estadísticas básicas de la tabla integrada vigente y vista materializada region.

- Resolución de la tabla integrada vigente (table_registry, is_latest).
- Vista materializada region desde geo_master_geography (parent por parent_id y region_id).
- Vista materializada departamento desde geo_master_geography (subtype = departamento).
- Vista materializada municipio desde geo_master_geography (subtype = municipio).
"""

import logging
import os
import sys

from dotenv import load_dotenv

from utils.connection import check_connection, get_db, table_exists

load_dotenv()

logger = logging.getLogger('sintesis_biocifras')

INTEGRATED_PREFIX = 'dwc_integrated_%'


def setup_console_logger():
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    )
    logger.addHandler(handler)
    return logger


def get_latest_integrated_table(db):
    with db.connect() as conn:
        rows = conn.execute(
            """
            SELECT table_name
            FROM table_registry
            WHERE is_latest = TRUE
              AND table_name LIKE %(prefix)s
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            {'prefix': INTEGRATED_PREFIX},
        ).fetchall()
    if not rows:
        raise ValueError(
            'No hay tabla integrada con is_latest=TRUE en table_registry.'
        )
    return rows[0][0]


def print_record_count(table_name, total):
    print(f'Tabla integrada (is_latest): {table_name}')
    print(f'Registros: {total:,}')


def create_region_materialized_view(db):
    if not table_exists(db, 'geo_master_geography'):
        raise ValueError('La tabla geo_master_geography no existe en la base de datos.')
    with db.connect() as conn:
        conn.execute('DROP MATERIALIZED VIEW IF EXISTS region')
        conn.execute("""
            CREATE MATERIALIZED VIEW region AS
            SELECT
                COALESCE(p.slug, '0') AS parent,
                d.slug,
                d."name" AS label,
                d."type" AS tipo,
                d.subtype AS subtipo,
                d.description AS descripcion,
                d.ismarine AS marino
            FROM geo_master_geography d
            LEFT JOIN geo_master_geography p ON p.id = d.parent_id

            UNION ALL

            SELECT
                r.slug AS parent,
                d.slug,
                d."name" AS label,
                d."type" AS tipo,
                d.subtype AS subtipo,
                d.description AS descripcion,
                d.ismarine AS marino
            FROM geo_master_geography d
            JOIN geo_master_geography r ON r.id = d.region_id
            ORDER BY parent
        """)
        conn.commit()
        total = conn.execute('SELECT COUNT(*) FROM region').fetchall()[0][0]
    logger.info('Vista materializada region creada (%s filas)', total)
    print(f'Vista materializada region: {total:,} filas')
    return total


def create_departamento_materialized_view(db):
    if not table_exists(db, 'geo_master_geography'):
        raise ValueError('La tabla geo_master_geography no existe en la base de datos.')
    with db.connect() as conn:
        conn.execute('DROP MATERIALIZED VIEW IF EXISTS departamento')
        conn.execute("""
            CREATE MATERIALIZED VIEW departamento AS
            SELECT
                d.slug,
                d."name" AS label,
                d.codedane AS cod_dane,
                d.ismarine AS marino,
                d."date" AS fecha_corte
            FROM geo_master_geography d
            WHERE d.subtype = 'departamento'
            ORDER BY slug
        """)
        conn.commit()
        total = conn.execute('SELECT COUNT(*) FROM departamento').fetchall()[0][0]
    logger.info('Vista materializada departamento creada (%s filas)', total)
    print(f'Vista materializada departamento: {total:,} filas')
    return total


def create_municipio_materialized_view(db):
    if not table_exists(db, 'geo_master_geography'):
        raise ValueError('La tabla geo_master_geography no existe en la base de datos.')
    with db.connect() as conn:
        conn.execute('DROP MATERIALIZED VIEW IF EXISTS municipio')
        conn.execute("""
            CREATE MATERIALIZED VIEW municipio AS
            SELECT
                d.slug,
                d."name" AS label,
                d.codedane AS cod_dane,
                d.ismarine AS marino,
                d."date" AS fecha_corte
            FROM geo_master_geography d
            WHERE d.subtype = 'municipio'
            ORDER BY slug
        """)
        conn.commit()
        total = conn.execute('SELECT COUNT(*) FROM municipio').fetchall()[0][0]
    logger.info('Vista materializada municipio creada (%s filas)', total)
    print(f'Vista materializada municipio: {total:,} filas')
    return total


def main():
    setup_console_logger()
    db = get_db()

    if not check_connection(db):
        logger.error(
            'No se pudo conectar a la base de datos. Verifique los valores de conexión en .env'
        )
        sys.exit(1)

    try:
        create_region_materialized_view(db)
        create_departamento_materialized_view(db)
        create_municipio_materialized_view(db)
        table_name = get_latest_integrated_table(db)
        logger.info('Tabla integrada vigente: %s', table_name)
    except ValueError as e:
        logger.error('%s', e)
        sys.exit(1)
    except Exception as e:
        logger.error('Error al generar estadísticas: %s', e, exc_info=True)
        sys.exit(1)
    finally:
        db.dispose()


if __name__ == '__main__':
    main()
