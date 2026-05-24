#!/usr/bin/env python3
"""
Prueba aislada de link_integrated_locality_id.

Asume que ya existen en PostgreSQL:
  - la tabla integrada (p. ej. dwc_integrated_20260523)
  - geo_locality_validation poblada (validate_localities)

Uso:
  python link_locality_id_test.py
  python link_locality_id_test.py --table dwc_integrated_20260523
  python link_locality_id_test.py --table dwc_integrated_20260523 --skip-vacuum

Variables .env: mismas que main.py (DATABASE_*). Opcional INTEGRATED_TABLE.
"""

import argparse
import os
import sys
from datetime import date

from dotenv import load_dotenv

load_dotenv()

from utils.connection import check_connection, get_db
from utils.functions import link_integrated_locality_id
from utils.logger import setup_logger


def main():
    parser = argparse.ArgumentParser(
        description='Prueba el enlace integrada → geo_locality_validation (locality_id)',
    )
    parser.add_argument(
        '--table',
        help='Tabla integrada. Por defecto INTEGRATED_TABLE o dwc_integrated_YYYYMMDD',
    )
    parser.add_argument(
        '--skip-vacuum',
        action='store_true',
        help='No ejecutar VACUUM/ANALYZE al final (SKIP_TABLE_MAINTENANCE=true)',
    )
    args = parser.parse_args()

    table_name = (
        args.table
        or os.getenv('INTEGRATED_TABLE')
        or f'dwc_integrated_{date.today().strftime("%Y%m%d")}'
    )

    if args.skip_vacuum:
        os.environ['SKIP_TABLE_MAINTENANCE'] = 'true'

    logger = setup_logger(os.getenv('LOG_FILE_PATH'))
    logger.info('Prueba link_integrated_locality_id en %s', table_name)

    db = get_db()
    if not check_connection(db):
        logger.error('No se pudo conectar a la base de datos (.env)')
        sys.exit(1)

    try:
        link_integrated_locality_id(db, table_name)
    except Exception:
        logger.exception('Falló el enlace locality_id')
        sys.exit(1)
    finally:
        db.dispose()

    logger.info('Prueba finalizada correctamente.')


if __name__ == '__main__':
    main()
