
from dotenv import load_dotenv
import os
import sys
import sqlalchemy as sqlal
import pandas as pd
from psycopg2 import sql
import warnings
import utils.connection as c
import warnings
import time
load_dotenv()

from utils.logger import setup_logger

logger = setup_logger(os.getenv('LOG_FILE_PATH'))

logger.info('Inicialización de la base de datos.')
warnings.warn("""Please make sure you have revised all initialization scripts in \"initialization_scripts\" and \"sql\".
  Any malicious command in there could have bad consequences.
  You have 1 minute to stop before the initialization scripts start
              """)
time.sleep(60)

try:
  db=c.get_db()

  logger.info('Ejecutando sql/create_tables.sql')
  with db.connect() as conn:
    conn.execute(open("sql/create_tables.sql", "r").read())
    conn.commit()

  db.dispose()

  logger.info('Ejecutando initialization_scripts/adm_dept_muni.py')
  with open("initialization_scripts/adm_dept_muni.py","r") as f:
    exec(f.read())

  logger.info('Ejecutando initialization_scripts/narino_maritimo.py')
  with open("initialization_scripts/narino_maritimo.py") as f:
    exec(f.read())

  logger.info('Ejecutando initialization_scripts/regiones_maritimas.py')
  with open("initialization_scripts/regiones_maritimas.py") as f:
    exec(f.read())

  logger.info('Ejecutando initialization_scripts/spt_nucleos_dfyb.py')
  with open("initialization_scripts/spt_nucleos_dfyb.py") as f:
    exec(f.read())

  logger.info('Ejecutando initialization_scripts/spt_region_amazonia.py')
  with open("initialization_scripts/spt_region_amazonia.py") as f:
    exec(f.read())

  logger.info('Ejecutando initialization_scripts/spt_reservas.py')
  with open("initialization_scripts/spt_reservas.py") as f:
    exec(f.read())

  logger.info('Ejecutando initialization_scripts/spt_resguardos.py')
  with open("initialization_scripts/spt_resguardos.py") as f:
    exec(f.read())

  logger.info('Ejecutando initialization_scripts/geo_master_geography.py')
  with open("initialization_scripts/geo_master_geography.py") as f:
    exec(f.read())

  logger.info('Ejecutando initialization_scripts/geo_dept_validation.py')
  with open("initialization_scripts/geo_dept_validation.py") as f:
    exec(f.read())

  logger.info('Ejecutando initialization_scripts/geo_muni_validation.py')
  with open("initialization_scripts/geo_muni_validation.py") as f:
    exec(f.read())

  logger.info('Ejecutando initialization_scripts/lista_taxonomica.py')
  with open("initialization_scripts/lista_taxonomica.py") as f:
    exec(f.read())

  logger.info('Ejecutando initialization_scripts/inva_exot.py')
  with open("initialization_scripts/inva_exot.py") as f:
    exec(f.read())

  logger.info('Ejecutando initialization_scripts/migratory_birds.py')
  with open("initialization_scripts/migratory_birds.py") as f:
    exec(f.read())

  logger.info('Ejecutando initialization_scripts/threat_cites.py')
  with open("initialization_scripts/threat_cites.py") as f:
    exec(f.read())

  logger.info('Ejecutando initialization_scripts/threat_iucn.py')
  with open("initialization_scripts/threat_iucn.py") as f:
    exec(f.read())

  logger.info('Ejecutando initialization_scripts/threat_mads.py')
  with open("initialization_scripts/threat_mads.py") as f:
    exec(f.read())

  logger.info('Ejecutando initialization_scripts/worms_validation.py')
  with open("initialization_scripts/worms_validation.py") as f:
    exec(f.read())

  logger.info('Ejecutando initialization_scripts/grupos_biologicos.py')
  with open("initialization_scripts/grupos_biologicos.py") as f:
    exec(f.read())

  logger.info('Ejecutando initialization_scripts/cifras_dato_relevante.py')
  with open("initialization_scripts/cifras_dato_relevante.py") as f:
    exec(f.read())

  logger.info('Ejecutando initialization_scripts/cifras_especies_meta.py')
  with open("initialization_scripts/cifras_especies_meta.py") as f:
    exec(f.read())

  logger.info('Ejecutando initialization_scripts/cifras_grupo.py')
  with open("initialization_scripts/cifras_grupo.py") as f:
    exec(f.read())

  logger.info('Ejecutando initialization_scripts/cifras_patrocinador.py')
  with open("initialization_scripts/cifras_patrocinador.py") as f:
    exec(f.read())

  logger.info('Ejecutando initialization_scripts/cifras_ranking.py')
  with open("initialization_scripts/cifras_ranking.py") as f:
    exec(f.read())

  logger.info('Ejecutando initialization_scripts/cifras_region_patrocinador.py')
  with open("initialization_scripts/cifras_region_patrocinador.py") as f:
    exec(f.read())

  logger.info('Ejecutando initialization_scripts/cifras_estimadas_depto.py')
  with open("initialization_scripts/cifras_estimadas_depto.py") as f:
    exec(f.read())

  logger.info('Ejecutando initialization_scripts/cifras_publicador.py')
  with open("initialization_scripts/cifras_publicador.py") as f:
    exec(f.read())

  logger.info('Proceso completado.')
except Exception as e:
  logger.error('Error durante la inicialización: %s', e, exc_info=True)
  sys.exit(1)
