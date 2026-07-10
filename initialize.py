
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



warnings.warn("""Please make sure you have revised all initialization scripts in \"initialization_scripts\" and \"sql\".
  Any malicious command in there could have bad consequences.
  You have 1 minute to stop before the initialization scripts start
              """)
time.sleep(60)

db=c.get_db()

with db.connect() as conn:
  conn.execute(open("sql/create_tables.sql", "r").read())
  conn.commit()

db.dispose()  
  
with open("initialization_scripts/adm_dept_muni.py","r") as f:
  exec(f.read())

with open("initialization_scripts/narino_maritimo.py") as f:
	exec(f.read())

with open("initialization_scripts/regiones_maritimas.py") as f:
	exec(f.read())

with open("initialization_scripts/spt_nucleos_dfyb.py") as f:
	exec(f.read())

with open("initialization_scripts/spt_region_amazonia.py") as f:
	exec(f.read())

with open("initialization_scripts/spt_reservas.py") as f:
	exec(f.read())

with open("initialization_scripts/spt_resguardos.py") as f:
	exec(f.read())

with open("initialization_scripts/geo_master_geography.py") as f:
	exec(f.read())

with open("initialization_scripts/geo_dept_validation.py") as f:
	exec(f.read())

with open("initialization_scripts/geo_muni_validation.py") as f:
	exec(f.read())

with open("initialization_scripts/lista_taxonomica.py") as f:
	exec(f.read())

with open("initialization_scripts/inva_exot.py") as f:
	exec(f.read())

with open("initialization_scripts/migratory_birds.py") as f:
	exec(f.read())

with open("initialization_scripts/threat_cites.py") as f:
	exec(f.read())

with open("initialization_scripts/threat_iucn.py") as f:
	exec(f.read())

with open("initialization_scripts/threat_mads.py") as f:
	exec(f.read())

with open("initialization_scripts/worms_validation.py") as f:
	exec(f.read())

with open("initialization_scripts/cifras_dato_relevante.py") as f:
	exec(f.read())

with open("initialization_scripts/cifras_especies_meta.py") as f:
	exec(f.read())

with open("initialization_scripts/cifras_grupo.py") as f:
	exec(f.read())

with open("initialization_scripts/cifras_patrocinador.py") as f:
	exec(f.read())

with open("initialization_scripts/cifras_ranking.py") as f:
	exec(f.read())

with open("initialization_scripts/cifras_region_patrocinador.py") as f:
	exec(f.read())