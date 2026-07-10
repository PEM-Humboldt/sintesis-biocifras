from dotenv import load_dotenv
import os
import sys
import sqlalchemy as sqlal
import pandas as pd
from psycopg2 import sql
import warnings
load_dotenv()

sys.path.append('..')
import utils.connection as c

pathfile=os.getenv("FILE_STATS_REGION_PATROCINADOR")

list_region_patrocinador=pd.read_csv(pathfile, sep="\t",low_memory=False)
list_region_patrocinador=list_region_patrocinador.rename(columns={i: i.lower() for i in list(list_region_patrocinador.columns)})

dropColumns=['Put here the columns from the source that you wanna suppress']
for col in list(list_region_patrocinador.columns):
  if col in dropColumns:
    list_region_patrocinador=list_region_patrocinador.drop(labels=[col],axis=1)

db=c.get_db()
with db.connect() as conn:
  conn.execute("DROP MATERIALIZED VIEW IF EXISTS region_patrocinador")
  conn.commit()

engine=sqlal.create_engine("postgresql+psycopg2://"+os.getenv("DATABASE_USER")+":"+os.getenv("DATABASE_PASS")+"@"+os.getenv("DATABASE_HOST")+":"+os.getenv("DATABASE_PORT")+"/"+os.getenv("DATABASE_NAME"))
list_region_patrocinador.to_sql("tmp_region_patrocinador",engine, if_exists='replace', index=False)
engine.dispose()

with db.connect() as conn:
  conn.execute('''
CREATE MATERIALIZED VIEW public.region_patrocinador AS
    select * from tmp_region_patrocinador
  WITH DATA;
  ''')
  conn.commit()
    
db.dispose()
  


