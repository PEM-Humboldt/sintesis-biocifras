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

pathfile=os.getenv("FILE_STATS_GROUP")

list_grupo=pd.read_csv(pathfile, sep="\t",low_memory=False)
list_grupo=list_grupo.rename(columns={i: i.lower() for i in list(list_grupo.columns)})

dropColumns=['Put here the columns from the source that you wanna suppress']
for col in list(list_grupo.columns):
  if col in dropColumns:
    list_grupo=list_grupo.drop(labels=[col],axis=1)

db=c.get_db()
with db.connect() as conn:
  conn.execute("DROP MATERIALIZED VIEW IF EXISTS grupo")
  conn.commit()

engine=sqlal.create_engine("postgresql+psycopg2://"+os.getenv("DATABASE_USER")+":"+os.getenv("DATABASE_PASS")+"@"+os.getenv("DATABASE_HOST")+":"+os.getenv("DATABASE_PORT")+"/"+os.getenv("DATABASE_NAME"))
list_grupo.to_sql("tmp_cifras_group",engine, if_exists='replace', index=False)
engine.dispose()

with db.connect() as conn:
  conn.execute('''
CREATE MATERIALIZED VIEW public.grupo AS
    select * from tmp_cifras_group
  WITH DATA;
  ''')
  conn.commit()
    
db.dispose()
  


