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

pathfile=os.getenv("FILE_STATS_DATA_RELEVANT")

lista_cifrasRelevante=pd.read_csv(pathfile, sep="\t",low_memory=False)
lista_cifrasRelevante=lista_cifrasRelevante.rename(columns={i: i.lower() for i in list(lista_cifrasRelevante.columns)})

dropColumns=['Put here the columns from the source that you wanna suppress']
for col in list(lista_cifrasRelevante.columns):
  if col in dropColumns:
    lista_cifrasRelevante=lista_cifrasRelevante.drop(labels=[col],axis=1)

db=c.get_db()
with db.connect() as conn:
  conn.execute("DROP MATERIALIZED VIEW IF EXISTS dato_relevante")
  conn.commit()

engine=sqlal.create_engine("postgresql+psycopg2://"+os.getenv("DATABASE_USER")+":"+os.getenv("DATABASE_PASS")+"@"+os.getenv("DATABASE_HOST")+":"+os.getenv("DATABASE_PORT")+"/"+os.getenv("DATABASE_NAME"))
lista_cifrasRelevante.to_sql("tmp_cifras_data_relevant",engine, if_exists='replace', index=False)
engine.dispose()

with db.connect() as conn:
  conn.execute('''
CREATE MATERIALIZED VIEW public.dato_relevante AS
    select * from tmp_cifras_data_relevant
  WITH DATA;
  ''')
  conn.commit()
    
db.dispose()
  


