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

pathfile=os.getenv("FILE_STATS_SPECIES_META")

lista_especies_meta=pd.read_csv(pathfile, sep="\t",low_memory=False)
lista_especies_meta=lista_especies_meta.rename(columns={i: i.lower() for i in list(lista_especies_meta.columns)})

dropColumns=['Put here the columns from the source that you wanna suppress']
for col in list(lista_especies_meta.columns):
  if col in dropColumns:
    lista_especies_meta=lista_especies_meta.drop(labels=[col],axis=1)

engine=sqlal.create_engine("postgresql+psycopg2://"+os.getenv("DATABASE_USER")+":"+os.getenv("DATABASE_PASS")+"@"+os.getenv("DATABASE_HOST")+":"+os.getenv("DATABASE_PORT")+"/"+os.getenv("DATABASE_NAME"))
lista_especies_meta.to_sql("taxonomic_species_meta",engine, if_exists='replace', index=False)
engine.dispose()

db=c.get_db()
with db.connect() as conn:
  conn.execute("CREATE INDEX idx_taxonomic_species_meta_slug ON taxonomic_species_meta USING btree (slug)")
  conn.commit()
db.dispose()


