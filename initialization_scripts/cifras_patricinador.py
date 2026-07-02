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

pathfile=os.getenv("FILE_STATS_RANKING")

list_ranking=pd.read_csv(pathfile, sep="\t",low_memory=False)
list_ranking=list_ranking.rename(columns={i: i.lower() for i in list(list_ranking.columns)})

dropColumns=['Put here the columns from the source that you wanna suppress']
for col in list(list_ranking.columns):
  if col in dropColumns:
    list_ranking=list_ranking.drop(labels=[col],axis=1)

db=c.get_db()
with db.connect() as conn:
  conn.execute("DROP MATERIALIZED VIEW IF EXISTS ranking")
  conn.commit()

engine=sqlal.create_engine("postgresql+psycopg2://"+os.getenv("DATABASE_USER")+":"+os.getenv("DATABASE_PASS")+"@"+os.getenv("DATABASE_HOST")+":"+os.getenv("DATABASE_PORT")+"/"+os.getenv("DATABASE_NAME"))
list_ranking.to_sql("tmp_cifras_ranking",engine, if_exists='replace', index=False)
engine.dispose()

with db.connect() as conn:
  conn.execute('''
CREATE MATERIALIZED VIEW public.ranking AS
    select * from tmp_cifras_ranking
  WITH DATA;
  ''')
  conn.commit()
    
db.dispose()
  


