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


datadir = "../../data_sintesis-biocifras/fuentesExternas/"
file = "geo_stateprovince_validation_20260522.csv"

stateprovince_val=pd.read_csv(datadir+file, sep="\t",low_memory=False)
#stateprovince_val=stateprovince_val.rename(columns={i: i.lower() for i in list(stateprovince_val.columns)})
#stateprovince_val=stateprovince_val.rename(columns={"file" : "sourcefile"})

#stateprovince_val.insert(0, "id", [i+1 for i in range(stateprovince_val.shape[0])], True)

dropColumns=['put the names of the columns you wanna suppress here']
for col in list(stateprovince_val.columns):
  if col in dropColumns:
    stateprovince_val=stateprovince_val.drop(labels=[col],axis=1)

colNeeded=['originalstateprovince','revisedstateprovince']
for col in colNeeded:
  if not col in list(stateprovince_val.columns):
    raise Exception(col + ' column is absent, and needed for the database to work')

engine=sqlal.create_engine("postgresql+psycopg2://"+os.getenv("DATABASE_USER")+":"+os.getenv("DATABASE_PASS")+"@"+os.getenv("DATABASE_HOST")+":"+os.getenv("DATABASE_PORT")+"/"+os.getenv("DATABASE_NAME"))
stateprovince_val.to_sql("tmp_stateprovince_val",engine, if_exists='replace')
engine.dispose()

db=c.get_db()

# Checking data
with db.connect() as conn:
  cur=conn.cursor()
  cur.execute("SELECT originalstateprovince, count(*) FROM tmp_stateprovince_val GROUP BY originalstateprovince HAVING count(*)>1")
  res=cur.fetchall()

if len(res)>0:
  print (res)
  raise Exception('''Variable originalstateprovince got repetition, the table is not uploaded but you may check on tmp_stateprovince_val in the database\n
        Please use "SELECT originalstateprovince, count(*) FROM tmp_stateprovince_val GROUP BY originalstateprovince HAVING count(*)>1"
        ''')


with db.connect() as conn:
  conn.execute("DROP TABLE IF EXISTS geo_stateprovince_validation")
  conn.execute('''CREATE TABLE public.geo_stateprovince_validation (
    id serial PRIMARY KEY,
    originalstateprovince text NOT NULL UNIQUE,
    geo_master_geography_id int REFERENCES geo_master_geography (id)
  );
  ''')
  conn.commit()
  cur=conn.cursor()
  cur.execute('''SELECT column_name FROM information_schema.columns WHERE table_name='geo_stateprovince_validation' AND column_name <> 'id' ORDER BY ordinal_position ASC''')
  res=list(cur.fetchall())
  res=[i[0] for i in res]
  query=sql.SQL("INSERT INTO geo_stateprovince_validation ({0}) SELECT {1},{2} FROM tmp_stateprovince_val tsv LEFT JOIN geo_master_geography gmg ON tsv.revisedstateprovince = gmg.name AND gmg.subtype = 'departamento' ").format(
    sql.SQL(', ').join([sql.Identifier(i) for i in res]),
    sql.Identifier(res[0]),
    sql.Identifier("gmg","id")
    )
  cur.execute(query)
  conn.execute('DROP TABLE IF EXISTS tmp_stateprovince_val')
  conn.commit()
    
db.dispose()
  


