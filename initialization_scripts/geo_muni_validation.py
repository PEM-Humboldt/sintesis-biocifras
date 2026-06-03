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

pathfile=os.getenv("FILE_MUNI_VALIDATION")
#datadir = "../../data_sintesis-biocifras/fuentesExternas/"
#file = "geo_county_validation_20260522.csv"

county_val=pd.read_csv(pathfile, sep="\t",low_memory=False)
#county_val=county_val.rename(columns={i: i.lower() for i in list(county_val.columns)})
#county_val=county_val.rename(columns={"file" : "sourcefile"})

#county_val.insert(0, "id", [i+1 for i in range(county_val.shape[0])], True)

dropColumns=['put the names of the columns you wanna suppress here']
for col in list(county_val.columns):
  if col in dropColumns:
    county_val=county_val.drop(labels=[col],axis=1)

engine=sqlal.create_engine("postgresql+psycopg2://"+os.getenv("DATABASE_USER")+":"+os.getenv("DATABASE_PASS")+"@"+os.getenv("DATABASE_HOST")+":"+os.getenv("DATABASE_PORT")+"/"+os.getenv("DATABASE_NAME"))
county_val.to_sql("tmp_county_val",engine, if_exists='replace')
engine.dispose()

db=c.get_db()
with db.connect() as conn:
  conn.execute("DROP TABLE IF EXISTS geo_county_validation")
  conn.execute('''CREATE TABLE public.geo_county_validation (
    id serial PRIMARY KEY,
    originalcounty text NOT NULL UNIQUE,
    revisedcounty text
  );
  ''')
  conn.commit()
  cur=conn.cursor()
  cur.execute('''SELECT column_name FROM information_schema.columns WHERE table_name='geo_county_validation' AND column_name <> 'id' ''')
  res=list(cur.fetchall())
  res=[i[0] for i in res]
  if res!=list(county_val.columns):
    warnings.warn('Taxonomic list for colombia: columns from the source file and in the database do not correspond, the list will not be created in the database')
    conn.execute('DROP TABLE IF EXISTS geo_county_validation')
    conn.execute('DROP TABLE IF EXISTS tmp_county_val')
    conn.commit()
  else:
    query=sql.SQL("INSERT INTO geo_county_validation ({0}) SELECT {1} FROM tmp_county_val").format(
      sql.SQL(', ').join([sql.Identifier(i) for i in list(county_val.columns)]),
      sql.SQL(', ').join([sql.Identifier(i) for i in list(county_val.columns)])
      )
    conn.execute(query)
    conn.execute("DROP TABLE IF EXISTS tmp_county_val")
    conn.commit()
    
db.dispose()
  


