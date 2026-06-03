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

pathfile=os.getenv("FILE_TAXONOMIC_CITES")
#datadir = "../../data_sintesis-biocifras/fuentesExternas/"
#file = "Lista_CITES_20241231.tsv"

lista_thCITES=pd.read_csv(pathfile, sep="\t",low_memory=False)
lista_thCITES=lista_thCITES.rename(columns={i: i.lower() for i in list(lista_thCITES.columns)})
lista_thCITES=lista_thCITES.rename(columns={"taxonid" : "originaltaxonid", 'appendixcites': 'cites'})

#lista_thCITES.insert(0, "id", [i+1 for i in range(lista_thCITES.shape[0])], True)

dropColumns=['Put here the columns from the source that you wanna suppress']
for col in list(lista_thCITES.columns):
  if col in dropColumns:
    lista_thCITES=lista_thCITES.drop(labels=[col],axis=1)

engine=sqlal.create_engine("postgresql+psycopg2://"+os.getenv("DATABASE_USER")+":"+os.getenv("DATABASE_PASS")+"@"+os.getenv("DATABASE_HOST")+":"+os.getenv("DATABASE_PORT")+"/"+os.getenv("DATABASE_NAME"))
lista_thCITES.to_sql("tmp_threat_cites",engine, if_exists='replace', index=False)
engine.dispose()

db=c.get_db()
with db.connect() as conn:
  conn.execute("DROP TABLE IF EXISTS taxonomic_cites")
  conn.execute('''
CREATE TABLE public.taxonomic_cites (
    id serial PRIMARY KEY,
    originaltaxonid text,
    scientificname text,
    species text NOT NULL,
    scientificnameauthorship text,
    kingdom text,
    phylum text,
    class text,
    "order" text,
    family text,
    genus text,
    specificepithet text,
    taxonrank text,
    cites text
  );
  ''')
  conn.commit()
  cur=conn.cursor()
  cur.execute('''SELECT column_name FROM information_schema.columns WHERE table_name='taxonomic_cites' AND column_name <> 'id' ''')
  res=list(cur.fetchall())
  res=[i[0] for i in res]
  if res!=list(lista_thCITES.columns):
    warnings.warn('Taxonomic list for colombia: columns from the source file and in the database do not correspond, the list will not be created in the database')
    conn.execute('DROP TABLE IF EXISTS taxonomic_cites')
    conn.execute('DROP TABLE IF EXISTS tmp_threat_cites')
    conn.commit()
  else:
    query=sql.SQL("INSERT INTO taxonomic_cites ({0}) SELECT {1} FROM tmp_threat_cites").format(
      sql.SQL(', ').join([sql.Identifier(i) for i in list(lista_thCITES.columns)]),
      sql.SQL(', ').join([sql.Identifier(i) for i in list(lista_thCITES.columns)])
      )
    conn.execute(query)
    conn.execute("DROP TABLE IF EXISTS tmp_threat_cites")
    conn.execute("CREATE INDEX idx_taxonomic_cites_species ON taxonomic_cites USING BTREE(species)")
    conn.commit()
    
db.dispose()
  


