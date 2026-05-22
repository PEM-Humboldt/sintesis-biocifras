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
file = "Lista_UICNRedList_2024T4.tsv"

lista_thIUCN=pd.read_csv(datadir+file, sep="\t",low_memory=False)
lista_thIUCN=lista_thIUCN.rename(columns={i: i.lower() for i in list(lista_thIUCN.columns)})
lista_thIUCN=lista_thIUCN.rename(columns={"internaltaxonid" : "originaltaxonid",'threatstatus_uicn':"threatstatus"})

#lista_thIUCN.insert(0, "id", [i+1 for i in range(lista_thIUCN.shape[0])], True)

dropColumns=['Put here the columns you wanna supress']
for col in list(lista_thIUCN.columns):
  if col in dropColumns:
    lista_thIUCN=lista_thIUCN.drop(labels=[col],axis=1)

engine=sqlal.create_engine("postgresql+psycopg2://"+os.getenv("DATABASE_USER")+":"+os.getenv("DATABASE_PASS")+"@"+os.getenv("DATABASE_HOST")+":"+os.getenv("DATABASE_PORT")+"/"+os.getenv("DATABASE_NAME"))
lista_thIUCN.to_sql("tmp_threat_iucn",engine, if_exists='replace', index=False)
engine.dispose()

db=c.get_db()
with db.connect() as conn:
  conn.execute("DROP TABLE IF EXISTS taxonomic_threat_iucn")
  conn.execute('''
CREATE TABLE public.taxonomic_threat_iucn (
    id serial PRIMARY KEY,
    originaltaxonid text,
    scientificname text,
    species text NOT NULL,
    scientificnameauthorship text,
    taxonrank text,
    kingdom text,
    phylum text,
    class text,
    "order" text,
    family text,
    genus text,
    specificepithet text,
    threatstatus text
  );
  ''')
  conn.commit()
  cur=conn.cursor()
  cur.execute('''SELECT column_name FROM information_schema.columns WHERE table_name='taxonomic_threat_iucn' AND column_name <> 'id' ''')
  res=list(cur.fetchall())
  res=[i[0] for i in res]
  if res!=list(lista_thIUCN.columns):
    warnings.warn('Taxonomic list for colombia: columns from the source file and in the database do not correspond, the list will not be created in the database')
    conn.execute('DROP TABLE IF EXISTS taxonomic_threat_iucn')
    conn.execute('DROP TABLE IF EXISTS tmp_threat_iucn')
    conn.commit()
  else:
    query=sql.SQL("INSERT INTO taxonomic_threat_iucn ({0}) SELECT {1} FROM tmp_threat_iucn").format(
      sql.SQL(', ').join([sql.Identifier(i) for i in list(lista_thIUCN.columns)]),
      sql.SQL(', ').join([sql.Identifier(i) for i in list(lista_thIUCN.columns)])
      )
    conn.execute(query)
    conn.execute("DROP TABLE IF EXISTS tmp_threat_iucn")
    conn.execute("CREATE INDEX idx_taxonomic_threat_iucn_species ON taxonomic_threat_iucn USING BTREE(species)")
    conn.commit()
    
db.dispose()
  


