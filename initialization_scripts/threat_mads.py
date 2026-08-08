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

pathfile=os.getenv("FILE_TAXONOMIC_MADS")
#datadir = "../../data_sintesis-biocifras/fuentesExternas/"
#file = "ListaAmenazadasMADS_2024.tsv"

lista_thMADS=pd.read_csv(pathfile, sep="\t",low_memory=False)
lista_thMADS=lista_thMADS.rename(columns={i: i.lower() for i in list(lista_thMADS.columns)})
lista_thMADS=lista_thMADS.rename(columns={"taxonid" : "originaltaxonid"})

#lista_thMADS.insert(0, "id", [i+1 for i in range(lista_thMADS.shape[0])], True)

dropColumns=['higherclassification']
for col in list(lista_thMADS.columns):
  if col in dropColumns:
    lista_thMADS=lista_thMADS.drop(labels=[col],axis=1)

engine=sqlal.create_engine("postgresql+psycopg2://"+os.getenv("DATABASE_USER")+":"+os.getenv("DATABASE_PASS")+"@"+os.getenv("DATABASE_HOST")+":"+os.getenv("DATABASE_PORT")+"/"+os.getenv("DATABASE_NAME"))
lista_thMADS.to_sql("tmp_threat_mads",engine, if_exists='replace', index=False)
engine.dispose()

db=c.get_db()
with db.connect() as conn:
  conn.execute("DROP TABLE IF EXISTS taxonomic_threat_mads")
  conn.execute('''CREATE TABLE public.taxonomic_threat_mads (
    id serial PRIMARY KEY,
    originaltaxonid text,
    threatstatus text,
    scientificname text,
    species text NOT NULL,
    scientificnameauthorship text,
    taxonomicstatus text,
    kingdom text,
    phylum text,
    class text,
    "order" text,
    family text,
    genus text,
    specificepithet text,
    taxonrank text,
    vernacularname text,
    taxonremarks text
  );
  ''')
  conn.commit()
  cur=conn.cursor()
  cur.execute('''SELECT column_name FROM information_schema.columns WHERE table_name='taxonomic_threat_mads' AND column_name <> 'id' ''')
  res=list(cur.fetchall())
  res=[i[0] for i in res]
  if res!=list(lista_thMADS.columns):
    warnings.warn('Taxonomic list for colombia: columns from the source file and in the database do not correspond, the list will not be created in the database')
    conn.execute('DROP TABLE IF EXISTS taxonomic_threat_mads')
    conn.execute('DROP TABLE IF EXISTS tmp_threat_mads')
    conn.commit()
  else:
    query=sql.SQL("INSERT INTO taxonomic_threat_mads ({0}) SELECT {1} FROM tmp_threat_mads").format(
      sql.SQL(', ').join([sql.Identifier(i) for i in list(lista_thMADS.columns)]),
      sql.SQL(', ').join([sql.Identifier(i) for i in list(lista_thMADS.columns)])
      )
    conn.execute(query)
    conn.execute("DROP TABLE IF EXISTS tmp_threat_mads")
    conn.execute("CREATE INDEX idx_taxonomic_threat_mads_species ON taxonomic_threat_mads USING BTREE(species)")
    conn.commit()
    
db.dispose()
  


