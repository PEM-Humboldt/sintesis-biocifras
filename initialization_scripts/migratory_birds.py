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

pathfile=os.getenv("FILE_TAXONOMIC_AVES_MIGRATORIAS")
#datadir = "../../data_sintesis-biocifras/fuentesExternas/"
#file = "Lista_Migratorias_2025.tsv"

lista_migrat=pd.read_csv(pathfile, sep="\t",low_memory=False)
lista_migrat=lista_migrat.rename(columns={i: i.lower() for i in list(lista_migrat.columns)})
lista_migrat=lista_migrat.rename(columns={"especies_migratorias" : "migratory"})

#lista_migrat.insert(0, "id", [i+1 for i in range(lista_migrat.shape[0])], True)

dropColumns=['Put here the columns from the source that you wanna suppress']
for col in list(lista_migrat.columns):
  if col in dropColumns:
    lista_migrat=lista_migrat.drop(labels=[col],axis=1)

engine=sqlal.create_engine("postgresql+psycopg2://"+os.getenv("DATABASE_USER")+":"+os.getenv("DATABASE_PASS")+"@"+os.getenv("DATABASE_HOST")+":"+os.getenv("DATABASE_PORT")+"/"+os.getenv("DATABASE_NAME"))
lista_migrat.to_sql("tmp_migrat",engine, if_exists='replace', index=False)
engine.dispose()

db=c.get_db()
with db.connect() as conn:
  conn.execute("DROP TABLE IF EXISTS taxonomic_migratory")
  conn.execute('''CREATE TABLE public.taxonomic_migratory (
    id serial PRIMARY KEY,
    scientificname text,
    species text NOT NULL,
    migratory text,
    scientificnameauthorship text,
    taxonrank text,
    kingdom text,
    phylum text,
    class text,
    "order" text,
    family text,
    genus text,
    specificepithet text,
    infraspecificepithet text
  );
  ''')
  conn.commit()
  cur=conn.cursor()
  cur.execute('''SELECT column_name FROM information_schema.columns WHERE table_name='taxonomic_migratory' AND column_name <> 'id' ''')
  res=list(cur.fetchall())
  res=[i[0] for i in res]
  if res!=list(lista_migrat.columns):
    warnings.warn('Taxonomic list for colombia: columns from the source file and in the database do not correspond, the list will not be created in the database')
    conn.execute('DROP TABLE IF EXISTS taxonomic_migratory')
    conn.execute('DROP TABLE IF EXISTS tmp_migrat')
    conn.commit()
  else:
    query=sql.SQL("INSERT INTO taxonomic_migratory ({0}) SELECT {1} FROM tmp_migrat").format(
      sql.SQL(', ').join([sql.Identifier(i) for i in list(lista_migrat.columns)]),
      sql.SQL(', ').join([sql.Identifier(i) for i in list(lista_migrat.columns)])
      )
    conn.execute(query)
    conn.execute("DROP TABLE IF EXISTS tmp_migrat")
    conn.execute("CREATE INDEX idx_taxonomic_migratory_species ON taxonomic_migratory USING BTREE(species)")
    conn.commit()
    
db.dispose()
  


