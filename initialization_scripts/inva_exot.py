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
file = "lista-invasoras-exoticas-2024T4.tsv"

lista_inex=pd.read_csv(datadir+file, sep="\t",low_memory=False)
lista_inex=lista_inex.rename(columns={i: i.lower() for i in list(lista_inex.columns)})
lista_inex=lista_inex.rename(columns={"fuente" : "source", "especies_exoticas": "exotic", "especies_exotica_riesgo_invasion": "exoticriskinvasion",'species_invasiveness': "invasiveness", 'especies_invasoras':"invasive", 'especies_trasplantadas':"transplanted"})

#lista_inex.insert(0, "id", [i+1 for i in range(lista_inex.shape[0])], True)

dropColumns=['put the names of the columns you wanna suppress here']
for col in list(lista_inex.columns):
  if col in dropColumns:
    lista_inex=lista_inex.drop(labels=[col],axis=1)

engine=sqlal.create_engine("postgresql+psycopg2://"+os.getenv("DATABASE_USER")+":"+os.getenv("DATABASE_PASS")+"@"+os.getenv("DATABASE_HOST")+":"+os.getenv("DATABASE_PORT")+"/"+os.getenv("DATABASE_NAME"))
lista_inex.to_sql("tmp_inva_exot",engine, if_exists='replace', index=False)
engine.dispose()

db=c.get_db()
with db.connect() as conn:
  conn.execute("DROP TABLE IF EXISTS taxonomic_invasive_exotic")
  conn.execute('''CREATE TABLE public.taxonomic_invasive_exotic (
    id  serial PRIMARY KEY,
    source text,
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
    exotic text,
    exoticriskinvasion text,
    invasiveness text,
    invasive text,
    transplanted text
    );
  ''')
  conn.commit()
  cur=conn.cursor()
  cur.execute('''SELECT column_name FROM information_schema.columns WHERE table_name='taxonomic_invasive_exotic' AND column_name <> 'id' ''')
  res=list(cur.fetchall())
  res=[i[0] for i in res]
  if res!=list(lista_inex.columns):
    warnings.warn('Taxonomic list for colombia: columns from the source file and in the database do not correspond, the list will not be created in the database')
    conn.execute('DROP TABLE IF EXISTS taxonomic_invasive_exotic')
    conn.execute('DROP TABLE IF EXISTS tmp_inva_exot')
    conn.commit()
  else:
    query=sql.SQL("INSERT INTO taxonomic_invasive_exotic ({0}) SELECT {1} FROM tmp_inva_exot").format(
      sql.SQL(', ').join([sql.Identifier(i) for i in list(lista_inex.columns)]),
      sql.SQL(', ').join([sql.Identifier(i) for i in list(lista_inex.columns)])
      )
    conn.execute(query)
    conn.execute('''ALTER TABLE taxonomic_invasive_exotic ADD COLUMN exotictotal text ''')
    conn.execute('''UPDATE taxonomic_invasive_exotic SET exotictotal=COALESCE(exotic,exoticriskinvasion)''')
    conn.execute("DROP TABLE IF EXISTS tmp_inva_exot")
    conn.execute("CREATE INDEX idx_taxonomic_invasive_exotic_species ON taxonomic_invasive_exotic USING BTREE(species)")
    conn.commit()
    
db.dispose()
  


