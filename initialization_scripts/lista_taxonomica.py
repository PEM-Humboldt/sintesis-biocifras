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

pathfile= os.getenv("FILE_TAXONOMIC_LIST")
#datadir = "../../data_sintesis-biocifras/fuentesExternas/"
#file = "Listas_taxonomicasCol_2024T4.tsv"

lista_tax=pd.read_csv(pathfile, sep="\t",low_memory=False)
lista_tax=lista_tax.rename(columns={i: i.lower() for i in list(lista_tax.columns)})
lista_tax=lista_tax.rename(columns={"file" : "sourcefile"})

#lista_tax.insert(0, "id", [i+1 for i in range(lista_tax.shape[0])], True)

dropColumns=['put the names of the columns you wanna suppress here']
for col in list(lista_tax.columns):
  if col in dropColumns:
    lista_tax=lista_tax.drop(labels=[col],axis=1)

engine=sqlal.create_engine("postgresql+psycopg2://"+os.getenv("DATABASE_USER")+":"+os.getenv("DATABASE_PASS")+"@"+os.getenv("DATABASE_HOST")+":"+os.getenv("DATABASE_PORT")+"/"+os.getenv("DATABASE_NAME"))
lista_tax.to_sql("tmp_taxonomic_list",engine, if_exists='replace')
engine.dispose()

db=c.get_db()
with db.connect() as conn:
  conn.execute("DROP TABLE IF EXISTS taxonomic_col_list")
  conn.execute('''CREATE TABLE taxonomic_col_list (
    id serial PRIMARY KEY,
    sourcefile text,
    scientificname text,
    species text NOT NULL,
    migratory text,
    endemic text,
    taxonrank text,
    especies_unicas text,
    establishmentmeans text,
    scientificnameauthorship text,
    taxonomicstatus text,
    kingdom text,
    phylum text,
    class text,
    "order" text,
    superfamily text,
    family text,
    subfamily text,
    tribe text,
    subtribe text,
    genus text,
    subgenus text,
    specificepithet text,
    infraspecificepithet text,
    infragenericepithet text,
    cultivarepithet text,
    genericname text,
    taxonid text,
    datasetid text
    );
  ''')
  conn.commit()
  cur=conn.cursor()
  cur.execute('''SELECT column_name FROM information_schema.columns WHERE table_name='taxonomic_col_list' AND column_name <> 'id' ''')
  res=list(cur.fetchall())
  res=[i[0] for i in res]
  if res!=list(lista_tax.columns):
    warnings.warn('Taxonomic list for colombia: columns from the source file and in the database do not correspond, the list will not be created in the database')
    conn.execute('DROP TABLE IF EXISTS taxonomic_col_list')
    conn.execute('DROP TABLE IF EXISTS tmp_taxonomic_list')
    conn.commit()
  else:
    query=sql.SQL("INSERT INTO taxonomic_col_list ({0}) SELECT {1} FROM tmp_taxonomic_list").format(
      sql.SQL(', ').join([sql.Identifier(i) for i in list(lista_tax.columns)]),
      sql.SQL(', ').join([sql.Identifier(i) for i in list(lista_tax.columns)])
      )
    conn.execute(query)
    conn.execute("DROP TABLE IF EXISTS tmp_taxonomic_list")
    conn.execute("CREATE INDEX idx_taxonomic_col_list_species ON taxonomic_col_list USING BTREE(species)")
    conn.commit()
    
db.dispose()
  


