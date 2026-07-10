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

pathfile=os.getenv("FILE_TAXONOMIC_GROUPS")

lista_taxGrupos=pd.read_csv(pathfile, sep="\t",low_memory=False)
lista_taxGrupos=lista_taxGrupos.rename(columns={i: i.lower() for i in list(lista_taxGrupos.columns)})
lista_taxGrupos=lista_taxGrupos.rename(columns={"grupo_parentid" : "originalgroupparentid", 'grupo_id': 'slug', 'grupo_label': 'name', 'grupotax':'taxon', 'grupotax_original':'originaltaxon', 'tipo_grupo': 'grouptype'})

dropColumns=['Put here the columns from the source that you wanna suppress']
for col in list(lista_taxGrupos.columns):
  if col in dropColumns:
    lista_taxGrupos=lista_taxGrupos.drop(labels=[col],axis=1)

engine=sqlal.create_engine("postgresql+psycopg2://"+os.getenv("DATABASE_USER")+":"+os.getenv("DATABASE_PASS")+"@"+os.getenv("DATABASE_HOST")+":"+os.getenv("DATABASE_PORT")+"/"+os.getenv("DATABASE_NAME"))
lista_taxGrupos.to_sql("tmp_taxonomic_groups",engine, if_exists='replace', index=False)
engine.dispose()

db=c.get_db()
with db.connect() as conn:
  conn.execute("DROP TABLE IF EXISTS taxonomic_groups")
  conn.execute('''
CREATE TABLE public.taxonomic_groups (
    id serial PRIMARY KEY,
    originalgroupparentid text,
    slug text,
    name text NOT NULL,
    taxon text,
    originaltaxon text,
    taxonrank text,
    ipbes text,
    col text,
    web text,
    grouptype text
  );
  ''')
  conn.commit()
  cur=conn.cursor()
  cur.execute('''SELECT column_name FROM information_schema.columns WHERE table_name='taxonomic_groups' AND column_name <> 'id' ''')
  res=list(cur.fetchall())
  res=[i[0] for i in res]
  if res!=list(lista_taxGrupos.columns):
    warnings.warn('Biologicas groups: columns from the source file and in the database do not correspond, the list will not be created in the database')
    conn.execute('DROP TABLE IF EXISTS taxonomic_groups')
    conn.execute('DROP TABLE IF EXISTS tmp_taxonomic_groups')
    conn.commit()
  else:
    query=sql.SQL("INSERT INTO taxonomic_groups ({0}) SELECT {1} FROM tmp_taxonomic_groups").format(
      sql.SQL(', ').join([sql.Identifier(i) for i in list(lista_taxGrupos.columns)]),
      sql.SQL(', ').join([sql.Identifier(i) for i in list(lista_taxGrupos.columns)])
      )
    conn.execute(query)
    conn.execute("DROP TABLE IF EXISTS tmp_taxonomic_groups")
    conn.execute("CREATE INDEX idx_taxonomic_groups ON taxonomic_groups USING BTREE(taxon)")
    conn.execute("CREATE INDEX idx_taxonomic_groups_slug ON taxonomic_groups USING BTREE(slug)")
    conn.execute("CREATE INDEX idx_taxonomic_groups_rank_taxon ON taxonomic_groups (taxonrank, taxon) WHERE grouptype IS NOT NULL AND BTRIM(grouptype) <> '-'")
    conn.commit()
    
db.dispose()


