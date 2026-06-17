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

pathfile = os.getenv("FILE_TAXONOMIC_WORMS")

worms_validation = pd.read_csv(pathfile, sep="\t", low_memory=False)
worms_validation = worms_validation.rename(columns={
    i: i.lower().replace('_', '') for i in list(worms_validation.columns)
})

dropColumns = ['put the names of the columns you wanna suppress here']
for col in list(worms_validation.columns):
    if col in dropColumns:
        worms_validation = worms_validation.drop(labels=[col], axis=1)

engine = sqlal.create_engine(
    "postgresql+psycopg2://"
    + os.getenv("DATABASE_USER") + ":" + os.getenv("DATABASE_PASS") + "@"
    + os.getenv("DATABASE_HOST") + ":" + os.getenv("DATABASE_PORT") + "/"
    + os.getenv("DATABASE_NAME")
)
worms_validation.to_sql("tmp_worms_validation", engine, if_exists='replace', index=False)
engine.dispose()

db = c.get_db()
with db.connect() as conn:
    conn.execute("DROP TABLE IF EXISTS taxonomic_worms")
    conn.execute('''
CREATE TABLE public.taxonomic_worms (
    id serial PRIMARY KEY,
    scientificname text NOT NULL,
    requiredfieldscheck text,
    environmentaphiaworms text,
    nameaphiaworms text,
    aphiaidworms text,
    acceptednameaphiaworms text,
    validaphiaidworms text,
    statusaphiaworms text,
    taxonmatchmatchcountworms text,
    taxonmatchnoteworms text
);
    ''')
    conn.commit()
    cur = conn.cursor()
    cur.execute(
        '''SELECT column_name FROM information_schema.columns '''
        '''WHERE table_name='taxonomic_worms' AND column_name <> 'id' '''
    )
    res = list(cur.fetchall())
    res = [i[0] for i in res]
    if res != list(worms_validation.columns):
        warnings.warn(
            'WoRMS validation: columns from the source file and in the database '
            'do not correspond, the list will not be created in the database'
        )
        conn.execute('DROP TABLE IF EXISTS taxonomic_worms')
        conn.execute('DROP TABLE IF EXISTS tmp_worms_validation')
        conn.commit()
    else:
        query = sql.SQL(
            "INSERT INTO taxonomic_worms ({0}) SELECT {1} FROM tmp_worms_validation"
        ).format(
            sql.SQL(', ').join([sql.Identifier(i) for i in list(worms_validation.columns)]),
            sql.SQL(', ').join([sql.Identifier(i) for i in list(worms_validation.columns)]),
        )
        conn.execute(query)
        conn.execute("DROP TABLE IF EXISTS tmp_worms_validation")
        conn.execute(
            'ALTER TABLE taxonomic_worms RENAME COLUMN scientificname TO species'
        )
        conn.execute(
            "CREATE INDEX idx_taxonomic_worms_species "
            "ON taxonomic_worms USING BTREE(species)"
        )
        conn.commit()

db.dispose()
