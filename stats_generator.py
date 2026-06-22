# Autor: Diego Moreno-Vargas (github.com/damorenov)
"""
Estadísticas de síntesis: vistas geo, tabla integrada vigente y cifras estimadas por temática.

Cifras estimadas:
- MV taxonomic_estimated_source: unión de taxonomic_col_list, taxonomic_cites, taxonomic_threat_mads, taxonomic_threat_iucn, taxonomic_invasive_exotic y taxonomic_migratory por species y JOIN temático por taxonomía para tener una única vista con todas las taxonomías y temáticas.
- MV estimadas_total: doble LATERAL (rangos × temáticas) + JOIN taxonomic_groups + pivot FILTER para obtener las cifras estimadas por temática y rango taxonómico.
"""

import argparse
import logging
import os
import sys

from dotenv import load_dotenv

from utils.connection import check_connection, get_db, table_exists
from utils.functions import DWC_INTEGRATED_TABLE

load_dotenv()

logger = logging.getLogger('sintesis_biocifras')

ESTIMADAS_TOTAL_MV = 'estimadas_total'
ESTIMATED_SPECIES_MV_LEGACY = 'estimated_species_totals'
TAXONOMIC_ESTIMATED_SOURCE_MV = 'taxonomic_estimated_source'

# SQL para crear la vista taxonomic_estimated_source con todas las taxonomías y temáticas.
_ESTIMATED_SOURCE_MV_SQL = """
    WITH all_species AS (
        SELECT species FROM taxonomic_col_list
        UNION
        SELECT species FROM taxonomic_cites
        UNION
        SELECT species FROM taxonomic_threat_mads
        UNION
        SELECT species FROM taxonomic_threat_iucn
        UNION
        SELECT species FROM taxonomic_invasive_exotic
        UNION
        SELECT species FROM taxonomic_migratory
    )
    SELECT
        s.species,
        COALESCE(c.kingdom,  ci.kingdom,  m.kingdom,  i.kingdom,  e.kingdom,  mig.kingdom)  AS kingdom,
        COALESCE(c.phylum,   ci.phylum,   m.phylum,   i.phylum,   e.phylum,   mig.phylum)   AS phylum,
        COALESCE(c."class",  ci."class",  m."class",  i."class",  e."class",  mig."class")  AS "class",
        COALESCE(c."order",  ci."order",  m."order",  i."order",  e."order",  mig."order")  AS "order",
        COALESCE(c.family,   ci.family,   m.family,   i.family,   e.family,   mig.family)   AS family,
        COALESCE(c.genus,    ci.genus,    m.genus,    i.genus,    e.genus,    mig.genus)    AS genus,
        ci.cites,
        m.threatstatus AS threatstatus_mads,
        i.threatstatus AS threatstatus_iucn,
        e.exotic,
        e.exoticriskinvasion,
        e.invasive,
        e.transplanted,
        COALESCE(c.migratory, mig.migratory) AS migratory,
        c.endemic
    FROM all_species s
    LEFT JOIN (
        SELECT DISTINCT ON (species) * FROM taxonomic_col_list ORDER BY species, id
    ) c ON c.species = s.species
    LEFT JOIN (
        SELECT DISTINCT ON (species) * FROM taxonomic_cites ORDER BY species, id
    ) ci ON ci.species = s.species
    LEFT JOIN (
        SELECT DISTINCT ON (species) * FROM taxonomic_threat_mads ORDER BY species, id
    ) m ON m.species = s.species
    LEFT JOIN (
        SELECT DISTINCT ON (species) * FROM taxonomic_threat_iucn ORDER BY species, id
    ) i ON i.species = s.species
    LEFT JOIN (
        SELECT DISTINCT ON (species) * FROM taxonomic_invasive_exotic ORDER BY species, id
    ) e ON e.species = s.species
    LEFT JOIN (
        SELECT DISTINCT ON (species) * FROM taxonomic_migratory ORDER BY species, id
    ) mig ON mig.species = s.species
"""
# SQL para crear la vista estimadas_total con las cifras estimadas por temática y rango taxonómico.
_ESTIMADAS_TOTAL_MV_SQL = f"""
    WITH counts_by_taxon_rank_and_theme AS (
        SELECT
            th.theme,
            r.taxon_rank,
            r.grupo_tax,
            th.thematic,
            COUNT(*)::int AS taxones
        FROM "{TAXONOMIC_ESTIMATED_SOURCE_MV}" t
        CROSS JOIN LATERAL (VALUES
            ('kingdom', t.kingdom), ('phylum', t.phylum), ('class', t."class"),
            ('order', t."order"), ('family', t.family), ('genus', t.genus),
            ('species', t.species)
        ) AS r(taxon_rank, grupo_tax)
        CROSS JOIN LATERAL (VALUES
            ('cites', t.cites),
            ('mads', t.threatstatus_mads),
            ('iucn', t.threatstatus_iucn),
            ('potencial', t.exoticriskinvasion),
            ('exoticas', t.exotic),
            ('invasoras', t.invasive),
            ('trasplantadas', t.transplanted),
            ('endemicas', t.endemic),
            ('migratorias', t.migratory)
        ) AS th(theme, thematic)
        WHERE r.grupo_tax IS NOT NULL
          AND NULLIF(BTRIM(th.thematic::text), '') IS NOT NULL
          AND (th.theme <> 'iucn' OR th.thematic IN ('VU', 'EN', 'CR'))
        GROUP BY th.theme, r.taxon_rank, r.grupo_tax, th.thematic
    ),
    joined AS (
        SELECT g.slug AS slug_grupo, l.theme, l.thematic, SUM(l.taxones) AS taxones
        FROM counts_by_taxon_rank_and_theme l
        JOIN taxonomic_groups g
          ON g.taxon = l.grupo_tax
         AND g.taxonrank = l.taxon_rank
         AND g.grouptype IS NOT NULL AND BTRIM(g.grouptype) <> '-'
        GROUP BY 1, 2, 3
    )
    SELECT
        s.slug_grupo,
        (SUM(j.taxones) FILTER (WHERE j.theme = 'cites' AND j.thematic = 'I'))::text
            AS "especies_cites_i_estimadas",
        (SUM(j.taxones) FILTER (WHERE j.theme = 'cites' AND j.thematic = 'II'))::text
            AS "especies_cites_ii_estimadas",
        (SUM(j.taxones) FILTER (WHERE j.theme = 'cites' AND j.thematic = 'I/II'))::text
            AS "especies_cites_i_ii_estimadas",
        (SUM(j.taxones) FILTER (WHERE j.theme = 'cites' AND j.thematic = 'III'))::text
            AS "especies_cites_iii_estimadas",
        (SUM(j.taxones) FILTER (WHERE j.theme = 'cites'))::text
            AS "especies_cites_total_estimadas",
        (SUM(j.taxones) FILTER (WHERE j.theme = 'mads' AND j.thematic = 'CR'))::text
            AS "especies_amenazadas_nacional_CR_estimadas",
        (SUM(j.taxones) FILTER (WHERE j.theme = 'mads' AND j.thematic = 'EN'))::text
            AS "especies_amenazadas_nacional_EN_estimadas",
        (SUM(j.taxones) FILTER (WHERE j.theme = 'mads' AND j.thematic = 'VU'))::text
            AS "especies_amenazadas_nacional_VU_estimadas",
        (SUM(j.taxones) FILTER (WHERE j.theme = 'mads'))::text
            AS "especies_amenazadas_nacional_total_estimadas",
        (SUM(j.taxones) FILTER (WHERE j.theme = 'iucn' AND j.thematic = 'CR'))::text
            AS "especies_amenazadas_global_CR_estimadas",
        (SUM(j.taxones) FILTER (WHERE j.theme = 'iucn' AND j.thematic = 'EN'))::text
            AS "especies_amenazadas_global_EN_estimadas",
        (SUM(j.taxones) FILTER (WHERE j.theme = 'iucn' AND j.thematic = 'VU'))::text
            AS "especies_amenazadas_global_VU_estimadas",
        (SUM(j.taxones) FILTER (WHERE j.theme = 'iucn'))::text
            AS "especies_amenazadas_global_total_estimadas",
        (SUM(j.taxones) FILTER (WHERE j.theme = 'potencial'
            AND j.thematic = 'Exótica con potencial de invasión Alto Riesgo'))::text
            AS "especies_potencial_invasion_alto_estimadas",
        (SUM(j.taxones) FILTER (WHERE j.theme = 'potencial'
            AND j.thematic = 'Exótica con potencial de invasión Bajo Riesgo'))::text
            AS "especies_potencial_invasion_bajo_estimadas",
        (SUM(j.taxones) FILTER (WHERE j.theme = 'potencial'
            AND j.thematic = 'Exótica con potencial de invasión Riesgo Moderado'))::text
            AS "especies_potencial_invasion_moderado_estimadas",
        (SUM(j.taxones) FILTER (WHERE j.theme = 'potencial'
            AND j.thematic = 'Exótica con potencial de invasión Riesgo Moderado/ Alto'))::text
            AS "especies_potencial_invasion_moderado_alto_estimadas",
        (SUM(j.taxones) FILTER (WHERE j.theme = 'potencial'))::text
            AS "especies_potencial_invasion_total_estimadas",
        (SUM(j.taxones) FILTER (WHERE j.theme = 'exoticas'))::text
            AS "especies_exoticas_estimadas",
        (SUM(j.taxones) FILTER (WHERE j.theme = 'invasoras'))::text
            AS "especies_invasoras_estimadas",
        (SUM(j.taxones) FILTER (WHERE j.theme = 'trasplantadas'))::text
            AS "especies_trasplantadas_estimadas",
        (SUM(j.taxones) FILTER (WHERE j.theme = 'endemicas'))::text
            AS "especies_endemicas_estimadas",
        (SUM(j.taxones) FILTER (WHERE j.theme = 'migratorias'))::text
            AS "especies_migratorias_estimadas"
    FROM (
        SELECT DISTINCT slug AS slug_grupo
        FROM taxonomic_groups
        WHERE grouptype IS NOT NULL AND BTRIM(grouptype) <> '-'
    ) s
    LEFT JOIN joined j USING (slug_grupo)
    GROUP BY s.slug_grupo
"""

def setup_console_logger():
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    )
    logger.addHandler(handler)
    return logger


def get_integrated_table(db):
    if not table_exists(db, DWC_INTEGRATED_TABLE):
        raise ValueError(f'La tabla integrada {DWC_INTEGRATED_TABLE} no existe en la base de datos.')
    return DWC_INTEGRATED_TABLE


def print_record_count(db, table_name):
    with db.connect() as conn:
        total = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchall()[0][0]
    print(f'Tabla integrada: {table_name}')
    print(f'Registros: {total:,}')
    return total


def create_region_materialized_view(db):
    if not table_exists(db, 'geo_master_geography'):
        raise ValueError('La tabla geo_master_geography no existe en la base de datos.')
    with db.connect() as conn:
        conn.execute('DROP MATERIALIZED VIEW IF EXISTS region')
        conn.execute("""
            CREATE MATERIALIZED VIEW region AS
            SELECT
                COALESCE(p.slug, '0') AS parent,
                d.slug,
                d."name" AS label,
                d."type" AS tipo,
                d.subtype AS subtipo,
                d.description AS descripcion,
                d.ismarine AS marino
            FROM geo_master_geography d
            LEFT JOIN geo_master_geography p ON p.id = d.parent_id

            UNION ALL

            SELECT
                r.slug AS parent,
                d.slug,
                d."name" AS label,
                d."type" AS tipo,
                d.subtype AS subtipo,
                d.description AS descripcion,
                d.ismarine AS marino
            FROM geo_master_geography d
            JOIN geo_master_geography r ON r.id = d.region_id
            ORDER BY parent
        """)
        conn.commit()
        total = conn.execute('SELECT COUNT(*) FROM region').fetchall()[0][0]
    logger.info('Vista materializada region creada (%s filas)', total)
    print(f'Vista materializada region: {total:,} filas')
    return total


def create_departamento_materialized_view(db):
    if not table_exists(db, 'geo_master_geography'):
        raise ValueError('La tabla geo_master_geography no existe en la base de datos.')
    with db.connect() as conn:
        conn.execute('DROP MATERIALIZED VIEW IF EXISTS departamento')
        conn.execute("""
            CREATE MATERIALIZED VIEW departamento AS
            SELECT
                d.slug,
                d."name" AS label,
                d.codedane AS cod_dane,
                d.ismarine AS marino,
                d."date" AS fecha_corte
            FROM geo_master_geography d
            WHERE d.subtype = 'departamento'
            ORDER BY slug
        """)
        conn.commit()
        total = conn.execute('SELECT COUNT(*) FROM departamento').fetchall()[0][0]
    logger.info('Vista materializada departamento creada (%s filas)', total)
    print(f'Vista materializada departamento: {total:,} filas')
    return total


def create_municipio_materialized_view(db):
    if not table_exists(db, 'geo_master_geography'):
        raise ValueError('La tabla geo_master_geography no existe en la base de datos.')
    with db.connect() as conn:
        conn.execute('DROP MATERIALIZED VIEW IF EXISTS municipio')
        conn.execute("""
            CREATE MATERIALIZED VIEW municipio AS
            SELECT
                d.slug,
                d."name" AS label,
                d.codedane AS cod_dane,
                d.ismarine AS marino,
                d."date" AS fecha_corte
            FROM geo_master_geography d
            WHERE d.subtype = 'municipio'
            ORDER BY slug
        """)
        conn.commit()
        total = conn.execute('SELECT COUNT(*) FROM municipio').fetchall()[0][0]
    logger.info('Vista materializada municipio creada (%s filas)', total)
    print(f'Vista materializada municipio: {total:,} filas')
    return total


def create_estimated_species_materialized_view(db) -> int:
    """Crea taxonomic_estimated_source y estimadas_total (doble LATERAL + pivot FILTER)."""

    with db.connect() as conn:
        conn.execute(f'DROP MATERIALIZED VIEW IF EXISTS "{ESTIMADAS_TOTAL_MV}"')
        conn.execute(f'DROP MATERIALIZED VIEW IF EXISTS "{ESTIMATED_SPECIES_MV_LEGACY}"')
        conn.execute(f'DROP MATERIALIZED VIEW IF EXISTS "{TAXONOMIC_ESTIMATED_SOURCE_MV}"')
        conn.execute('DROP TABLE IF EXISTS "_estimated_species_staging"')

        conn.execute(
            f'CREATE MATERIALIZED VIEW "{TAXONOMIC_ESTIMATED_SOURCE_MV}" AS {_ESTIMATED_SOURCE_MV_SQL}'
        )
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_taxonomic_estimated_source_species '
            f'ON "{TAXONOMIC_ESTIMATED_SOURCE_MV}" (species)'
        )

        source_total = conn.execute(
            f'SELECT COUNT(*) FROM "{TAXONOMIC_ESTIMATED_SOURCE_MV}"'
        ).fetchall()[0][0]
        logger.info('MV %s creada (%s filas)', TAXONOMIC_ESTIMATED_SOURCE_MV, source_total)

        conn.execute(f'CREATE MATERIALIZED VIEW "{ESTIMADAS_TOTAL_MV}" AS {_ESTIMADAS_TOTAL_MV_SQL}')
        conn.commit()
        total = conn.execute(f'SELECT COUNT(*) FROM "{ESTIMADAS_TOTAL_MV}"').fetchall()[0][0]

    logger.info('MV %s creada (%s filas)', ESTIMADAS_TOTAL_MV, total)
    print(f'Vista materializada {TAXONOMIC_ESTIMATED_SOURCE_MV}: {source_total:,} filas')
    print(f'Vista materializada {ESTIMADAS_TOTAL_MV}: {total:,} filas')
    return total


def parse_args():
    parser = argparse.ArgumentParser(description='Generador de estadísticas y cifras estimadas')
    parser.add_argument('--skip-geo-views', action='store_true', help='Omitir MV geo')
    parser.add_argument('--skip-estimated', action='store_true', help='Omitir cifras estimadas')
    return parser.parse_args()


def main():
    setup_console_logger()
    args = parse_args()
    db = get_db()

    if not check_connection(db):
        logger.error(
            'No se pudo conectar a la base de datos. Verifique los valores de conexión en .env'
        )
        sys.exit(1)

    try:
        if not args.skip_geo_views:
            create_region_materialized_view(db)
            create_departamento_materialized_view(db)
            create_municipio_materialized_view(db)

        try:
            table_name = get_integrated_table(db)
            print_record_count(db, table_name)
        except ValueError as e:
            logger.warning('Tabla integrada: %s', e)

        if not args.skip_estimated:
            create_estimated_species_materialized_view(db)

    except ValueError as e:
        logger.error('%s', e)
        sys.exit(1)
    except Exception as e:
        logger.error('Error al generar estadísticas: %s', e, exc_info=True)
        sys.exit(1)
    finally:
        db.dispose()


if __name__ == '__main__':
    main()
