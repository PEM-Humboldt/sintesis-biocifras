# Autor: Diego Moreno-Vargas (github.com/damorenov)
"""
Estadísticas de síntesis: vistas geo, tabla integrada vigente y cifras estimadas por temática.

Cifras estimadas:
- Staging largo (_estimated_species_staging) con unpivot LATERAL por temática.
- MV estimadas_total: JOIN taxonomic_groups, pivot FILTER, eje = todos los slug de grupos.
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
ESTIMATED_SPECIES_STAGING = '_estimated_species_staging'

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
    """Crea estimadas_total: staging largo + JOIN grupos + pivot FILTER; eje = taxonomic_groups."""

    mv_sql = f"""
        WITH joined AS (
            SELECT g.slug AS slug_grupo, l.theme, l.thematic, SUM(l.taxones) AS taxones
            FROM "{ESTIMATED_SPECIES_STAGING}" l
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

    with db.connect() as conn:
        conn.execute(f'DROP MATERIALIZED VIEW IF EXISTS "{ESTIMADAS_TOTAL_MV}"')
        conn.execute(f'DROP MATERIALIZED VIEW IF EXISTS "{ESTIMATED_SPECIES_MV_LEGACY}"')
        conn.execute(f'DROP TABLE IF EXISTS "{ESTIMATED_SPECIES_STAGING}"')
        conn.execute(f"""
            CREATE UNLOGGED TABLE "{ESTIMATED_SPECIES_STAGING}" (
                theme text NOT NULL,
                taxon_rank text NOT NULL,
                grupo_tax text NOT NULL,
                thematic text NOT NULL,
                taxones bigint NOT NULL
            )
        """)

        conn.execute(f"""
            INSERT INTO "{ESTIMATED_SPECIES_STAGING}" (theme, taxon_rank, grupo_tax, thematic, taxones)
            SELECT 'cites', r.taxon_rank, r.grupo_tax, t."cites" AS thematic, COUNT(*)::bigint
            FROM "taxonomic_cites" t
            CROSS JOIN LATERAL (VALUES
                ('kingdom', t."kingdom"), ('phylum', t."phylum"), ('class', t."class"),
                ('order', t."order"), ('family', t."family"), ('genus', t."genus"),
                ('species', t."species")
            ) AS r(taxon_rank, grupo_tax)
            WHERE r.grupo_tax IS NOT NULL
              AND NULLIF(BTRIM(t."cites"::text), '') IS NOT NULL
            GROUP BY r.taxon_rank, r.grupo_tax, t."cites"
        """)
        conn.execute(f"""
            INSERT INTO "{ESTIMATED_SPECIES_STAGING}" (theme, taxon_rank, grupo_tax, thematic, taxones)
            SELECT 'mads', r.taxon_rank, r.grupo_tax, t."threatstatus" AS thematic, COUNT(*)::bigint
            FROM "taxonomic_threat_mads" t
            CROSS JOIN LATERAL (VALUES
                ('kingdom', t."kingdom"), ('phylum', t."phylum"), ('class', t."class"),
                ('order', t."order"), ('family', t."family"), ('genus', t."genus"),
                ('species', t."species")
            ) AS r(taxon_rank, grupo_tax)
            WHERE r.grupo_tax IS NOT NULL
              AND NULLIF(BTRIM(t."threatstatus"::text), '') IS NOT NULL
            GROUP BY r.taxon_rank, r.grupo_tax, t."threatstatus"
        """)
        conn.execute(f"""
            INSERT INTO "{ESTIMATED_SPECIES_STAGING}" (theme, taxon_rank, grupo_tax, thematic, taxones)
            SELECT 'iucn', r.taxon_rank, r.grupo_tax, t."threatstatus" AS thematic, COUNT(*)::bigint
            FROM "taxonomic_threat_iucn" t
            CROSS JOIN LATERAL (VALUES
                ('kingdom', t."kingdom"), ('phylum', t."phylum"), ('class', t."class"),
                ('order', t."order"), ('family', t."family"), ('genus', t."genus"),
                ('species', t."species")
            ) AS r(taxon_rank, grupo_tax)
            WHERE r.grupo_tax IS NOT NULL
              AND NULLIF(BTRIM(t."threatstatus"::text), '') IS NOT NULL
              AND (threatstatus IN ('VU', 'EN', 'CR'))
            GROUP BY r.taxon_rank, r.grupo_tax, t."threatstatus"
        """)
        conn.execute(f"""
            INSERT INTO "{ESTIMATED_SPECIES_STAGING}" (theme, taxon_rank, grupo_tax, thematic, taxones)
            SELECT 'potencial', r.taxon_rank, r.grupo_tax, t."exoticriskinvasion" AS thematic, COUNT(*)::bigint
            FROM "taxonomic_invasive_exotic" t
            CROSS JOIN LATERAL (VALUES
                ('kingdom', t."kingdom"), ('phylum', t."phylum"), ('class', t."class"),
                ('order', t."order"), ('family', t."family"), ('genus', t."genus"),
                ('species', t."species")
            ) AS r(taxon_rank, grupo_tax)
            WHERE r.grupo_tax IS NOT NULL
              AND NULLIF(BTRIM(t."exoticriskinvasion"::text), '') IS NOT NULL
            GROUP BY r.taxon_rank, r.grupo_tax, t."exoticriskinvasion"
        """)
        conn.execute(f"""
            INSERT INTO "{ESTIMATED_SPECIES_STAGING}" (theme, taxon_rank, grupo_tax, thematic, taxones)
            SELECT 'exoticas', r.taxon_rank, r.grupo_tax, t."exotic" AS thematic, COUNT(*)::bigint
            FROM "taxonomic_invasive_exotic" t
            CROSS JOIN LATERAL (VALUES
                ('kingdom', t."kingdom"), ('phylum', t."phylum"), ('class', t."class"),
                ('order', t."order"), ('family', t."family"), ('genus', t."genus"),
                ('species', t."species")
            ) AS r(taxon_rank, grupo_tax)
            WHERE r.grupo_tax IS NOT NULL
              AND NULLIF(BTRIM(t."exotic"::text), '') IS NOT NULL
            GROUP BY r.taxon_rank, r.grupo_tax, t."exotic"
        """)
        conn.execute(f"""
            INSERT INTO "{ESTIMATED_SPECIES_STAGING}" (theme, taxon_rank, grupo_tax, thematic, taxones)
            SELECT 'invasoras', r.taxon_rank, r.grupo_tax, t."invasive" AS thematic, COUNT(*)::bigint
            FROM "taxonomic_invasive_exotic" t
            CROSS JOIN LATERAL (VALUES
                ('kingdom', t."kingdom"), ('phylum', t."phylum"), ('class', t."class"),
                ('order', t."order"), ('family', t."family"), ('genus', t."genus"),
                ('species', t."species")
            ) AS r(taxon_rank, grupo_tax)
            WHERE r.grupo_tax IS NOT NULL
              AND NULLIF(BTRIM(t."invasive"::text), '') IS NOT NULL
            GROUP BY r.taxon_rank, r.grupo_tax, t."invasive"
        """)
        conn.execute(f"""
            INSERT INTO "{ESTIMATED_SPECIES_STAGING}" (theme, taxon_rank, grupo_tax, thematic, taxones)
            SELECT 'trasplantadas', r.taxon_rank, r.grupo_tax, t."transplanted" AS thematic, COUNT(*)::bigint
            FROM "taxonomic_invasive_exotic" t
            CROSS JOIN LATERAL (VALUES
                ('kingdom', t."kingdom"), ('phylum', t."phylum"), ('class', t."class"),
                ('order', t."order"), ('family', t."family"), ('genus', t."genus"),
                ('species', t."species")
            ) AS r(taxon_rank, grupo_tax)
            WHERE r.grupo_tax IS NOT NULL
              AND NULLIF(BTRIM(t."transplanted"::text), '') IS NOT NULL
            GROUP BY r.taxon_rank, r.grupo_tax, t."transplanted"
        """)
        conn.execute(f"""
            INSERT INTO "{ESTIMATED_SPECIES_STAGING}" (theme, taxon_rank, grupo_tax, thematic, taxones)
            SELECT 'endemicas', r.taxon_rank, r.grupo_tax, t."endemic" AS thematic, COUNT(*)::bigint
            FROM "taxonomic_col_list" t
            CROSS JOIN LATERAL (VALUES
                ('kingdom', t."kingdom"), ('phylum', t."phylum"), ('class', t."class"),
                ('order', t."order"), ('family', t."family"), ('genus', t."genus"),
                ('species', t."species")
            ) AS r(taxon_rank, grupo_tax)
            WHERE r.grupo_tax IS NOT NULL
              AND NULLIF(BTRIM(t."endemic"::text), '') IS NOT NULL
            GROUP BY r.taxon_rank, r.grupo_tax, t."endemic"
        """)
        conn.execute(f"""
            INSERT INTO "{ESTIMATED_SPECIES_STAGING}" (theme, taxon_rank, grupo_tax, thematic, taxones)
            SELECT 'migratorias', r.taxon_rank, r.grupo_tax, t."migratory" AS thematic, COUNT(*)::bigint
            FROM "taxonomic_migratory" t
            CROSS JOIN LATERAL (VALUES
                ('kingdom', t."kingdom"), ('phylum', t."phylum"), ('class', t."class"),
                ('order', t."order"), ('family', t."family"), ('genus', t."genus"),
                ('species', t."species")
            ) AS r(taxon_rank, grupo_tax)
            WHERE r.grupo_tax IS NOT NULL
              AND NULLIF(BTRIM(t."migratory"::text), '') IS NOT NULL
            GROUP BY r.taxon_rank, r.grupo_tax, t."migratory"
        """)

        conn.execute(f'CREATE MATERIALIZED VIEW "{ESTIMADAS_TOTAL_MV}" AS {mv_sql}')
        conn.execute(f'DROP TABLE IF EXISTS "{ESTIMATED_SPECIES_STAGING}"')
        conn.commit()
        total = conn.execute(f'SELECT COUNT(*) FROM "{ESTIMADAS_TOTAL_MV}"').fetchall()[0][0]

    logger.info('MV %s creada (%s filas)', ESTIMADAS_TOTAL_MV, total)
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
