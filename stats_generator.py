# Autor: Diego Moreno-Vargas (github.com/damorenov)
"""
Estadísticas de síntesis: vistas geo, tabla integrada vigente y cifras estimadas por temática.

Cifras estimadas:
- Agregación en PostgreSQL desde tablas taxonomic_* y taxonomic_groups.
- MV estimadas_total creada con MATERIALIZED VIEW (MV).
"""

import argparse
import logging
import os
import sys

from dotenv import load_dotenv

from utils.connection import check_connection, get_db, table_exists

load_dotenv()

logger = logging.getLogger('sintesis_biocifras')

INTEGRATED_PREFIX = 'dwc_integrated_%'
ESTIMADAS_TOTAL_MV = 'estimadas_total'
ESTIMATED_SPECIES_MV_LEGACY = 'estimated_species_totals'
ESTIMATED_SPECIES_STAGING = '_estimated_species_staging'

TAXON_RANKS = ('kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species')
_GROUPS_FILTER = "grouptype IS NOT NULL AND BTRIM(grouptype) <> '-'"

_ESTIMATED_REQUIRED_TABLES = (
    'taxonomic_groups',
    'taxonomic_cites',
    'taxonomic_threat_mads',
    'taxonomic_threat_iucn',
    'taxonomic_invasive_exotic',
    'taxonomic_col_list',
    'taxonomic_migratory',
)


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


def _assert_estimated_tables(db):
    missing = [t for t in _ESTIMATED_REQUIRED_TABLES if not table_exists(db, t)]
    if missing:
        raise ValueError(f'Faltan tablas para cifras estimadas: {", ".join(missing)}')


def get_latest_integrated_table(db):
    with db.connect() as conn:
        rows = conn.execute(
            """
            SELECT table_name
            FROM table_registry
            WHERE is_latest = TRUE
              AND table_name LIKE %(prefix)s
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            {'prefix': INTEGRATED_PREFIX},
        ).fetchall()
    if not rows:
        raise ValueError(
            'No hay tabla integrada con is_latest=TRUE en table_registry.'
        )
    return rows[0][0]


def print_record_count(db, table_name):
    with db.connect() as conn:
        total = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchall()[0][0]
    print(f'Tabla integrada (is_latest): {table_name}')
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


def _sql_rank_counts_union(table: str, thematic_col: str, where_clause: str | None) -> str:
    extra = f'AND ({where_clause})' if where_clause else ''
    parts = []
    for rank in TAXON_RANKS:
        parts.append(f"""
            SELECT "{rank}" AS grupo_tax,
                   '{rank}' AS taxon_rank,
                   "{thematic_col}" AS thematic,
                   COUNT(*)::bigint AS taxones
            FROM "{table}"
            WHERE "{rank}" IS NOT NULL
              AND NULLIF(BTRIM("{thematic_col}"::text), '') IS NOT NULL
              {extra}
            GROUP BY 1, 2, 3
        """)
    return ' UNION ALL '.join(parts)


def _sql_theme_joined_cte(cte_prefix: str, table: str, thematic_col: str, where_clause: str | None) -> str:
    counts = _sql_rank_counts_union(table, thematic_col, where_clause)
    return f"""
{cte_prefix}_raw AS (
    {counts}
),
{cte_prefix}_joined AS (
    SELECT DISTINCT
        r.grupo_tax,
        r.taxon_rank,
        r.thematic,
        r.taxones,
        g.slug AS grupo_id
    FROM {cte_prefix}_raw r
    LEFT JOIN taxonomic_groups g
      ON g.taxon = r.grupo_tax
     AND g.taxonrank = r.taxon_rank
     AND g.{_GROUPS_FILTER}
),
{cte_prefix}_agg AS (
    SELECT grupo_id, thematic, SUM(taxones) AS taxones
    FROM {cte_prefix}_joined
    WHERE grupo_id IS NOT NULL
    GROUP BY grupo_id, thematic
)"""


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_count_as_text(expr: str) -> str:
    """Castea conteos a TEXT; sin valor → NULL (no '-')."""
    return f'({expr})::text'


def _sql_pivot_wide_cte(cte_prefix: str, thematic_map: dict, total_col: str) -> str:
    pivot_cols = ',\n        '.join(
        _sql_count_as_text(
            f'SUM(taxones) FILTER (WHERE thematic = {_sql_quote(thematic)})'
        )
        + f' AS "{out_col}"'
        for thematic, out_col in thematic_map.items()
    )
    return f"""
{cte_prefix}_wide AS (
    SELECT
        grupo_id AS slug_grupo,
        {pivot_cols},
        {_sql_count_as_text('SUM(taxones)')} AS "{total_col}"
    FROM {cte_prefix}_agg
    GROUP BY grupo_id
)"""


def _sql_total_wide_cte(cte_prefix: str, output_col: str) -> str:
    return f"""
{cte_prefix}_wide AS (
    SELECT
        grupo_id AS slug_grupo,
        {_sql_count_as_text('SUM(taxones)')} AS "{output_col}"
    FROM {cte_prefix}_agg
    GROUP BY grupo_id
)"""


def _build_estimated_species_mv_sql() -> str:
    """Consulta única equivalente al script legado (conteos + grupos + pivot + outer join)."""
    pivot_themes = (
        ('cites', 'taxonomic_cites', 'cites', None, {
            'I': 'especies_cites_i_estimadas',
            'II': 'especies_cites_ii_estimadas',
            'I/II': 'especies_cites_i_ii_estimadas',
            'III': 'especies_cites_iii_estimadas',
        }, 'especies_cites_total_estimadas'),
        ('mads', 'taxonomic_threat_mads', 'threatstatus', None, {
            'CR': 'especies_amenazadas_nacional_CR_estimadas',
            'EN': 'especies_amenazadas_nacional_EN_estimadas',
            'VU': 'especies_amenazadas_nacional_VU_estimadas',
        }, 'especies_amenazadas_nacional_total_estimadas'),
        ('iucn', 'taxonomic_threat_iucn', 'threatstatus',
         "threatstatus IN ('VU', 'EN', 'CR')", {
            'CR': 'especies_amenazadas_global_CR_estimadas',
            'EN': 'especies_amenazadas_global_EN_estimadas',
            'VU': 'especies_amenazadas_global_VU_estimadas',
        }, 'especies_amenazadas_global_total_estimadas'),
        ('potencial', 'taxonomic_invasive_exotic', 'exoticriskinvasion', None, {
            'Exótica con potencial de invasión Alto Riesgo': 'especies_potencial_invasion_alto_estimadas',
            'Exótica con potencial de invasión Bajo Riesgo': 'especies_potencial_invasion_bajo_estimadas',
            'Exótica con potencial de invasión Riesgo Moderado': 'especies_potencial_invasion_moderado_estimadas',
            'Exótica con potencial de invasión Riesgo Moderado/ Alto': (
                'especies_potencial_invasion_moderado_alto_estimadas'
            ),
        }, 'especies_potencial_invasion_total_estimadas'),
    )

    total_themes = (
        ('exoticas', 'taxonomic_invasive_exotic', 'exotic', 'especies_exoticas_estimadas'),
        ('invasoras', 'taxonomic_invasive_exotic', 'invasive', 'especies_invasoras_estimadas'),
        ('trasplantadas', 'taxonomic_invasive_exotic', 'transplanted', 'especies_trasplantadas_estimadas'),
        ('endemicas', 'taxonomic_col_list', 'endemic', 'especies_endemicas_estimadas'),
        ('migratorias', 'taxonomic_migratory', 'migratory', 'especies_migratorias_estimadas'),
    )

    wide_ctes = []
    body_parts = []

    for prefix, table, col, where, tmap, total_col in pivot_themes:
        body_parts.append(_sql_theme_joined_cte(prefix, table, col, where))
        body_parts.append(_sql_pivot_wide_cte(prefix, tmap, total_col))
        wide_ctes.append(f'{prefix}_wide')

    for prefix, table, col, out_col in total_themes:
        body_parts.append(_sql_theme_joined_cte(prefix, table, col, None))
        body_parts.append(_sql_total_wide_cte(prefix, out_col))
        wide_ctes.append(f'{prefix}_wide')

    slug_unions = ' UNION '.join(f'SELECT slug_grupo FROM {w}' for w in wide_ctes)

    join_clauses = []
    select_cols = ['s.slug_grupo']
    aliases = {
        'cites': 'c',
        'mads': 'm',
        'iucn': 'i',
        'potencial': 'p',
        'exoticas': 'e',
        'invasoras': 'v',
        'trasplantadas': 't',
        'endemicas': 'n',
        'migratorias': 'g',
    }
    col_by_prefix = {
        'cites': [
            'especies_cites_i_estimadas', 'especies_cites_ii_estimadas',
            'especies_cites_i_ii_estimadas', 'especies_cites_iii_estimadas',
            'especies_cites_total_estimadas',
        ],
        'mads': [
            'especies_amenazadas_nacional_CR_estimadas',
            'especies_amenazadas_nacional_EN_estimadas',
            'especies_amenazadas_nacional_VU_estimadas',
            'especies_amenazadas_nacional_total_estimadas',
        ],
        'iucn': [
            'especies_amenazadas_global_CR_estimadas',
            'especies_amenazadas_global_EN_estimadas',
            'especies_amenazadas_global_VU_estimadas',
            'especies_amenazadas_global_total_estimadas',
        ],
        'potencial': [
            'especies_potencial_invasion_alto_estimadas',
            'especies_potencial_invasion_bajo_estimadas',
            'especies_potencial_invasion_moderado_estimadas',
            'especies_potencial_invasion_moderado_alto_estimadas',
            'especies_potencial_invasion_total_estimadas',
        ],
        'exoticas': ['especies_exoticas_estimadas'],
        'invasoras': ['especies_invasoras_estimadas'],
        'trasplantadas': ['especies_trasplantadas_estimadas'],
        'endemicas': ['especies_endemicas_estimadas'],
        'migratorias': ['especies_migratorias_estimadas'],
    }

    for prefix, alias in aliases.items():
        join_clauses.append(f'LEFT JOIN {prefix}_wide {alias} USING (slug_grupo)')
        for col in col_by_prefix[prefix]:
            select_cols.append(f'{alias}."{col}"')

    return f"""
WITH
{','.join(body_parts)},
all_slugs AS (
    {slug_unions}
)
SELECT
    {', '.join(select_cols)}
FROM all_slugs s
{' '.join(join_clauses)}
"""


def create_estimated_species_materialized_view(db) -> int:
    """Crea o recrea la MV estimadas_total desde SQL (sin pandas ni staging)."""
    _assert_estimated_tables(db)
    sql = _build_estimated_species_mv_sql()

    with db.connect() as conn:
        conn.execute(f'DROP MATERIALIZED VIEW IF EXISTS "{ESTIMADAS_TOTAL_MV}"')
        conn.execute(f'DROP MATERIALIZED VIEW IF EXISTS "{ESTIMATED_SPECIES_MV_LEGACY}"')
        conn.execute(f'DROP TABLE IF EXISTS "{ESTIMATED_SPECIES_STAGING}"')
        conn.execute(f'CREATE MATERIALIZED VIEW "{ESTIMADAS_TOTAL_MV}" AS {sql}')
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
            table_name = get_latest_integrated_table(db)
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
