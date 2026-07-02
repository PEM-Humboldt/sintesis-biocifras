# Autor: Diego Moreno-Vargas (github.com/damorenov)
"""
Cifras observadas: consultas directas sobre tablas base, salida en MVs.

Por defecto crea/refresca MVs de resultado por nivel y producto.
Con --export-tsv opcionalmente exporta TSV desde las MVs.
"""

import argparse
import csv
import json
import logging
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

from utils.connection import check_connection, get_db, table_exists
from utils.functions import DWC_INTEGRATED_TABLE

load_dotenv()

logger = logging.getLogger('sintesis_biocifras')

PRODUCTS = ('cifras_totales', 'geografia_resumen', 'region_tematica')
CDM_LEVELS = ('CCDM', 'DCDM', 'MCDM')

FLAGTAXO_FILTER = "ts.flagtaxo IS DISTINCT FROM 'Ausente en lista taxonómica'"

LEVELS = {
    'CCDM': {
        'slug_expr': "'colombia'",
        'where_extra': None,
        'marine': True,
        'prefix': 'Nacionales_',
    },
    'CSDM': {
        'slug_expr': "'colombia'",
        'where_extra': None,
        'marine': False,
        'prefix': 'Nacionales_',
    },
    'DCDM': {
        'slug_expr': 'e.dept_slug',
        'where_extra': 'e.dept_slug IS NOT NULL',
        'marine': True,
        'prefix': 'Departamentales_',
    },
    'DSDM': {
        'slug_expr': 'e.dept_slug',
        'where_extra': 'e.dept_slug IS NOT NULL',
        'marine': False,
        'prefix': 'Departamentales_',
    },
    'MCDM': {
        'slug_expr': 'e.muni_slug',
        'where_extra': 'e.muni_slug IS NOT NULL',
        'marine': True,
        'prefix': 'Municipales_',
    },
    'MSDM': {
        'slug_expr': 'e.muni_slug',
        'where_extra': 'e.muni_slug IS NOT NULL',
        'marine': False,
        'prefix': 'Municipales_',
    },
}

THEMATIC_LATERAL = """
    CROSS JOIN LATERAL (VALUES
        ('threatStatus_UICN', ts.threatstatusuicn),
        ('threatStatus_MADS', ts.threatstatusmads),
        ('appendixCITES', ts.cites),
        ('especies_invasoras', ts.invasive),
        ('especies_exoticas', ts.exotic),
        ('especies_exotica_riesgo_invasion', ts.exoticriskinvasion),
        ('especies_trasplantadas', ts.transplanted),
        ('endemic', ts.endemic),
        ('migratory', ts.migratory)
    ) AS th(thematic, category)
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


def _geo_slug_exprs():
    return """
        COALESCE(gl.stateprovinceslug, dept.slug) AS dept_slug,
        COALESCE(gl.countyslug, muni.slug) AS muni_slug
    """


def dwc_integrated_join_sql():
    return f"""
        FROM "{DWC_INTEGRATED_TABLE}" i
        INNER JOIN taxonomic_species_validation ts ON ts.id = i.taxonomic_species_id
        INNER JOIN geo_locality_validation gl ON gl.id = i.locality_id
        LEFT JOIN geo_master_geography gm ON gm.id = gl.geo_master_geography_id
        LEFT JOIN geo_master_geography muni
            ON muni.id = CASE WHEN gm.subtype = 'municipio' THEN gm.id END
        LEFT JOIN geo_master_geography dept
            ON dept.id = COALESCE(
                muni.parent_id,
                CASE WHEN gm.subtype = 'departamento' THEN gm.id END
            )
        WHERE {FLAGTAXO_FILTER}
    """


def enriched_subquery_sql():
    return f"""
        SELECT
            i.gbifid,
            i.locality_id,
            i.publishingorgkey,
            ts.id AS species_id,
            {_geo_slug_exprs().strip()}
        {dwc_integrated_join_sql()}
    """


def enriched_source_from():
    return f"""(
        {enriched_subquery_sql()}
    ) e"""


def _level_where(level_cfg):
    extra = level_cfg.get('where_extra')
    if extra:
        return f'WHERE {extra}'
    return ''


def _is_national_level(cfg):
    return cfg['slug_expr'] == "'colombia'"


def _group_by_geo(cfg, *extra_cols):
    if _is_national_level(cfg):
        if extra_cols:
            return f"GROUP BY {', '.join(extra_cols)}"
        return ''
    parts = [cfg['slug_expr'], *extra_cols]
    return f"GROUP BY {', '.join(parts)}"


def _marine_metrics_sql():
    return """
        COUNT(*) FILTER (WHERE ts.ismarine = 'Marine')::bigint AS registros_marinos,
        COUNT(DISTINCT e.species_id) FILTER (WHERE ts.ismarine = 'Marine')::bigint AS especies_marinas,
        COUNT(*) FILTER (WHERE ts.isbrackish = 'Brackish')::bigint AS registros_salobres,
        COUNT(DISTINCT e.species_id) FILTER (WHERE ts.isbrackish = 'Brackish')::bigint AS especies_salobres,
        COUNT(*) FILTER (WHERE ts.isterrestrial = 'Terrestrial')::bigint AS registros_continentales,
        COUNT(DISTINCT e.species_id) FILTER (WHERE ts.isterrestrial = 'Terrestrial')::bigint AS especies_continentales
    """


def geografia_resumen_sql(level):
    cfg = LEVELS[level]
    slug = cfg['slug_expr']
    where_clause = _level_where(cfg)
    marine_select = f', {_marine_metrics_sql()}' if cfg['marine'] else ''

    return f"""
        SELECT
            {slug} AS slug_region,
            COUNT(*)::bigint AS registros,
            COUNT(DISTINCT e.species_id)::bigint AS especies{marine_select}
        FROM {enriched_source_from()}
        INNER JOIN taxonomic_species_validation ts ON ts.id = e.species_id
        {where_clause}
        {_group_by_geo(cfg)}
        ORDER BY slug_region
    """


def cifras_totales_sql(level):
    cfg = LEVELS[level]
    where_clause = _level_where(cfg)
    marine_select = f', {_marine_metrics_sql()}' if cfg['marine'] else ''

    return f"""
        SELECT
            COUNT(*)::bigint AS registros,
            COUNT(DISTINCT e.species_id)::bigint AS especies{marine_select}
        FROM {enriched_source_from()}
        INNER JOIN taxonomic_species_validation ts ON ts.id = e.species_id
        {where_clause}
    """


def region_tematica_sql(level):
    cfg = LEVELS[level]
    slug = cfg['slug_expr']
    where_parts = []
    if cfg.get('where_extra'):
        where_parts.append(cfg['where_extra'])
    where_parts.append("NULLIF(BTRIM(th.category::text), '') IS NOT NULL")
    where_clause = 'WHERE ' + ' AND '.join(where_parts)
    marine_select = f', {_marine_metrics_sql()}' if cfg['marine'] else ''

    return f"""
        SELECT
            {slug} AS slug_region,
            th.thematic,
            th.category,
            COUNT(*)::bigint AS registros,
            COUNT(DISTINCT e.species_id)::bigint AS especies{marine_select}
        FROM {enriched_source_from()}
        INNER JOIN taxonomic_species_validation ts ON ts.id = e.species_id
        {THEMATIC_LATERAL}
        {where_clause}
        {_group_by_geo(cfg, 'th.thematic', 'th.category')}
        ORDER BY slug_region, thematic, category
    """


def product_query_sql(level, product):
    builders = {
        'cifras_totales': cifras_totales_sql,
        'geografia_resumen': geografia_resumen_sql,
        'region_tematica': region_tematica_sql,
    }
    return builders[product](level)


def mv_name(level, product):
    return f'stats_{product}_{level.lower()}'


def create_product_mv_sql(level, product):
    name = mv_name(level, product)
    body = product_query_sql(level, product)
    index_sql = ''
    if product != 'cifras_totales':
        index_sql = f'CREATE INDEX IF NOT EXISTS idx_{name}_region ON "{name}" (slug_region)'
    return f"""
        DROP MATERIALIZED VIEW IF EXISTS "{name}" CASCADE;
        CREATE MATERIALIZED VIEW "{name}" AS
        {body};
        {index_sql}
    """


def mv_select_sql(level, product):
    """SELECT ordenado para exportar TSV desde la MV."""
    name = mv_name(level, product)
    if product == 'cifras_totales':
        return f'SELECT * FROM "{name}"'
    if product == 'geografia_resumen':
        return f'SELECT * FROM "{name}" ORDER BY slug_region'
    return f'SELECT * FROM "{name}" ORDER BY slug_region, thematic, category'


def execute_script(conn, sql):
    for stmt in (s.strip() for s in sql.split(';')):
        if stmt:
            conn.execute(stmt)
    conn.commit()


def check_prerequisites(db):
    required = (
        DWC_INTEGRATED_TABLE,
        'taxonomic_species_validation',
        'geo_locality_validation',
        'geo_master_geography',
    )
    missing = [name for name in required if not table_exists(db, name)]
    if missing:
        raise ValueError(f'Faltan tablas requeridas: {", ".join(missing)}')
    with db.connect() as conn:
        linked = conn.execute(
            f"""
            SELECT COUNT(*) FROM "{DWC_INTEGRATED_TABLE}"
            WHERE taxonomic_species_id IS NOT NULL AND locality_id IS NOT NULL
            """
        ).fetchall()[0][0]
    if linked == 0:
        raise ValueError(
            'La tabla integrada no tiene enlaces taxonomic_species_id/locality_id. '
            'Ejecute main.py antes de generar cifras.'
        )
    logger.info('Registros enlazados en integrada: %s', f'{linked:,}')
    return linked


def export_query_to_tsv(conn, sql, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        headers = [desc[0] for desc in cur.description] if cur.description else []
    with path.open('w', encoding='utf-8', newline='') as fh:
        writer = csv.writer(fh, delimiter='\t', lineterminator='\n')
        if headers:
            writer.writerow(headers)
        writer.writerows(rows)
    logger.info('Exportado %s (%s filas)', path, f'{len(rows):,}')
    return len(rows)


def create_or_refresh_product_mv(conn, level, product, refresh_only, timing):
    name = mv_name(level, product)
    t0 = time.perf_counter()
    if refresh_only:
        conn.execute(f'REFRESH MATERIALIZED VIEW "{name}"')
        conn.commit()
    else:
        execute_script(conn, create_product_mv_sql(level, product))
    total = conn.execute(f'SELECT COUNT(*) FROM "{name}"').fetchall()[0][0]
    timing[f'{level}_{product}_mv'] = {
        'seconds': round(time.perf_counter() - t0, 3),
        'rows': total,
        'mv': name,
    }
    logger.info('MV %s lista (%s filas)', name, f'{total:,}')


def export_product_tsv(conn, level, product, output_dir, timing):
    prefix = LEVELS[level]['prefix']
    level_dir = Path(output_dir) / level.lower()
    t0 = time.perf_counter()
    rows = export_query_to_tsv(
        conn,
        mv_select_sql(level, product),
        level_dir / f'{prefix}{product}.tsv',
    )
    timing[f'{level}_{product}_tsv'] = {
        'seconds': round(time.perf_counter() - t0, 3),
        'rows': rows,
    }


def parse_args():
    parser = argparse.ArgumentParser(
        description='Cifras observadas: consultas directas, salida en MVs PostgreSQL'
    )
    parser.add_argument(
        '--levels',
        nargs='+',
        choices=list(LEVELS.keys()),
        default=list(CDM_LEVELS),
        help='Niveles a generar (por defecto CCDM DCDM MCDM)',
    )
    parser.add_argument(
        '--products',
        nargs='+',
        choices=list(PRODUCTS),
        default=list(PRODUCTS),
        help='Productos a generar',
    )
    parser.add_argument(
        '--export-tsv',
        action='store_true',
        help='Exportar TSV desde las MVs creadas',
    )
    parser.add_argument(
        '--output-dir',
        default='output/stats_observadas',
        help='Directorio de salida TSV (solo con --export-tsv)',
    )
    parser.add_argument(
        '--refresh-only',
        action='store_true',
        help='REFRESH MATERIALIZED VIEW en lugar de DROP/CREATE',
    )
    return parser.parse_args()


def main():
    setup_console_logger()
    args = parse_args()
    db = get_db()

    if not check_connection(db):
        logger.error('No se pudo conectar a la base de datos.')
        sys.exit(1)

    timing = {'levels': args.levels, 'products': args.products}
    try:
        t0 = time.perf_counter()
        check_prerequisites(db)
        timing['prerequisites_seconds'] = round(time.perf_counter() - t0, 3)

        with db.connect() as conn:
            for level in args.levels:
                logger.info('Generando nivel %s', level)
                for product in args.products:
                    create_or_refresh_product_mv(
                        conn, level, product, args.refresh_only, timing
                    )
                    if args.export_tsv:
                        export_product_tsv(conn, level, product, args.output_dir, timing)

        if args.export_tsv:
            timing_path = Path(args.output_dir) / '_timing.json'
            timing_path.parent.mkdir(parents=True, exist_ok=True)
            with timing_path.open('w', encoding='utf-8') as fh:
                json.dump(timing, fh, indent=2, ensure_ascii=False)
            logger.info('Tiempos guardados en %s', timing_path)

        logger.info('Proceso completado')

    except ValueError as exc:
        logger.error('%s', exc)
        sys.exit(1)
    except Exception as exc:
        logger.error('Error al generar cifras: %s', exc, exc_info=True)
        sys.exit(1)
    finally:
        db.dispose()


if __name__ == '__main__':
    main()
