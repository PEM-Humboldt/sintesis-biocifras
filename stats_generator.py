# Autor: Diego Moreno-Vargas (github.com/damorenov)
"""
Estadísticas de síntesis: vistas geo, catálogo de especies, tabla integrada vigente y cifras estimadas por temática.

- MV especie: catálogo taxonómico desde taxonomic_species_validation (slug + rangos).
- MV especie_meta: metadatos de especie (vernacular, URLs) desde taxonomic_species_meta por slug.
- MV especie_grupo: relación especie ↔ grupo biológico/interés desde taxonomic_groups.
- MV especie_region: registros por especie y región (nacional, depto, muni, amazonía, reservas, resguardos, núcleos DFYB).
- MV especie_tematica: relación DISTINCT especie ↔ región ↔ temática (nacional, depto, muni, amazonía, reservas, resguardos, núcleos DFYB).
- MV cifras_totales: conteos globales por nivel CDM (registros/especies y hábitat en niveles marinos).
- MV geografia_resumen: conteos por nivel CDM y slug_region (departamentos o municipios).
- MV staging_agg_taxon_region: agregados por rango taxonómico y región (solo para region_grupo).
- MV region_tematica: cifras geográficas anchas por slug_region (nacional, depto, muni, amazonía, reservas, resguardos, núcleos DFYB); incluye especies_region_estimadas y estimada_region_ref_id desde tmp_cifras_estimated_dept.
- MV region_grupo: cifras anchas por slug_grupo y slug_region (nacional, depto, muni, amazonía, reservas, resguardos, núcleos DFYB).
- MV publicador: catálogo de publicadores con registros (integrada total) y especies (validadas).
- MV region_publicador: cifras por slug_region y publicador (nacional, depto, muni, amazonía, reservas, resguardos, núcleos DFYB).

Cifras estimadas:
- MV taxonomic_estimated_source: unión de taxonomic_col_list, taxonomic_cites, taxonomic_threat_mads, taxonomic_threat_iucn, taxonomic_invasive_exotic y taxonomic_migratory por species y JOIN temático por taxonomía para tener una única vista con todas las taxonomías y temáticas.
- MV estimadas_total: doble LATERAL (rangos × temáticas) + JOIN taxonomic_groups + pivot FILTER para obtener las cifras estimadas por temática y rango taxonómico.
"""

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from typing import Literal

from dotenv import load_dotenv

from utils.connection import check_connection, get_db, table_exists

# Cargar .env antes de importar utils.functions: sus constantes de tuning
# (_WORK_MEM, _MAX_PARALLEL_WORKERS_MV) se evalúan al importar el módulo para no sobreescribir el valor por defecto.
load_dotenv()

from utils.functions import (
    DWC_INTEGRATED_TABLE,
    _MAX_PARALLEL_WORKERS_MV,
    _WORK_MEM,
)

logger = logging.getLogger('sintesis_biocifras')

ESTIMADAS_TOTAL_MV = 'estimadas_total'
ESTIMATED_SPECIES_MV_LEGACY = 'estimated_species_totals'
TAXONOMIC_ESTIMATED_SOURCE_MV = 'taxonomic_estimated_source'
ESPECIE_MV = 'especie'
ESPECIE_META_MV = 'especie_meta'
ESPECIE_GRUPO_MV = 'especie_grupo'
ESPECIE_REGION_MV = 'especie_region'
ESPECIE_TEMATICA_MV = 'especie_tematica'
CIFRAS_TOTALES_MV = 'cifras_totales'
GEOGRAFIA_RESUMEN_MV = 'geografia_resumen'
PUBLICADOR_MV = 'publicador'
REGION_PUBLICADOR_MV = 'region_publicador'
REGION_TEMATICA_MV = 'region_tematica'
REGION_GRUPO_MV = 'region_grupo'
STAGING_AGG_TAXON_REGION_MV = 'staging_agg_taxon_region'
STAGING_OCURRENCIA_GEO_MV = 'staging_ocurrencia_geo'  # legacy, se elimina al crear staging_agg
CIFRAS_ESTIMADAS_DEPT_TABLE = 'tmp_cifras_estimated_dept'

_CIFRAS_TOTALES_NIVEL_LATERAL_SQL = """
    CROSS JOIN LATERAL (VALUES
        ('CCDM', true, 'nacional'),
        ('CSDM', false, 'nacional'),
        ('DCDM', true, 'depto'),
        ('DSDM', false, 'depto'),
        ('MCDM', true, 'muni'),
        ('MSDM', false, 'muni')
    ) AS n(nivel, incluye_marino, alcance)
"""

_GEOGRAFIA_NIVEL_LATERAL_SQL = """
    CROSS JOIN LATERAL (VALUES
        ('CCDM', true,  'nacional', 'depto'),
        ('CSDM', false, 'nacional', 'depto'),
        ('DCDM', true,  'depto',    'depto'),
        ('DSDM', false, 'depto',    'depto'),
        ('MCDM', true,  'muni',     'muni'),
        ('MSDM', false, 'muni',     'muni')
    ) AS n(nivel, incluye_marino, alcance, granularidad)
"""

_CATEGORY_TO_SLUG_EXPR = """
    CASE category
        WHEN 'Exótica con potencial de invasión Alto Riesgo'
            THEN 'exotica-riesgo-invasion-alto'
        WHEN 'Exótica con potencial de invasión Bajo Riesgo'
            THEN 'exotica-riesgo-invasion-bajo'
        WHEN 'Exótica con potencial de invasión Riesgo Moderado'
            THEN 'exotica-riesgo-invasion-moderado'
        WHEN 'Exótica con potencial de invasión Riesgo Moderado/ Alto'
            THEN 'exotica-riesgo-invasion-moderado-alto'
        WHEN 'LC_IUCN' THEN 'amenazadas-global-lc'
        WHEN 'NT_IUCN' THEN 'amenazadas-global-nt'
        WHEN 'VU_IUCN' THEN 'amenazadas-global-vu'
        WHEN 'EN_IUCN' THEN 'amenazadas-global-en'
        WHEN 'CR_IUCN' THEN 'amenazadas-global-cr'
        WHEN 'DD_IUCN' THEN 'amenazadas-global-dd'
        WHEN 'LR/lc_IUCN' THEN 'amenazadas-global-lr-lc'
        WHEN 'LR/nt_IUCN' THEN 'amenazadas-global-lr-nt'
        WHEN 'EW_IUCN' THEN 'amenazadas-global-ew'
        WHEN 'EX_IUCN' THEN 'amenazadas-global-ex'
        WHEN 'NE_IUCN' THEN 'amenazadas-global-ne'
        WHEN 'LR/cd_IUCN' THEN 'amenazadas-global-lr-cd'
        WHEN 'Invasora' THEN 'invasoras'
        WHEN 'I/II' THEN 'cites-i-ii'
        WHEN 'III' THEN 'cites-iii'
        WHEN 'II' THEN 'cites-ii'
        WHEN 'I' THEN 'cites-i'
        WHEN 'VU_MADS' THEN 'amenazadas-nacional-vu'
        WHEN 'EN_MADS' THEN 'amenazadas-nacional-en'
        WHEN 'CR_MADS' THEN 'amenazadas-nacional-cr'
        WHEN 'Exótica' THEN 'exoticas'
        WHEN 'Endémica' THEN 'endemicas'
        WHEN 'Migratorio' THEN 'migratorias'
        WHEN 'Errática' THEN 'erraticas'
        WHEN 'Residente' THEN 'residente'
        WHEN 'Trasplantada' THEN 'trasplantadas'
        ELSE LOWER(REPLACE(BTRIM(category::text), ' ', '-'))
    END
"""

_THEMATIC_LATERAL_SQL = """
    CROSS JOIN LATERAL (VALUES
        (b.threatstatusuicn),
        (b.threatstatusmads),
        (b.cites),
        (b.invasive),
        (b.exotic),
        (b.exoticriskinvasion),
        (b.transplanted),
        (b.endemic),
        (b.migratory)
    ) AS th(category)
"""

_ESPECIE_MV_SQL = """
    SELECT
        slugspecies AS slug,
        kingdom,
        phylum,
        "class",
        "order",
        family,
        genus
    FROM taxonomic_species_validation
    WHERE flagtaxo IS DISTINCT FROM 'Ausente en lista taxonómica'
    ORDER BY slug
"""

_ESPECIE_META_MV_SQL = """
    SELECT
        ts.slugspecies AS slug,
        m.vernacular_name_es,
        m.url_gbif_ AS url_gbif,
        m.url_cbc,
        m.flagtaxo AS "flagTAXO"
    FROM taxonomic_species_validation ts
    LEFT JOIN taxonomic_species_meta m ON m.slug = ts.slugspecies
    WHERE ts.flagtaxo IS DISTINCT FROM 'Ausente en lista taxonómica'
    ORDER BY slug
"""

_ESPECIE_GRUPO_MV_SQL = """
    SELECT DISTINCT
        s.slugspecies AS slug_especie,
        g.slug AS slug_grupo,
        g.grouptype AS tipo
    FROM taxonomic_species_validation s
    CROSS JOIN LATERAL (VALUES
        ('kingdom', s.kingdom),
        ('phylum', s.phylum),
        ('class', s."class"),
        ('order', s."order"),
        ('family', s.family),
        ('genus', s.genus),
        ('species', s.species)
    ) AS r(taxonrank, taxon)
    INNER JOIN taxonomic_groups g
        ON g.taxonrank = r.taxonrank
       AND g.taxon = r.taxon
    WHERE s.flagtaxo IS DISTINCT FROM 'Ausente en lista taxonómica'
      AND r.taxon IS NOT NULL
      AND NULLIF(BTRIM(r.taxon::text), '') IS NOT NULL
      AND g.grouptype IS NOT NULL
      AND BTRIM(g.grouptype) <> '-'
    ORDER BY slug_especie, slug_grupo
"""

def _slug_region_lateral_values(alias: str) -> str:
    """Valores LATERAL de expansión geo (nacional, depto, muni y capas slug)."""
    return f"""
            ('colombia'),
            ({alias}.dept_slug),
            ({alias}.muni_slug),
            ({alias}.amazonregion),
            ({alias}.reserve),
            ({alias}.indigenousreserve),
            ({alias}.dfybnucleus)
"""


_ESPECIE_REGION_MV_SQL = f"""
    WITH base AS (
        SELECT
            ts.slugspecies AS slug_especie,
            COALESCE(gl.stateprovinceslug, dept.slug) AS dept_slug,
            COALESCE(gl.countyslug, muni.slug) AS muni_slug,
            gl.amazonregion,
            gl.reserve,
            gl.indigenousreserve,
            gl.dfybnucleus
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
        WHERE ts.flagtaxo IS DISTINCT FROM 'Ausente en lista taxonómica'
          AND i.taxonomic_species_id IS NOT NULL
          AND i.locality_id IS NOT NULL
    ),
    por_region AS (
        SELECT
            b.slug_especie,
            r.slug_region
        FROM base b
        CROSS JOIN LATERAL (VALUES
            {_slug_region_lateral_values('b')}
        ) AS r(slug_region)
        WHERE r.slug_region IS NOT NULL
    )
    SELECT
        slug_region,
        slug_especie,
        COUNT(*)::int AS registros
    FROM por_region
    GROUP BY slug_region, slug_especie
    ORDER BY slug_region, slug_especie
"""

_ESPECIE_TEMATICA_MV_SQL = f"""
    WITH base AS (
        SELECT
            ts.slugspecies AS slug_especie,
            COALESCE(gl.stateprovinceslug, dept.slug) AS dept_slug,
            COALESCE(gl.countyslug, muni.slug) AS muni_slug,
            gl.amazonregion,
            gl.reserve,
            gl.indigenousreserve,
            gl.dfybnucleus,
            ts.threatstatusuicn,
            ts.threatstatusmads,
            ts.cites,
            ts.invasive,
            ts.exotic,
            ts.exoticriskinvasion,
            ts.transplanted,
            ts.endemic,
            ts.migratory
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
        WHERE ts.flagtaxo IS DISTINCT FROM 'Ausente en lista taxonómica'
          AND ts.species IS NOT NULL
          AND NULLIF(BTRIM(ts.species::text), '') IS NOT NULL
          AND i.taxonomic_species_id IS NOT NULL
          AND i.locality_id IS NOT NULL
    ),
    expanded AS (
        SELECT
            b.slug_especie,
            r.slug_region,
            th.category
        FROM base b
        {_THEMATIC_LATERAL_SQL}
        CROSS JOIN LATERAL (VALUES
            {_slug_region_lateral_values('b')}
        ) AS r(slug_region)
        WHERE r.slug_region IS NOT NULL
          AND NULLIF(BTRIM(th.category::text), '') IS NOT NULL
    )
    SELECT DISTINCT
        slug_especie,
        slug_region,
        {_CATEGORY_TO_SLUG_EXPR} AS slug_tematica
    FROM expanded
    ORDER BY slug_region, slug_especie, slug_tematica
"""

_CIFRAS_TOTALES_MV_SQL = f"""
    WITH base AS (
        SELECT
            i.gbifid,
            ts.species,
            ts.flagtaxo,
            ts.ismarine,
            ts.isbrackish,
            ts.isterrestrial,
            COALESCE(gl.stateprovinceslug, dept.slug) AS dept_slug,
            COALESCE(gl.countyslug, muni.slug) AS muni_slug
        FROM "{DWC_INTEGRATED_TABLE}" i
        LEFT JOIN taxonomic_species_validation ts ON ts.id = i.taxonomic_species_id
        LEFT JOIN geo_locality_validation gl ON gl.id = i.locality_id
        LEFT JOIN geo_master_geography gm ON gm.id = gl.geo_master_geography_id
        LEFT JOIN geo_master_geography muni
            ON muni.id = CASE WHEN gm.subtype = 'municipio' THEN gm.id END
        LEFT JOIN geo_master_geography dept
            ON dept.id = COALESCE(
                muni.parent_id,
                CASE WHEN gm.subtype = 'departamento' THEN gm.id END
            )
    ),
    por_nivel AS (
        SELECT
            n.nivel,
            n.incluye_marino,
            b.species,
            b.flagtaxo,
            b.ismarine,
            b.isbrackish,
            b.isterrestrial
        FROM base b
        {_CIFRAS_TOTALES_NIVEL_LATERAL_SQL}
        WHERE CASE n.alcance
            WHEN 'nacional' THEN true
            WHEN 'depto' THEN b.dept_slug IS NOT NULL
            WHEN 'muni' THEN b.muni_slug IS NOT NULL
        END
    )
    SELECT
        nivel,
        COUNT(*)::bigint AS registros,
        CASE WHEN bool_or(incluye_marino) THEN
            COUNT(*) FILTER (WHERE isterrestrial IS NOT NULL)::int
        END AS registros_continentales,
        CASE WHEN bool_or(incluye_marino) THEN
            COUNT(*) FILTER (WHERE ismarine IS NOT NULL)::int
        END AS registros_marinos,
        CASE WHEN bool_or(incluye_marino) THEN
            COUNT(*) FILTER (WHERE isbrackish IS NOT NULL)::int
        END AS registros_salobres,
        COUNT(DISTINCT species) FILTER (
            WHERE species IS NOT NULL
              AND NULLIF(BTRIM(species::text), '') IS NOT NULL
              AND flagtaxo IS DISTINCT FROM 'Ausente en lista taxonómica'
        )::int AS especies,
        CASE WHEN bool_or(incluye_marino) THEN
            COUNT(DISTINCT species) FILTER (
                WHERE isterrestrial = 'Terrestrial'
                  AND species IS NOT NULL
                  AND NULLIF(BTRIM(species::text), '') IS NOT NULL
                  AND flagtaxo IS DISTINCT FROM 'Ausente en lista taxonómica'
            )::int
        END AS especies_continentales,
        CASE WHEN bool_or(incluye_marino) THEN
            COUNT(DISTINCT species) FILTER (
                WHERE ismarine = 'Marine'
                  AND species IS NOT NULL
                  AND NULLIF(BTRIM(species::text), '') IS NOT NULL
                  AND flagtaxo IS DISTINCT FROM 'Ausente en lista taxonómica'
            )::int
        END AS especies_marinas,
        CASE WHEN bool_or(incluye_marino) THEN
            COUNT(DISTINCT species) FILTER (
                WHERE isbrackish = 'Brackish'
                  AND species IS NOT NULL
                  AND NULLIF(BTRIM(species::text), '') IS NOT NULL
                  AND flagtaxo IS DISTINCT FROM 'Ausente en lista taxonómica'
            )::int
        END AS especies_salobres
    FROM por_nivel
    GROUP BY nivel
    ORDER BY nivel
"""

_GEOGRAFIA_RESUMEN_MV_SQL = f"""
    WITH base AS (
        SELECT
            i.gbifid,
            ts.species,
            ts.flagtaxo,
            ts.ismarine,
            ts.isbrackish,
            ts.isterrestrial,
            COALESCE(gl.stateprovinceslug, dept.slug) AS dept_slug,
            COALESCE(gl.countyslug, muni.slug) AS muni_slug
        FROM "{DWC_INTEGRATED_TABLE}" i
        LEFT JOIN taxonomic_species_validation ts ON ts.id = i.taxonomic_species_id
        LEFT JOIN geo_locality_validation gl ON gl.id = i.locality_id
        LEFT JOIN geo_master_geography gm ON gm.id = gl.geo_master_geography_id
        LEFT JOIN geo_master_geography muni
            ON muni.id = CASE WHEN gm.subtype = 'municipio' THEN gm.id END
        LEFT JOIN geo_master_geography dept
            ON dept.id = COALESCE(
                muni.parent_id,
                CASE WHEN gm.subtype = 'departamento' THEN gm.id END
            )
    ),
    por_geo AS (
        SELECT
            n.nivel,
            n.incluye_marino,
            CASE n.granularidad
                WHEN 'depto' THEN b.dept_slug
                WHEN 'muni' THEN b.muni_slug
            END AS slug_region,
            b.species,
            b.flagtaxo,
            b.ismarine,
            b.isbrackish,
            b.isterrestrial
        FROM base b
        {_GEOGRAFIA_NIVEL_LATERAL_SQL}
        WHERE CASE n.granularidad
            WHEN 'depto' THEN b.dept_slug IS NOT NULL
            WHEN 'muni' THEN b.muni_slug IS NOT NULL
        END
    )
    SELECT
        nivel,
        slug_region,
        COUNT(*)::bigint AS registros,
        CASE WHEN bool_or(incluye_marino) THEN
            COUNT(*) FILTER (WHERE isterrestrial IS NOT NULL)::int
        END AS registros_continentales,
        CASE WHEN bool_or(incluye_marino) THEN
            COUNT(*) FILTER (WHERE ismarine IS NOT NULL)::int
        END AS registros_marinos,
        CASE WHEN bool_or(incluye_marino) THEN
            COUNT(*) FILTER (WHERE isbrackish IS NOT NULL)::int
        END AS registros_salobres,
        COUNT(DISTINCT species) FILTER (
            WHERE species IS NOT NULL
              AND NULLIF(BTRIM(species::text), '') IS NOT NULL
              AND flagtaxo IS DISTINCT FROM 'Ausente en lista taxonómica'
        )::int AS especies,
        CASE WHEN bool_or(incluye_marino) THEN
            COUNT(DISTINCT ROW(species, isterrestrial)) FILTER (
                WHERE species IS NOT NULL
                  AND NULLIF(BTRIM(species::text), '') IS NOT NULL
                  AND flagtaxo IS DISTINCT FROM 'Ausente en lista taxonómica'
                  AND isterrestrial IS NOT NULL
            )::int
        END AS especies_continentales,
        CASE WHEN bool_or(incluye_marino) THEN
            COUNT(DISTINCT ROW(species, ismarine)) FILTER (
                WHERE species IS NOT NULL
                  AND NULLIF(BTRIM(species::text), '') IS NOT NULL
                  AND flagtaxo IS DISTINCT FROM 'Ausente en lista taxonómica'
                  AND ismarine IS NOT NULL
            )::int
        END AS especies_marinas,
        CASE WHEN bool_or(incluye_marino) THEN
            COUNT(DISTINCT ROW(species, isbrackish)) FILTER (
                WHERE species IS NOT NULL
                  AND NULLIF(BTRIM(species::text), '') IS NOT NULL
                  AND flagtaxo IS DISTINCT FROM 'Ausente en lista taxonómica'
                  AND isbrackish IS NOT NULL
            )::int
        END AS especies_salobres
    FROM por_geo
    GROUP BY nivel, slug_region
    ORDER BY nivel, slug_region
"""

_PUBLICADOR_MV_SQL = f"""
    WITH registros_por_pub AS (
        SELECT
            i.publishingorgkey,
            COUNT(*)::bigint AS registros,
            MAX(
                REPLACE(BTRIM(i.publishingcountry), 'País publicador: ', '')
            ) AS pais_publicacion
        FROM "{DWC_INTEGRATED_TABLE}" i
        WHERE i.publishingorgkey IS NOT NULL
          AND NULLIF(BTRIM(i.publishingorgkey::text), '') IS NOT NULL
        GROUP BY i.publishingorgkey
    ),
    especies_por_pub AS (
        SELECT
            i.publishingorgkey,
            COUNT(DISTINCT ts.id)::int AS especies
        FROM "{DWC_INTEGRATED_TABLE}" i
        INNER JOIN taxonomic_species_validation ts ON ts.id = i.taxonomic_species_id
        WHERE i.publishingorgkey IS NOT NULL
          AND NULLIF(BTRIM(i.publishingorgkey::text), '') IS NOT NULL
          AND i.taxonomic_species_id IS NOT NULL
          AND ts.flagtaxo IS DISTINCT FROM 'Ausente en lista taxonómica'
          AND ts.species IS NOT NULL
          AND NULLIF(BTRIM(ts.species::text), '') IS NOT NULL
        GROUP BY i.publishingorgkey
    )
    SELECT
        r.publishingorgkey AS slug,
        p.organization AS label,
        r.pais_publicacion,
        ''::text AS tipo_organizacion,
        CASE
            WHEN r.pais_publicacion = 'CO' THEN 'Nacional'
            ELSE 'Internacional'
        END AS tipo_publicador,
        ''::text AS url_logo,
        'https://biodiversidad.co/data/?publishingOrg=' || r.publishingorgkey AS url_socio,
        COALESCE(e.especies, 0) AS especies,
        r.registros
    FROM registros_por_pub r
    LEFT JOIN gbif_publishers p ON p.publishingorgkey = r.publishingorgkey
    LEFT JOIN especies_por_pub e ON e.publishingorgkey = r.publishingorgkey
    ORDER BY p.organization NULLS LAST, r.publishingorgkey
"""

_REGION_PUBLICADOR_MV_SQL = f"""
    WITH enriched AS (
        SELECT
            i.publishingorgkey,
            ts.id AS species_id,
            ts.species,
            ts.flagtaxo,
            ts.ismarine,
            ts.isbrackish,
            ts.isterrestrial,
            COALESCE(gl.stateprovinceslug, dept.slug) AS dept_slug,
            COALESCE(gl.countyslug, muni.slug) AS muni_slug,
            gl.amazonregion,
            gl.reserve,
            gl.indigenousreserve,
            gl.dfybnucleus
        FROM "{DWC_INTEGRATED_TABLE}" i
        LEFT JOIN taxonomic_species_validation ts ON ts.id = i.taxonomic_species_id
        LEFT JOIN geo_locality_validation gl ON gl.id = i.locality_id
        LEFT JOIN geo_master_geography gm ON gm.id = gl.geo_master_geography_id
        LEFT JOIN geo_master_geography muni
            ON muni.id = CASE WHEN gm.subtype = 'municipio' THEN gm.id END
        LEFT JOIN geo_master_geography dept
            ON dept.id = COALESCE(
                muni.parent_id,
                CASE WHEN gm.subtype = 'departamento' THEN gm.id END
            )
        WHERE i.publishingorgkey IS NOT NULL
          AND NULLIF(BTRIM(i.publishingorgkey::text), '') IS NOT NULL
    ),
    por_region AS (
        SELECT
            r.slug_region,
            e.publishingorgkey,
            e.species_id,
            e.species,
            e.flagtaxo,
            e.ismarine,
            e.isbrackish,
            e.isterrestrial
        FROM enriched e
        CROSS JOIN LATERAL (VALUES
            {_slug_region_lateral_values('e')}
        ) AS r(slug_region)
        WHERE r.slug_region IS NOT NULL
    )
    SELECT
        slug_region,
        publishingorgkey AS slug_publicador,
        COUNT(*)::bigint AS registros,
        COUNT(*) FILTER (WHERE isterrestrial = 'Terrestrial')::int AS registros_continentales,
        COUNT(*) FILTER (WHERE ismarine = 'Marine')::int AS registros_marinos,
        COUNT(*) FILTER (WHERE isbrackish = 'Brackish')::int AS registros_salobres,
        COUNT(DISTINCT species_id) FILTER (
            WHERE flagtaxo IS DISTINCT FROM 'Ausente en lista taxonómica'
              AND species IS NOT NULL AND NULLIF(BTRIM(species::text), '') IS NOT NULL
        )::int AS especies,
        COUNT(DISTINCT species_id) FILTER (
            WHERE isterrestrial = 'Terrestrial'
              AND flagtaxo IS DISTINCT FROM 'Ausente en lista taxonómica'
              AND species IS NOT NULL AND NULLIF(BTRIM(species::text), '') IS NOT NULL
        )::int AS especies_continentales,
        COUNT(DISTINCT species_id) FILTER (
            WHERE ismarine = 'Marine'
              AND flagtaxo IS DISTINCT FROM 'Ausente en lista taxonómica'
              AND species IS NOT NULL AND NULLIF(BTRIM(species::text), '') IS NOT NULL
        )::int AS especies_marinas,
        COUNT(DISTINCT species_id) FILTER (
            WHERE isbrackish = 'Brackish'
              AND flagtaxo IS DISTINCT FROM 'Ausente en lista taxonómica'
              AND species IS NOT NULL AND NULLIF(BTRIM(species::text), '') IS NOT NULL
        )::int AS especies_salobres
    FROM por_region
    GROUP BY slug_region, publishingorgkey
    ORDER BY slug_region, slug_publicador
"""

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


def create_especie_materialized_view(db):
    """Crea MV especie desde taxonomic_species_validation (slug + rangos taxonómicos)."""
    if not table_exists(db, 'taxonomic_species_validation'):
        raise ValueError('La tabla taxonomic_species_validation no existe en la base de datos.')
    with db.connect() as conn:
        conn.execute(f'DROP MATERIALIZED VIEW IF EXISTS {ESPECIE_MV}')
        conn.execute(f'CREATE MATERIALIZED VIEW {ESPECIE_MV} AS {_ESPECIE_MV_SQL}')
        conn.execute(f'CREATE INDEX IF NOT EXISTS idx_{ESPECIE_MV}_slug ON {ESPECIE_MV} (slug)')
        conn.commit()
        total = conn.execute(f'SELECT COUNT(*) FROM {ESPECIE_MV}').fetchall()[0][0]
    logger.info('Vista materializada %s creada (%s filas)', ESPECIE_MV, total)
    print(f'Vista materializada {ESPECIE_MV}: {total:,} filas')
    return total


def create_especie_meta_materialized_view(db):
    """Crea MV especie_meta: slug validado + metadatos desde taxonomic_species_meta."""
    required = ('taxonomic_species_validation', 'taxonomic_species_meta')
    missing = [name for name in required if not table_exists(db, name)]
    if missing:
        raise ValueError(f'Faltan tablas requeridas: {", ".join(missing)}')
    with db.connect() as conn:
        conn.execute(f'DROP MATERIALIZED VIEW IF EXISTS {ESPECIE_META_MV}')
        conn.execute(f'CREATE MATERIALIZED VIEW {ESPECIE_META_MV} AS {_ESPECIE_META_MV_SQL}')
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{ESPECIE_META_MV}_slug '
            f'ON {ESPECIE_META_MV} (slug)'
        )
        conn.commit()
        total = conn.execute(f'SELECT COUNT(*) FROM {ESPECIE_META_MV}').fetchall()[0][0]
    logger.info('Vista materializada %s creada (%s filas)', ESPECIE_META_MV, total)
    print(f'Vista materializada {ESPECIE_META_MV}: {total:,} filas')
    return total


def create_especie_grupo_materialized_view(db):
    """Crea MV especie_grupo: slug_especie, slug_grupo y tipo desde taxonomic_groups."""
    if not table_exists(db, 'taxonomic_species_validation'):
        raise ValueError('La tabla taxonomic_species_validation no existe en la base de datos.')
    if not table_exists(db, 'taxonomic_groups'):
        raise ValueError('La tabla taxonomic_groups no existe en la base de datos.')
    with db.connect() as conn:
        conn.execute(f'DROP MATERIALIZED VIEW IF EXISTS {ESPECIE_GRUPO_MV}')
        conn.execute(f'CREATE MATERIALIZED VIEW {ESPECIE_GRUPO_MV} AS {_ESPECIE_GRUPO_MV_SQL}')
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{ESPECIE_GRUPO_MV}_slug_especie '
            f'ON {ESPECIE_GRUPO_MV} (slug_especie)'
        )
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{ESPECIE_GRUPO_MV}_slug_grupo '
            f'ON {ESPECIE_GRUPO_MV} (slug_grupo)'
        )
        conn.commit()
        total = conn.execute(f'SELECT COUNT(*) FROM {ESPECIE_GRUPO_MV}').fetchall()[0][0]
    logger.info('Vista materializada %s creada (%s filas)', ESPECIE_GRUPO_MV, total)
    print(f'Vista materializada {ESPECIE_GRUPO_MV}: {total:,} filas')
    return total


def create_especie_region_materialized_view(db):
    """Crea MV especie_region: conteos por slug_especie y slug_region."""
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
        conn.execute(f'DROP MATERIALIZED VIEW IF EXISTS {ESPECIE_REGION_MV}')
        conn.execute(f'CREATE MATERIALIZED VIEW {ESPECIE_REGION_MV} AS {_ESPECIE_REGION_MV_SQL}')
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{ESPECIE_REGION_MV}_slug_region '
            f'ON {ESPECIE_REGION_MV} (slug_region)'
        )
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{ESPECIE_REGION_MV}_slug_especie '
            f'ON {ESPECIE_REGION_MV} (slug_especie)'
        )
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{ESPECIE_REGION_MV}_region_especie '
            f'ON {ESPECIE_REGION_MV} (slug_region, slug_especie)'
        )
        conn.commit()
        total = conn.execute(f'SELECT COUNT(*) FROM {ESPECIE_REGION_MV}').fetchall()[0][0]
    logger.info('Vista materializada %s creada (%s filas)', ESPECIE_REGION_MV, total)
    print(f'Vista materializada {ESPECIE_REGION_MV}: {total:,} filas')
    return total


def create_especie_tematica_materialized_view(db):
    """Crea MV especie_tematica: relación DISTINCT slug_especie, slug_region, slug_tematica."""
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
        conn.execute(f'DROP MATERIALIZED VIEW IF EXISTS {ESPECIE_TEMATICA_MV}')
        conn.execute(f'CREATE MATERIALIZED VIEW {ESPECIE_TEMATICA_MV} AS {_ESPECIE_TEMATICA_MV_SQL}')
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{ESPECIE_TEMATICA_MV}_slug_region '
            f'ON {ESPECIE_TEMATICA_MV} (slug_region)'
        )
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{ESPECIE_TEMATICA_MV}_slug_especie '
            f'ON {ESPECIE_TEMATICA_MV} (slug_especie)'
        )
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{ESPECIE_TEMATICA_MV}_slug_tematica '
            f'ON {ESPECIE_TEMATICA_MV} (slug_tematica)'
        )
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{ESPECIE_TEMATICA_MV}_region_especie_tematica '
            f'ON {ESPECIE_TEMATICA_MV} (slug_region, slug_especie, slug_tematica)'
        )
        conn.commit()
        total = conn.execute(f'SELECT COUNT(*) FROM {ESPECIE_TEMATICA_MV}').fetchall()[0][0]
    logger.info('Vista materializada %s creada (%s filas)', ESPECIE_TEMATICA_MV, total)
    print(f'Vista materializada {ESPECIE_TEMATICA_MV}: {total:,} filas')
    return total


def create_cifras_totales_materialized_view(db):
    """Crea MV cifras_totales: conteos globales por nivel CDM (6 filas)."""
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
        conn.execute(f'DROP MATERIALIZED VIEW IF EXISTS {CIFRAS_TOTALES_MV}')
        conn.execute(f'CREATE MATERIALIZED VIEW {CIFRAS_TOTALES_MV} AS {_CIFRAS_TOTALES_MV_SQL}')
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{CIFRAS_TOTALES_MV}_nivel '
            f'ON {CIFRAS_TOTALES_MV} (nivel)'
        )
        conn.commit()
        total = conn.execute(f'SELECT COUNT(*) FROM {CIFRAS_TOTALES_MV}').fetchall()[0][0]
    logger.info('Vista materializada %s creada (%s filas)', CIFRAS_TOTALES_MV, total)
    print(f'Vista materializada {CIFRAS_TOTALES_MV}: {total:,} filas')
    return total


def create_geografia_resumen_materialized_view(db):
    """Crea MV geografia_resumen: conteos por nivel CDM y slug_region."""
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
        conn.execute(f'DROP MATERIALIZED VIEW IF EXISTS {GEOGRAFIA_RESUMEN_MV}')
        conn.execute(
            f'CREATE MATERIALIZED VIEW {GEOGRAFIA_RESUMEN_MV} AS {_GEOGRAFIA_RESUMEN_MV_SQL}'
        )
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{GEOGRAFIA_RESUMEN_MV}_nivel '
            f'ON {GEOGRAFIA_RESUMEN_MV} (nivel)'
        )
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{GEOGRAFIA_RESUMEN_MV}_slug_region '
            f'ON {GEOGRAFIA_RESUMEN_MV} (slug_region)'
        )
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{GEOGRAFIA_RESUMEN_MV}_nivel_region '
            f'ON {GEOGRAFIA_RESUMEN_MV} (nivel, slug_region)'
        )
        conn.commit()
        total = conn.execute(f'SELECT COUNT(*) FROM {GEOGRAFIA_RESUMEN_MV}').fetchall()[0][0]
    logger.info('Vista materializada %s creada (%s filas)', GEOGRAFIA_RESUMEN_MV, total)
    print(f'Vista materializada {GEOGRAFIA_RESUMEN_MV}: {total:,} filas')
    return total


# --- region_tematica: métricas y SQL (legacy geografia_total + ajuste_nombres) ---

_RegionTematicaKind = Literal['registros', 'especies']
_RegionTematicaHabitat = Literal['continental', 'marino', 'salobre'] | None

_REGION_TEMATICA_SPECIES_VALID = """
    species IS NOT NULL
    AND NULLIF(BTRIM(species::text), '') IS NOT NULL
    AND flagtaxo IS DISTINCT FROM 'Ausente en lista taxonómica'
""".strip()

_REGION_TEMATICA_HABITAT_SQL = {
    'continental': 'isterrestrial IS NOT NULL',
    'marino': 'ismarine IS NOT NULL',
    'salobre': 'isbrackish IS NOT NULL',
}

_REGION_TEMATICA_HABITAT_COL = {
    'continental': 'isterrestrial',
    'marino': 'ismarine',
    'salobre': 'isbrackish',
}

_REGION_TEMATICA_THEMATIC_FIELDS = {
    'threatStatus_MADS': 'threatstatusmads',
    'appendixCITES': 'cites',
    'endemic': 'endemic',
    'migratory': 'migratory',
}

_REGION_TEMATICA_CATEGORY_MAP = {
    'CR_MADS': ('threatstatusmads', 'CR_MADS'),
    'EN_MADS': ('threatstatusmads', 'EN_MADS'),
    'VU_MADS': ('threatstatusmads', 'VU_MADS'),
    'I': ('cites', 'I'),
    'I/II': ('cites', 'I/II'),
    'II': ('cites', 'II'),
    'III': ('cites', 'III'),
    'Exótica': ('exotic', 'Exótica'),
    'Invasora': ('invasive', 'Invasora'),
    'Trasplantada': ('transplanted', 'Trasplantada'),
    'Exótica con potencial de invasión Alto Riesgo': (
        'exoticriskinvasion',
        'Exótica con potencial de invasión Alto Riesgo',
    ),
    'Exótica con potencial de invasión Bajo Riesgo': (
        'exoticriskinvasion',
        'Exótica con potencial de invasión Bajo Riesgo',
    ),
    'Exótica con potencial de invasión Riesgo Moderado': (
        'exoticriskinvasion',
        'Exótica con potencial de invasión Riesgo Moderado',
    ),
    'Exótica con potencial de invasión Riesgo Moderado/ Alto': (
        'exoticriskinvasion',
        'Exótica con potencial de invasión Riesgo Moderado/ Alto',
    ),
    'EX_IUCN': ('threatstatusuicn', 'EX_IUCN'),
    'EW_IUCN': ('threatstatusuicn', 'EW_IUCN'),
    'CR_IUCN': ('threatstatusuicn', 'CR_IUCN'),
    'EN_IUCN': ('threatstatusuicn', 'EN_IUCN'),
    'VU_IUCN': ('threatstatusuicn', 'VU_IUCN'),
    'NT_IUCN': ('threatstatusuicn', 'NT_IUCN'),
    'LC_IUCN': ('threatstatusuicn', 'LC_IUCN'),
    'DD_IUCN': ('threatstatusuicn', 'DD_IUCN'),
    'LR/lc_IUCN': ('threatstatusuicn', 'LR/lc_IUCN'),
    'LR/nt_IUCN': ('threatstatusuicn', 'LR/nt_IUCN'),
}

_REGION_TEMATICA_DERIVED_SOURCES = {
    'especies_exoticas_total': [
        'especies_exoticas', 'especies_invasoras',
        'especies_exoticas_riesgo_invasion_alto', 'especies_exoticas_riesgo_invasion_bajo',
        'especies_exoticas_riesgo_invasion_moderado', 'especies_exoticas_riesgo_invasion_moderado_alto',
        'especies_trasplantadas',
    ],
    'registros_exoticas_total': [
        'registros_exoticas', 'registros_invasoras',
        'registros_exoticas_riesgo_invasion_alto', 'registros_exoticas_riesgo_invasion_bajo',
        'registros_exoticas_riesgo_invasion_moderado', 'registros_exoticas_riesgo_invasion_moderado_alto',
        'registros_trasplantadas',
    ],
    'especies_exoticas_riesgo_invasion_total': [
        'especies_exoticas_riesgo_invasion_alto', 'especies_exoticas_riesgo_invasion_bajo',
        'especies_exoticas_riesgo_invasion_moderado', 'especies_exoticas_riesgo_invasion_moderado_alto',
    ],
    'registros_exoticas_riesgo_invasion_total': [
        'registros_exoticas_riesgo_invasion_alto', 'registros_exoticas_riesgo_invasion_bajo',
        'registros_exoticas_riesgo_invasion_moderado', 'registros_exoticas_riesgo_invasion_moderado_alto',
    ],
    'especies_amenazadas_global_total': [
        'especies_amenazadas_global_en', 'especies_amenazadas_global_cr', 'especies_amenazadas_global_vu',
    ],
    'registros_amenazadas_global_total': [
        'registros_amenazadas_global_en', 'registros_amenazadas_global_cr', 'registros_amenazadas_global_vu',
    ],
    'especies_continentales_exoticas_total': [
        'especies_continentales_exoticas', 'especies_continentales_invasoras',
        'especies_continentales_exoticas_riesgo_invasion_alto', 'especies_continentales_exoticas_riesgo_invasion_bajo',
        'especies_continentales_exoticas_riesgo_invasion_moderado',
        'especies_continentales_exoticas_riesgo_invasion_moderado_alto', 'especies_continentales_trasplantadas',
    ],
    'registros_continentales_exoticas_total': [
        'registros_continentales_exoticas', 'registros_continentales_invasoras',
        'registros_continentales_exoticas_riesgo_invasion_alto', 'registros_continentales_exoticas_riesgo_invasion_bajo',
        'registros_continentales_exoticas_riesgo_invasion_moderado',
        'registros_continentales_exoticas_riesgo_invasion_moderado_alto', 'registros_continentales_trasplantadas',
    ],
    'especies_continentales_exoticas_riesgo_invasion_total': [
        'especies_continentales_exoticas_riesgo_invasion_alto', 'especies_continentales_exoticas_riesgo_invasion_bajo',
        'especies_continentales_exoticas_riesgo_invasion_moderado',
        'especies_continentales_exoticas_riesgo_invasion_moderado_alto',
    ],
    'registros_continentales_exoticas_riesgo_invasion_total': [
        'registros_continentales_exoticas_riesgo_invasion_alto', 'registros_continentales_exoticas_riesgo_invasion_bajo',
        'registros_continentales_exoticas_riesgo_invasion_moderado',
        'registros_continentales_exoticas_riesgo_invasion_moderado_alto',
    ],
    'especies_marinas_exoticas_total': [
        'especies_marinas_exoticas', 'especies_marinas_invasoras',
        'especies_marinas_exoticas_riesgo_invasion_alto', 'especies_marinas_exoticas_riesgo_invasion_bajo',
        'especies_marinas_exoticas_riesgo_invasion_moderado', 'especies_marinas_exoticas_riesgo_invasion_moderado_alto',
        'especies_marinas_trasplantadas',
    ],
    'registros_marinas_exoticas_total': [
        'registros_marinas_exoticas', 'registros_marinas_invasoras',
        'registros_marinas_exoticas_riesgo_invasion_alto', 'registros_marinas_exoticas_riesgo_invasion_bajo',
        'registros_marinas_exoticas_riesgo_invasion_moderado', 'registros_marinas_exoticas_riesgo_invasion_moderado_alto',
        'registros_marinas_trasplantadas',
    ],
    'especies_marinas_exoticas_riesgo_invasion_total': [
        'especies_marinas_exoticas_riesgo_invasion_alto', 'especies_marinas_exoticas_riesgo_invasion_bajo',
        'especies_marinas_exoticas_riesgo_invasion_moderado', 'especies_marinas_exoticas_riesgo_invasion_moderado_alto',
    ],
    'registros_marinas_exoticas_riesgo_invasion_total': [
        'registros_marinas_exoticas_riesgo_invasion_alto', 'registros_marinas_exoticas_riesgo_invasion_bajo',
        'registros_marinas_exoticas_riesgo_invasion_moderado', 'registros_marinas_exoticas_riesgo_invasion_moderado_alto',
    ],
    'especies_salobres_exoticas_total': [
        'especies_salobres_exoticas', 'especies_salobres_invasoras',
        'especies_salobres_exoticas_riesgo_invasion_alto', 'especies_salobres_exoticas_riesgo_invasion_bajo',
        'especies_salobres_exoticas_riesgo_invasion_moderado', 'especies_salobres_exoticas_riesgo_invasion_moderado_alto',
        'especies_salobres_trasplantadas',
    ],
    'registros_salobres_exoticas_total': [
        'registros_salobres_exoticas', 'registros_salobres_invasoras',
        'registros_salobres_exoticas_riesgo_invasion_alto', 'registros_salobres_exoticas_riesgo_invasion_bajo',
        'registros_salobres_exoticas_riesgo_invasion_moderado', 'registros_salobres_exoticas_riesgo_invasion_moderado_alto',
        'registros_salobres_trasplantadas',
    ],
    'especies_salobres_exoticas_riesgo_invasion_total': [
        'especies_salobres_exoticas_riesgo_invasion_alto', 'especies_salobres_exoticas_riesgo_invasion_bajo',
        'especies_salobres_exoticas_riesgo_invasion_moderado', 'especies_salobres_exoticas_riesgo_invasion_moderado_alto',
    ],
    'registros_salobres_exoticas_riesgo_invasion_total': [
        'registros_salobres_exoticas_riesgo_invasion_alto', 'registros_salobres_exoticas_riesgo_invasion_bajo',
        'registros_salobres_exoticas_riesgo_invasion_moderado', 'registros_salobres_exoticas_riesgo_invasion_moderado_alto',
    ],
    'especies_continentales_amenazadas_global_total': [
        'especies_continentales_amenazadas_global_en', 'especies_continentales_amenazadas_global_cr',
        'especies_continentales_amenazadas_global_vu',
    ],
    'registros_continentales_amenazadas_global_total': [
        'registros_continentales_amenazadas_global_en', 'registros_continentales_amenazadas_global_cr',
        'registros_continentales_amenazadas_global_vu',
    ],
    'especies_marinas_amenazadas_global_total': [
        'especies_marinas_amenazadas_global_en', 'especies_marinas_amenazadas_global_cr',
        'especies_marinas_amenazadas_global_vu',
    ],
    'registros_marinas_amenazadas_global_total': [
        'registros_marinas_amenazadas_global_en', 'registros_marinas_amenazadas_global_cr',
        'registros_marinas_amenazadas_global_vu',
    ],
    'especies_salobres_amenazadas_global_total': [
        'especies_salobres_amenazadas_global_en', 'especies_salobres_amenazadas_global_cr',
        'especies_salobres_amenazadas_global_vu',
    ],
    'registros_salobres_amenazadas_global_total': [
        'registros_salobres_amenazadas_global_en', 'registros_salobres_amenazadas_global_cr',
        'registros_salobres_amenazadas_global_vu',
    ],
}


@dataclass(frozen=True)
class _RegionTematicaMetric:
    column: str
    kind: _RegionTematicaKind
    habitat: _RegionTematicaHabitat = None
    field: str | None = None
    value: str | None = None
    row_distinct_species: bool = False


def _region_tematica_parse_habitat(text: str) -> tuple[_RegionTematicaHabitat, str]:
    for label, hab in (
        ('Continentales', 'continental'),
        ('Marinos', 'marino'),
        ('Marinas', 'marino'),
        ('Salobres', 'salobre'),
    ):
        if text.startswith(label + ' '):
            return hab, text[len(label) + 1 :]
        if text == label:
            return hab, ''
    return None, text


def _region_tematica_resolve_field(label: str) -> tuple[str | None, str | None]:
    if label in _REGION_TEMATICA_THEMATIC_FIELDS:
        return _REGION_TEMATICA_THEMATIC_FIELDS[label], None
    if label in _REGION_TEMATICA_CATEGORY_MAP:
        return _REGION_TEMATICA_CATEGORY_MAP[label]
    return None, None


def _region_tematica_metric_from_legacy(output_col: str, legacy_key: str) -> _RegionTematicaMetric | None:
    if legacy_key in ('registros', 'especies'):
        return _RegionTematicaMetric(
            column=output_col,
            kind='registros' if legacy_key == 'registros' else 'especies',
        )
    direct_habitat = {
        'registrosContinentales': ('registros', 'continental'),
        'registrosMarinos': ('registros', 'marino'),
        'registrosSalobres': ('registros', 'salobre'),
        'especiesContinentales': ('especies', 'continental'),
        'especiesMarinas': ('especies', 'marino'),
        'especiesSalobres': ('especies', 'salobre'),
    }
    if legacy_key in direct_habitat:
        kind, hab = direct_habitat[legacy_key]
        return _RegionTematicaMetric(
            column=output_col, kind=kind, habitat=hab, row_distinct_species=kind == 'especies',
        )
    parts = legacy_key.split(' ', 1)
    if len(parts) != 2:
        return None
    kind_word, rest = parts
    if kind_word not in ('registros', 'especies'):
        return None
    habitat, label = _region_tematica_parse_habitat(rest)
    field, value = _region_tematica_resolve_field(label)
    if field is None:
        return None
    return _RegionTematicaMetric(
        column=output_col, kind=kind_word, habitat=habitat, field=field, value=value,
    )


_REGION_TEMATICA_LEGACY_MAP: dict[str, str] = {
    'registros': 'registros_region_total',
    'registrosContinentales': 'registros_continentales',
    'registrosMarinos': 'registros_marinos',
    'registrosSalobres': 'registros_salobres',
    'especies': 'especies_region_total',
    'especiesContinentales': 'especies_continentales',
    'especiesMarinas': 'especies_marinas',
    'especiesSalobres': 'especies_salobres',
    'especies threatStatus_MADS': 'especies_amenazadas_nacional_total',
    'especies CR_MADS': 'especies_amenazadas_nacional_cr',
    'especies EN_MADS': 'especies_amenazadas_nacional_en',
    'especies VU_MADS': 'especies_amenazadas_nacional_vu',
    'registros threatStatus_MADS': 'registros_amenazadas_nacional_total',
    'registros CR_MADS': 'registros_amenazadas_nacional_cr',
    'registros EN_MADS': 'registros_amenazadas_nacional_en',
    'registros VU_MADS': 'registros_amenazadas_nacional_vu',
    'especies appendixCITES': 'especies_cites_total',
    'especies I': 'especies_cites_i',
    'especies I/II': 'especies_cites_i_ii',
    'especies II': 'especies_cites_ii',
    'especies III': 'especies_cites_iii',
    'registros appendixCITES': 'registros_cites_total',
    'registros I': 'registros_cites_i',
    'registros I/II': 'registros_cites_i_ii',
    'registros II': 'registros_cites_ii',
    'registros III': 'registros_cites_iii',
    'especies Exótica': 'especies_exoticas',
    'especies Invasora': 'especies_invasoras',
    'especies Exótica con potencial de invasión Alto Riesgo': 'especies_exoticas_riesgo_invasion_alto',
    'especies Exótica con potencial de invasión Bajo Riesgo': 'especies_exoticas_riesgo_invasion_bajo',
    'especies Exótica con potencial de invasión Riesgo Moderado': 'especies_exoticas_riesgo_invasion_moderado',
    'especies Exótica con potencial de invasión Riesgo Moderado/ Alto': (
        'especies_exoticas_riesgo_invasion_moderado_alto'
    ),
    'especies Trasplantada': 'especies_trasplantadas',
    'registros Exótica': 'registros_exoticas',
    'registros Invasora': 'registros_invasoras',
    'registros Exótica con potencial de invasión Alto Riesgo': 'registros_exoticas_riesgo_invasion_alto',
    'registros Exótica con potencial de invasión Bajo Riesgo': 'registros_exoticas_riesgo_invasion_bajo',
    'registros Exótica con potencial de invasión Riesgo Moderado': 'registros_exoticas_riesgo_invasion_moderado',
    'registros Exótica con potencial de invasión Riesgo Moderado/ Alto': (
        'registros_exoticas_riesgo_invasion_moderado_alto'
    ),
    'registros Trasplantada': 'registros_trasplantadas',
    'especies endemic': 'especies_endemicas',
    'especies migratory': 'especies_migratorias',
    'registros endemic': 'registros_endemicas',
    'registros migratory': 'registros_migratorias',
    'especies EX_IUCN': 'especies_amenazadas_global_ex',
    'especies EW_IUCN': 'especies_amenazadas_global_ew',
    'especies CR_IUCN': 'especies_amenazadas_global_cr',
    'especies EN_IUCN': 'especies_amenazadas_global_en',
    'especies VU_IUCN': 'especies_amenazadas_global_vu',
    'especies NT_IUCN': 'especies_amenazadas_global_nt',
    'especies LC_IUCN': 'especies_amenazadas_global_lc',
    'especies DD_IUCN': 'especies_amenazadas_global_dd',
    'especies LR/lc_IUCN': 'especies_amenazadas_global_lr_lc',
    'especies LR/nt_IUCN': 'especies_amenazadas_global_lr_nt',
    'registros EX_IUCN': 'registros_amenazadas_global_ex',
    'registros EW_IUCN': 'registros_amenazadas_global_ew',
    'registros CR_IUCN': 'registros_amenazadas_global_cr',
    'registros EN_IUCN': 'registros_amenazadas_global_en',
    'registros VU_IUCN': 'registros_amenazadas_global_vu',
    'registros NT_IUCN': 'registros_amenazadas_global_nt',
    'registros LC_IUCN': 'registros_amenazadas_global_lc',
    'registros DD_IUCN': 'registros_amenazadas_global_dd',
    'registros LR/lc_IUCN': 'registros_amenazadas_global_lr_lc',
    'registros LR/nt_IUCN': 'registros_amenazadas_global_lr_nt',
}

for _rt_hab, _rt_pfx in (
    ('continental', 'especies_continentales_'),
    ('marino', 'especies_marinas_'),
    ('salobre', 'especies_salobres_'),
):
    _rt_hab_reg = {'continental': 'Continentales', 'marino': 'Marinos', 'salobre': 'Salobres'}[_rt_hab]
    _rt_hab_sp = {'continental': 'Continentales', 'marino': 'Marinas', 'salobre': 'Salobres'}[_rt_hab]
    for _rt_leg, _rt_out in list(_REGION_TEMATICA_LEGACY_MAP.items()):
        if (
            _rt_leg.startswith('especies ')
            and 'Continentales' not in _rt_leg
            and 'Marinas' not in _rt_leg
            and 'Salobres' not in _rt_leg
        ):
            _rt_body = _rt_leg.split(' ', 1)[1]
            _REGION_TEMATICA_LEGACY_MAP[f'especies {_rt_hab_sp} {_rt_body}'] = (
                _rt_pfx + _rt_out[len('especies_') :]
            )
        if (
            _rt_leg.startswith('registros ')
            and 'Continentales' not in _rt_leg
            and 'Marinos' not in _rt_leg
            and 'Salobres' not in _rt_leg
        ):
            _rt_body = _rt_leg.split(' ', 1)[1]
            _rt_reg_out = _rt_out.replace('especies_', 'registros_')
            _REGION_TEMATICA_LEGACY_MAP[f'registros {_rt_hab_reg} {_rt_body}'] = (
                _rt_pfx.replace('especies_', 'registros_') + _rt_reg_out[len('registros_') :]
            )


def _build_region_tematica_metrics() -> list[_RegionTematicaMetric]:
    metrics: list[_RegionTematicaMetric] = []
    seen: set[str] = set()
    for legacy_key, output_col in _REGION_TEMATICA_LEGACY_MAP.items():
        if output_col in _REGION_TEMATICA_DERIVED_SOURCES or output_col in seen:
            continue
        metric = _region_tematica_metric_from_legacy(output_col, legacy_key)
        if metric is None:
            continue
        metrics.append(metric)
        seen.add(output_col)
    return metrics


def _region_tematica_field_condition(field: str, value: str | None) -> str:
    if value is None:
        return f'{field} IS NOT NULL'
    escaped = value.replace("'", "''")
    return f"{field} = '{escaped}'"


def _region_tematica_metric_agg_sql(metric: _RegionTematicaMetric, incluye_marino: bool) -> str | None:
    if metric.habitat and not incluye_marino:
        return f'NULL::bigint AS {metric.column}'

    conditions: list[str] = []
    if metric.habitat:
        conditions.append(_REGION_TEMATICA_HABITAT_SQL[metric.habitat])
    if metric.field:
        conditions.append(_region_tematica_field_condition(metric.field, metric.value))

    if metric.kind == 'registros' and not conditions and metric.column == 'registros_region_total':
        return f'COUNT(*)::bigint AS {metric.column}'

    where = ' AND '.join(conditions) if conditions else 'TRUE'

    if metric.kind == 'registros':
        return f'COUNT(*) FILTER (WHERE {where})::bigint AS {metric.column}'

    if metric.row_distinct_species and metric.habitat:
        hab_col = _REGION_TEMATICA_HABITAT_COL[metric.habitat]
        return (
            f'COUNT(DISTINCT ROW(species, {hab_col})) FILTER '
            f'(WHERE {_REGION_TEMATICA_SPECIES_VALID} AND {where})::bigint AS {metric.column}'
        )
    return (
        f'COUNT(DISTINCT species) FILTER (WHERE {_REGION_TEMATICA_SPECIES_VALID} AND {where})::bigint '
        f'AS {metric.column}'
    )


def _region_tematica_derived_select_sql(incluye_marino: bool) -> list[str]:
    exprs: list[str] = []
    for target, sources in _REGION_TEMATICA_DERIVED_SOURCES.items():
        is_habitat_derived = any(
            token in target for token in ('_continentales_', '_marinas_', '_salobres_')
        )
        if is_habitat_derived and not incluye_marino:
            exprs.append(f'NULL::bigint AS {target}')
            continue
        parts = [f'COALESCE(m.{src}, 0)' for src in sources]
        exprs.append(f'({" + ".join(parts)})::bigint AS {target}')
    return exprs


_REGION_TEMATICA_AGGREGATE_METRICS = _build_region_tematica_metrics()


def _build_wide_metric_select_sql(incluye_marino: bool) -> tuple[str, str, str]:
    """Arma agg_sql, metric_cols y derived_sql para MVs de formato ancho CCDM."""
    agg_exprs = [
        expr
        for metric in _REGION_TEMATICA_AGGREGATE_METRICS
        if (expr := _region_tematica_metric_agg_sql(metric, incluye_marino))
    ]
    agg_sql = ',\n            '.join(agg_exprs)
    derived_sql = ',\n        '.join(_region_tematica_derived_select_sql(incluye_marino))
    metric_cols = ',\n        '.join(f'm.{m.column}' for m in _REGION_TEMATICA_AGGREGATE_METRICS)
    return agg_sql, metric_cols, derived_sql


_REGION_TEMATICA_POR_REGION_LATERAL_SQL = f"""
        CROSS JOIN LATERAL (VALUES
            {_slug_region_lateral_values('b')}
        ) AS r(slug_region)
        WHERE r.slug_region IS NOT NULL
"""

_REGION_TEMATICA_POR_REGION_LATERAL_JOIN_SQL = f"""
        CROSS JOIN LATERAL (VALUES
            {_slug_region_lateral_values('b')}
        ) AS r(slug_region)
"""

_OCCURRENCIA_GEO_BASE_SQL = f"""
        SELECT
            ts.species,
            ts.flagtaxo,
            ts.kingdom,
            ts.phylum,
            ts."class",
            ts."order",
            ts.family,
            ts.genus,
            ts.ismarine,
            ts.isbrackish,
            ts.isterrestrial,
            ts.threatstatusmads,
            ts.threatstatusuicn,
            ts.cites,
            ts.invasive,
            ts.exotic,
            ts.exoticriskinvasion,
            ts.transplanted,
            ts.endemic,
            ts.migratory,
            COALESCE(gl.stateprovinceslug, dept.slug) AS dept_slug,
            COALESCE(gl.countyslug, muni.slug) AS muni_slug,
            gl.amazonregion,
            gl.reserve,
            gl.indigenousreserve,
            gl.dfybnucleus
        FROM "{DWC_INTEGRATED_TABLE}" i
        LEFT JOIN taxonomic_species_validation ts ON ts.id = i.taxonomic_species_id
        LEFT JOIN geo_locality_validation gl ON gl.id = i.locality_id
        LEFT JOIN geo_master_geography gm ON gm.id = gl.geo_master_geography_id
        LEFT JOIN geo_master_geography muni
            ON muni.id = CASE WHEN gm.subtype = 'municipio' THEN gm.id END
        LEFT JOIN geo_master_geography dept
            ON dept.id = COALESCE(
                muni.parent_id,
                CASE WHEN gm.subtype = 'departamento' THEN gm.id END
            )
"""

_POR_REGION_COLUMNS_SQL = """
            b.species,
            b.flagtaxo,
            b.ismarine,
            b.isbrackish,
            b.isterrestrial,
            b.threatstatusmads,
            b.threatstatusuicn,
            b.cites,
            b.invasive,
            b.exotic,
            b.exoticriskinvasion,
            b.transplanted,
            b.endemic,
            b.migratory,
            r.slug_region
"""

# Validación de especie referida a columnas de por_region (alias pr).
_REGION_TEMATICA_SPECIES_VALID_PR = """
    pr.species IS NOT NULL
    AND NULLIF(BTRIM(pr.species::text), '') IS NOT NULL
    AND pr.flagtaxo IS DISTINCT FROM 'Ausente en lista taxonómica'
""".strip()

# Expande cada ocurrencia a una dimensión temática por fila; '__total__' es el
# conteo geográfico base. Se descarta la categoría nula (species sin ese atributo).
_REGION_TEMATICA_DIM_LATERAL_SQL = """
        CROSS JOIN LATERAL (VALUES
            ('__total__', '__ALL__'::text),
            ('threatstatusmads', pr.threatstatusmads::text),
            ('cites', pr.cites::text),
            ('exotic', pr.exotic::text),
            ('invasive', pr.invasive::text),
            ('exoticriskinvasion', pr.exoticriskinvasion::text),
            ('transplanted', pr.transplanted::text),
            ('endemic', pr.endemic::text),
            ('migratory', pr.migratory::text),
            ('threatstatusuicn', pr.threatstatusuicn::text)
        ) AS d(dim, category)
"""

_REGION_TEMATICA_MV_SQL = f"""
    WITH base AS (
        {_OCCURRENCIA_GEO_BASE_SQL}
    ),
    por_region AS (
        SELECT
            {_POR_REGION_COLUMNS_SQL}
        FROM base b
        {_REGION_TEMATICA_POR_REGION_LATERAL_SQL}
    ),
    exploded AS (
        SELECT
            pr.slug_region,
            pr.species,
            ({_REGION_TEMATICA_SPECIES_VALID_PR}) AS species_valid,
            pr.isterrestrial IS NOT NULL AS h_cont,
            pr.ismarine IS NOT NULL AS h_mar,
            pr.isbrackish IS NOT NULL AS h_sal,
            d.dim,
            d.category
        FROM por_region pr
        {_REGION_TEMATICA_DIM_LATERAL_SQL}
        WHERE d.category IS NOT NULL
    ),
    agg AS (
        SELECT
            slug_region,
            dim,
            category,
            COUNT(*)::bigint AS reg_all,
            COUNT(*) FILTER (WHERE h_cont)::bigint AS reg_cont,
            COUNT(*) FILTER (WHERE h_mar)::bigint AS reg_mar,
            COUNT(*) FILTER (WHERE h_sal)::bigint AS reg_sal,
            COUNT(DISTINCT species) FILTER (WHERE species_valid)::bigint AS esp_all,
            COUNT(DISTINCT species) FILTER (WHERE species_valid AND h_cont)::bigint AS esp_cont,
            COUNT(DISTINCT species) FILTER (WHERE species_valid AND h_mar)::bigint AS esp_mar,
            COUNT(DISTINCT species) FILTER (WHERE species_valid AND h_sal)::bigint AS esp_sal
        FROM exploded
        GROUP BY GROUPING SETS ((slug_region, dim, category), (slug_region, dim))
    ),
    metricas AS (
        SELECT
            slug_region,
            {{pivot_sql}}
        FROM agg
        GROUP BY slug_region
    )
    SELECT
        m.slug_region,
        (
            SELECT MAX("date")::date
            FROM geo_master_geography
            WHERE "date" IS NOT NULL
        ) AS fecha_corte,
        e.estimada::bigint AS especies_region_estimadas,
        (CASE
            WHEN m.slug_region = 'colombia' THEN '86'
            WHEN e.estimada IS NOT NULL THEN '87'
        END)::text AS estimada_region_ref_id,
        {{metric_cols}},
        {{derived_sql}}
    FROM metricas m
    LEFT JOIN {CIFRAS_ESTIMADAS_DEPT_TABLE} e ON e.departamento = m.slug_region
"""

_TAXON_RANK_COLUMNS: tuple[tuple[str, str], ...] = (
    ('kingdom', 'kingdom'),
    ('phylum', 'phylum'),
    ('class', '"class"'),
    ('order', '"order"'),
    ('family', 'family'),
    ('genus', 'genus'),
    ('species', 'species'),
)

_REGION_GRUPO_MV_SQL = f"""
    WITH metricas AS (
        SELECT
            g.slug AS slug_grupo,
            g.grouptype AS tipo,
            ar.slug_region,
            {{rollup_sum_sql}}
        FROM {STAGING_AGG_TAXON_REGION_MV} ar
        INNER JOIN taxonomic_groups g
            ON g.taxonrank = ar.taxonrank
           AND g.taxon = ar.taxon
        WHERE g.grouptype IS NOT NULL
          AND BTRIM(g.grouptype) <> '-'
        GROUP BY g.slug, g.grouptype, ar.slug_region
    )
    SELECT
        m.slug_grupo,
        m.slug_region,
        m.tipo,
        {{metric_cols}},
        {{derived_sql}}
    FROM metricas m
"""


def _materialized_view_exists(db, name: str) -> bool:
    with db.connect() as conn:
        rows = conn.execute(
            """
            SELECT 1
            FROM pg_matviews
            WHERE schemaname = 'public'
              AND matviewname = %(name)s
            """,
            {'name': name},
        ).fetchall()
    return bool(rows)


def _build_rank_union_from_base_sql(agg_sql: str) -> str:
    """Agrega por rango taxonómico y región desde base geo (una pasada por rango)."""
    branches: list[str] = []
    for rank_name, col_expr in _TAXON_RANK_COLUMNS:
        branches.append(f"""
        SELECT
            '{rank_name}' AS taxonrank,
            {col_expr} AS taxon,
            r.slug_region,
            {agg_sql}
        FROM base b
        {_REGION_TEMATICA_POR_REGION_LATERAL_JOIN_SQL}
        WHERE r.slug_region IS NOT NULL
          AND {col_expr} IS NOT NULL
          AND NULLIF(BTRIM({col_expr}::text), '') IS NOT NULL
        GROUP BY {col_expr}, r.slug_region""")
    return f"""
    WITH base AS (
        {_OCCURRENCIA_GEO_BASE_SQL}
    )
    """ + '\n        UNION ALL'.join(branches)


def _build_staging_agg_taxon_region_mv_sql() -> str:
    """Arma SQL de staging_agg_taxon_region (por_rango materializado)."""
    agg_sql, _, _ = _build_wide_metric_select_sql(incluye_marino=True)
    return _build_rank_union_from_base_sql(agg_sql)


def _build_region_grupo_rollup_sum_sql() -> str:
    """Suma conteos por rango hacia slug_grupo (estilo legacy sección 14)."""
    return ',\n            '.join(
        f'SUM(ar.{metric.column})::bigint AS {metric.column}'
        for metric in _REGION_TEMATICA_AGGREGATE_METRICS
    )


_REGION_TEMATICA_PIVOT_SRC = {
    ('registros', None): 'reg_all',
    ('registros', 'continental'): 'reg_cont',
    ('registros', 'marino'): 'reg_mar',
    ('registros', 'salobre'): 'reg_sal',
    ('especies', None): 'esp_all',
    ('especies', 'continental'): 'esp_cont',
    ('especies', 'marino'): 'esp_mar',
    ('especies', 'salobre'): 'esp_sal',
}


def _region_tematica_pivot_expr(metric: _RegionTematicaMetric) -> str:
    """Extrae una métrica ancha desde el agregado largo (agg)."""
    src = _REGION_TEMATICA_PIVOT_SRC[(metric.kind, metric.habitat)]
    dim = metric.field or '__total__'
    if metric.value is None:
        # Fila de total por dimensión (grouping set sin category).
        cat_cond = 'category IS NULL'
    else:
        escaped = metric.value.replace("'", "''")
        cat_cond = f"category = '{escaped}'"
    return (
        f"COALESCE(MAX({src}) FILTER (WHERE dim = '{dim}' AND {cat_cond}), 0)::bigint "
        f"AS {metric.column}"
    )


def _build_region_tematica_pivot_sql() -> str:
    """Arma las 232 columnas anchas por pivote sobre el agregado largo."""
    return ',\n            '.join(
        _region_tematica_pivot_expr(m) for m in _REGION_TEMATICA_AGGREGATE_METRICS
    )


def _build_region_tematica_mv_sql() -> str:
    """Arma SQL de region_tematica (CCDM, todas las regiones en una MV)."""
    _, metric_cols, derived_sql = _build_wide_metric_select_sql(incluye_marino=True)
    return _REGION_TEMATICA_MV_SQL.format(
        pivot_sql=_build_region_tematica_pivot_sql(),
        metric_cols=metric_cols,
        derived_sql=derived_sql,
    )


def _build_region_grupo_mv_sql() -> str:
    """Arma SQL de region_grupo (rollup desde staging_agg_taxon_region)."""
    _, metric_cols, derived_sql = _build_wide_metric_select_sql(incluye_marino=True)
    rollup_sum_sql = _build_region_grupo_rollup_sum_sql()
    return _REGION_GRUPO_MV_SQL.format(
        rollup_sum_sql=rollup_sum_sql,
        metric_cols=metric_cols,
        derived_sql=derived_sql,
    )


_LEGACY_REGION_TEMATICA_MVS = tuple(
    f'region_tematica_{nivel.lower()}' for nivel in ('CCDM', 'CSDM', 'DCDM', 'DSDM', 'MCDM', 'MSDM')
)


def _drop_region_grupo_dependent_mvs(conn) -> None:
    """Elimina MVs que dependen de staging_agg_taxon_region."""
    conn.execute(f'DROP MATERIALIZED VIEW IF EXISTS {REGION_GRUPO_MV}')


def create_staging_agg_taxon_region_materialized_view(db) -> int:
    """Crea MV staging_agg_taxon_region: agregados por taxonrank, taxon y slug_region."""
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
        _drop_region_grupo_dependent_mvs(conn)
        conn.execute(f'DROP MATERIALIZED VIEW IF EXISTS {STAGING_OCURRENCIA_GEO_MV}')
        conn.execute(f'DROP MATERIALIZED VIEW IF EXISTS {STAGING_AGG_TAXON_REGION_MV}')
        conn.execute(
            f'CREATE MATERIALIZED VIEW {STAGING_AGG_TAXON_REGION_MV} AS '
            f'{_build_staging_agg_taxon_region_mv_sql()}'
        )
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{STAGING_AGG_TAXON_REGION_MV}_taxon '
            f'ON {STAGING_AGG_TAXON_REGION_MV} (taxonrank, taxon)'
        )
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{STAGING_AGG_TAXON_REGION_MV}_taxon_region '
            f'ON {STAGING_AGG_TAXON_REGION_MV} (taxonrank, taxon, slug_region)'
        )
        conn.commit()
        total = conn.execute(
            f'SELECT COUNT(*) FROM {STAGING_AGG_TAXON_REGION_MV}'
        ).fetchall()[0][0]
    logger.info('Vista materializada %s creada (%s filas)', STAGING_AGG_TAXON_REGION_MV, total)
    print(f'Vista materializada {STAGING_AGG_TAXON_REGION_MV}: {total:,} filas')
    return total


def _ensure_staging_agg_taxon_region(db) -> None:
    if not _materialized_view_exists(db, STAGING_AGG_TAXON_REGION_MV):
        raise ValueError(
            f'Falta MV {STAGING_AGG_TAXON_REGION_MV}. '
            f'Ejecute sin --skip-staging-agg-taxon-region o créela antes.'
        )


def create_region_tematica_materialized_view(db) -> int:
    """Crea MV region_tematica: una fila por slug_region (nacional, depto, muni)."""
    required = (
        DWC_INTEGRATED_TABLE,
        'taxonomic_species_validation',
        'geo_locality_validation',
        'geo_master_geography',
        CIFRAS_ESTIMADAS_DEPT_TABLE,
    )
    missing = [name for name in required if not table_exists(db, name)]
    if missing:
        raise ValueError(f'Faltan tablas requeridas: {", ".join(missing)}')

    with db.connect() as conn:
        # Tuning local (valores del .env): la agregación larga con COUNT(DISTINCT)
        # y GROUPING SETS necesita más work_mem para evitar volcados a disco.
        # El paralelismo se controla por MAX_PARALLEL_WORKERS_MV
        # (WSL/Docker con /dev/shm pequeño: 0; Linux nativo: 4) para evitar DiskFull.
        conn.execute(f"SET LOCAL work_mem = '{_WORK_MEM}'")
        conn.execute(f'SET LOCAL max_parallel_workers_per_gather = {_MAX_PARALLEL_WORKERS_MV}')
        for legacy_mv in _LEGACY_REGION_TEMATICA_MVS:
            conn.execute(f'DROP MATERIALIZED VIEW IF EXISTS {legacy_mv}')
        conn.execute(f'DROP MATERIALIZED VIEW IF EXISTS {REGION_TEMATICA_MV}')
        conn.execute(
            f'CREATE MATERIALIZED VIEW {REGION_TEMATICA_MV} AS {_build_region_tematica_mv_sql()}'
        )
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{REGION_TEMATICA_MV}_slug_region '
            f'ON {REGION_TEMATICA_MV} (slug_region)'
        )
        conn.commit()
        total = conn.execute(f'SELECT COUNT(*) FROM {REGION_TEMATICA_MV}').fetchall()[0][0]
    logger.info('Vista materializada %s creada (%s filas)', REGION_TEMATICA_MV, total)
    print(f'Vista materializada {REGION_TEMATICA_MV}: {total:,} filas')
    return total


def create_region_grupo_materialized_view(db) -> int:
    """Crea MV region_grupo: una fila por slug_grupo y slug_region (nacional, depto, muni)."""
    required = ('taxonomic_groups',)
    missing = [name for name in required if not table_exists(db, name)]
    if missing:
        raise ValueError(f'Faltan tablas requeridas: {", ".join(missing)}')
    _ensure_staging_agg_taxon_region(db)

    with db.connect() as conn:
        conn.execute(f'DROP MATERIALIZED VIEW IF EXISTS {REGION_GRUPO_MV}')
        conn.execute(
            f'CREATE MATERIALIZED VIEW {REGION_GRUPO_MV} AS {_build_region_grupo_mv_sql()}'
        )
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{REGION_GRUPO_MV}_slug_grupo '
            f'ON {REGION_GRUPO_MV} (slug_grupo)'
        )
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{REGION_GRUPO_MV}_slug_region '
            f'ON {REGION_GRUPO_MV} (slug_region)'
        )
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{REGION_GRUPO_MV}_grupo_region '
            f'ON {REGION_GRUPO_MV} (slug_grupo, slug_region)'
        )
        conn.commit()
        total = conn.execute(f'SELECT COUNT(*) FROM {REGION_GRUPO_MV}').fetchall()[0][0]
    logger.info('Vista materializada %s creada (%s filas)', REGION_GRUPO_MV, total)
    print(f'Vista materializada {REGION_GRUPO_MV}: {total:,} filas')
    return total


def create_publicador_materialized_view(db):
    """Crea MV publicador: catálogo de publicadores con cifras nacionales."""
    required = (
        DWC_INTEGRATED_TABLE,
        'taxonomic_species_validation',
        'gbif_publishers',
    )
    missing = [name for name in required if not table_exists(db, name)]
    if missing:
        raise ValueError(f'Faltan tablas requeridas: {", ".join(missing)}')
    with db.connect() as conn:
        conn.execute(f'DROP MATERIALIZED VIEW IF EXISTS {PUBLICADOR_MV}')
        conn.execute(f'CREATE MATERIALIZED VIEW {PUBLICADOR_MV} AS {_PUBLICADOR_MV_SQL}')
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{PUBLICADOR_MV}_slug '
            f'ON {PUBLICADOR_MV} (slug)'
        )
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{PUBLICADOR_MV}_tipo_publicador '
            f'ON {PUBLICADOR_MV} (tipo_publicador)'
        )
        conn.commit()
        total = conn.execute(f'SELECT COUNT(*) FROM {PUBLICADOR_MV}').fetchall()[0][0]
    logger.info('Vista materializada %s creada (%s filas)', PUBLICADOR_MV, total)
    print(f'Vista materializada {PUBLICADOR_MV}: {total:,} filas')
    return total


def create_region_publicador_materialized_view(db):
    """Crea MV region_publicador: cifras por slug_region y publicador (todos los niveles)."""
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
        conn.execute(f'DROP MATERIALIZED VIEW IF EXISTS {REGION_PUBLICADOR_MV}')
        conn.execute(
            f'CREATE MATERIALIZED VIEW {REGION_PUBLICADOR_MV} AS {_REGION_PUBLICADOR_MV_SQL}'
        )
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{REGION_PUBLICADOR_MV}_slug_region '
            f'ON {REGION_PUBLICADOR_MV} (slug_region)'
        )
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{REGION_PUBLICADOR_MV}_slug_publicador '
            f'ON {REGION_PUBLICADOR_MV} (slug_publicador)'
        )
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{REGION_PUBLICADOR_MV}_region_publicador '
            f'ON {REGION_PUBLICADOR_MV} (slug_region, slug_publicador)'
        )
        conn.commit()
        total = conn.execute(f'SELECT COUNT(*) FROM {REGION_PUBLICADOR_MV}').fetchall()[0][0]
    logger.info('Vista materializada %s creada (%s filas)', REGION_PUBLICADOR_MV, total)
    print(f'Vista materializada {REGION_PUBLICADOR_MV}: {total:,} filas')
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
    parser.add_argument('--skip-especie', action='store_true', help='Omitir MV especie')
    parser.add_argument('--skip-especie-meta', action='store_true', help='Omitir MV especie_meta')
    parser.add_argument('--skip-especie-grupo', action='store_true', help='Omitir MV especie_grupo')
    parser.add_argument('--skip-especie-region', action='store_true', help='Omitir MV especie_region')
    parser.add_argument('--skip-especie-tematica', action='store_true', help='Omitir MV especie_tematica')
    parser.add_argument('--skip-cifras-totales', action='store_true', help='Omitir MV cifras_totales')
    parser.add_argument('--skip-geografia-resumen', action='store_true', help='Omitir MV geografia_resumen')
    parser.add_argument('--skip-region-tematica', action='store_true', help='Omitir MV region_tematica')
    parser.add_argument('--skip-region-grupo', action='store_true', help='Omitir MV region_grupo')
    parser.add_argument(
        '--skip-staging-agg-taxon-region',
        action='store_true',
        help='No recrear staging_agg_taxon_region (requiere MV existente para region_grupo)',
    )
    parser.add_argument('--skip-publicador', action='store_true', help='Omitir MV publicador')
    parser.add_argument('--skip-region-publicador', action='store_true', help='Omitir MV region_publicador')
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

        if not args.skip_especie:
            create_especie_materialized_view(db)

        if not args.skip_especie_meta:
            create_especie_meta_materialized_view(db)

        if not args.skip_especie_grupo:
            create_especie_grupo_materialized_view(db)

        if not args.skip_especie_region:
            create_especie_region_materialized_view(db)

        if not args.skip_especie_tematica:
            create_especie_tematica_materialized_view(db)

        if not args.skip_cifras_totales:
            create_cifras_totales_materialized_view(db)

        if not args.skip_geografia_resumen:
            create_geografia_resumen_materialized_view(db)

        if not args.skip_region_tematica:
            create_region_tematica_materialized_view(db)

        if not args.skip_region_grupo:
            if args.skip_staging_agg_taxon_region:
                _ensure_staging_agg_taxon_region(db)
            else:
                create_staging_agg_taxon_region_materialized_view(db)
            create_region_grupo_materialized_view(db)

        if not args.skip_publicador:
            create_publicador_materialized_view(db)

        if not args.skip_region_publicador:
            create_region_publicador_materialized_view(db)

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
