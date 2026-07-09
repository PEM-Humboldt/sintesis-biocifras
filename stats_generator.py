# Autor: Diego Moreno-Vargas (github.com/damorenov)
"""
Estadísticas de síntesis: vistas geo, catálogo de especies, tabla integrada vigente y cifras estimadas por temática.

- MV especie: catálogo taxonómico desde taxonomic_species_validation (slug + rangos).
- MV especie_meta: metadatos de especie (vernacular, URLs) desde taxonomic_species_meta por slug.
- MV especie_grupo: relación especie ↔ grupo biológico/interés desde taxonomic_groups.
- MV especie_region: registros por especie y región (nacional, departamental, municipal).
- MV especie_tematica: relación DISTINCT especie ↔ región ↔ temática (slug_tematica).
- MV cifras_totales: conteos globales por nivel CDM (registros/especies y hábitat en niveles marinos).
- MV geografia_resumen: conteos por nivel CDM y slug_region (departamentos o municipios).
- MV publicador: catálogo de publicadores con registros (integrada total) y especies (validadas).
- MV region_publicador: cifras por slug_region y publicador (nacional, departamental, municipal).

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
ESPECIE_MV = 'especie'
ESPECIE_META_MV = 'especie_meta'
ESPECIE_GRUPO_MV = 'especie_grupo'
ESPECIE_REGION_MV = 'especie_region'
ESPECIE_TEMATICA_MV = 'especie_tematica'
CIFRAS_TOTALES_MV = 'cifras_totales'
GEOGRAFIA_RESUMEN_MV = 'geografia_resumen'
PUBLICADOR_MV = 'publicador'
REGION_PUBLICADOR_MV = 'region_publicador'

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

_ESPECIE_REGION_MV_SQL = f"""
    WITH base AS (
        SELECT
            ts.slugspecies AS slug_especie,
            COALESCE(gl.stateprovinceslug, dept.slug) AS dept_slug,
            COALESCE(gl.countyslug, muni.slug) AS muni_slug
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
            ('colombia'),
            (b.dept_slug),
            (b.muni_slug)
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
            ('colombia'),
            (b.dept_slug),
            (b.muni_slug)
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
    WITH enriched AS (
        SELECT
            i.gbifid,
            ts.id AS species_id,
            COALESCE(gl.stateprovinceslug, dept.slug) AS dept_slug,
            COALESCE(gl.countyslug, muni.slug) AS muni_slug
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
    por_nivel AS (
        SELECT
            n.nivel,
            n.incluye_marino,
            e.species_id,
            ts.species,
            ts.ismarine,
            ts.isbrackish,
            ts.isterrestrial
        FROM enriched e
        INNER JOIN taxonomic_species_validation ts ON ts.id = e.species_id
        {_CIFRAS_TOTALES_NIVEL_LATERAL_SQL}
        WHERE CASE n.alcance
            WHEN 'nacional' THEN true
            WHEN 'depto' THEN e.dept_slug IS NOT NULL
            WHEN 'muni' THEN e.muni_slug IS NOT NULL
        END
    )
    SELECT
        nivel,
        COUNT(*)::bigint AS registros,
        CASE WHEN bool_or(incluye_marino) THEN
            COUNT(*) FILTER (WHERE isterrestrial = 'Terrestrial')::int
        END AS registros_continentales,
        CASE WHEN bool_or(incluye_marino) THEN
            COUNT(*) FILTER (WHERE ismarine = 'Marine')::int
        END AS registros_marinos,
        CASE WHEN bool_or(incluye_marino) THEN
            COUNT(*) FILTER (WHERE isbrackish = 'Brackish')::int
        END AS registros_salobres,
        COUNT(DISTINCT species_id) FILTER (
            WHERE species IS NOT NULL AND NULLIF(BTRIM(species::text), '') IS NOT NULL
        )::int AS especies,
        CASE WHEN bool_or(incluye_marino) THEN
            COUNT(DISTINCT species_id) FILTER (
                WHERE isterrestrial = 'Terrestrial'
                  AND species IS NOT NULL AND NULLIF(BTRIM(species::text), '') IS NOT NULL
            )::int
        END AS especies_continentales,
        CASE WHEN bool_or(incluye_marino) THEN
            COUNT(DISTINCT species_id) FILTER (
                WHERE ismarine = 'Marine'
                  AND species IS NOT NULL AND NULLIF(BTRIM(species::text), '') IS NOT NULL
            )::int
        END AS especies_marinas,
        CASE WHEN bool_or(incluye_marino) THEN
            COUNT(DISTINCT species_id) FILTER (
                WHERE isbrackish = 'Brackish'
                  AND species IS NOT NULL AND NULLIF(BTRIM(species::text), '') IS NOT NULL
            )::int
        END AS especies_salobres
    FROM por_nivel
    GROUP BY nivel
    ORDER BY nivel
"""

_GEOGRAFIA_RESUMEN_MV_SQL = f"""
    WITH enriched AS (
        SELECT
            i.gbifid,
            ts.id AS species_id,
            COALESCE(gl.stateprovinceslug, dept.slug) AS dept_slug,
            COALESCE(gl.countyslug, muni.slug) AS muni_slug
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
    por_geo AS (
        SELECT
            n.nivel,
            n.incluye_marino,
            CASE n.granularidad
                WHEN 'depto' THEN e.dept_slug
                WHEN 'muni' THEN e.muni_slug
            END AS slug_region,
            e.species_id,
            ts.species,
            ts.ismarine,
            ts.isbrackish,
            ts.isterrestrial
        FROM enriched e
        INNER JOIN taxonomic_species_validation ts ON ts.id = e.species_id
        {_GEOGRAFIA_NIVEL_LATERAL_SQL}
        WHERE CASE n.alcance
            WHEN 'nacional' THEN true
            WHEN 'depto' THEN e.dept_slug IS NOT NULL
            WHEN 'muni' THEN e.muni_slug IS NOT NULL
        END
          AND CASE n.granularidad
            WHEN 'depto' THEN e.dept_slug IS NOT NULL
            WHEN 'muni' THEN e.muni_slug IS NOT NULL
        END
    )
    SELECT
        nivel,
        slug_region,
        COUNT(*)::bigint AS registros,
        CASE WHEN bool_or(incluye_marino) THEN
            COUNT(*) FILTER (WHERE isterrestrial = 'Terrestrial')::int
        END AS registros_continentales,
        CASE WHEN bool_or(incluye_marino) THEN
            COUNT(*) FILTER (WHERE ismarine = 'Marine')::int
        END AS registros_marinos,
        CASE WHEN bool_or(incluye_marino) THEN
            COUNT(*) FILTER (WHERE isbrackish = 'Brackish')::int
        END AS registros_salobres,
        COUNT(DISTINCT species_id) FILTER (
            WHERE species IS NOT NULL AND NULLIF(BTRIM(species::text), '') IS NOT NULL
        )::int AS especies,
        CASE WHEN bool_or(incluye_marino) THEN
            COUNT(DISTINCT species_id) FILTER (
                WHERE isterrestrial = 'Terrestrial'
                  AND species IS NOT NULL AND NULLIF(BTRIM(species::text), '') IS NOT NULL
            )::int
        END AS especies_continentales,
        CASE WHEN bool_or(incluye_marino) THEN
            COUNT(DISTINCT species_id) FILTER (
                WHERE ismarine = 'Marine'
                  AND species IS NOT NULL AND NULLIF(BTRIM(species::text), '') IS NOT NULL
            )::int
        END AS especies_marinas,
        CASE WHEN bool_or(incluye_marino) THEN
            COUNT(DISTINCT species_id) FILTER (
                WHERE isbrackish = 'Brackish'
                  AND species IS NOT NULL AND NULLIF(BTRIM(species::text), '') IS NOT NULL
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
            ('colombia'),
            (e.dept_slug),
            (e.muni_slug)
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
