-- 1. Codigo para correr en bigquery que cea la tala fanasma

CREATE TABLE `neo-rank-tracker.rank_tracker_neo.tbl_dashboard_cache`
(
    fecha DATE,
    id_proyecto STRING,
    dominio STRING,
    rango_grupo STRING,
    tipo_resultado STRING,
    cantidad_keywords INT64
)
PARTITION BY fecha
CLUSTER BY id_proyecto, dominio;

-- 2. Migrar data a las tablas fantasma
INSERT INTO `neo-rank-tracker.rank_tracker_neo.tbl_dashboard_cache`
SELECT
    PARSE_DATE('%Y-%m-%d', fecha) as fecha,
    id_proyecto,
    dominio,
    rango_grupo,
    tipo_resultado,
    COUNT(DISTINCT keyword) as cantidad_keywords
FROM
    `neo-rank-tracker.rank_tracker_neo.tbl_keywords_organic_final`
WHERE
    fecha IS NOT NULL
GROUP BY
    1, 2, 3, 4, 5;
