# falta poner cada cuanto se cargarán las tablas fantasmas 
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
    
    PARSE_DATE('%Y-%m-%d', fecha) = DATE_SUB(CURRENT_DATE('America/Bogota'), INTERVAL 1 DAY)
GROUP BY
    1, 2, 3, 4, 5;
