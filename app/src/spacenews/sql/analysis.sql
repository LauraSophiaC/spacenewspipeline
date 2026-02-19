-- 1) Tendencias de temas por mes (DW)
SELECT
  dd.year,
  dd.month,
  dt.topic_name,
  SUM(ft.items_count) AS total_items
FROM fact_trends_daily ft
JOIN dim_date dd   ON dd.date_key = ft.date_key
JOIN dim_topic dt  ON dt.topic_key = ft.topic_key
GROUP BY dd.year, dd.month, dt.topic_name
ORDER BY dd.year, dd.month, total_items DESC;

-- 2) Fuentes más activas por mes (desde fact_content)
-- (influencia aquí la aproximamos como "volumen de publicaciones")
SELECT
  dd.year,
  dd.month,
  ds.news_site,
  ds.content_type,
  COUNT(*) AS total_items
FROM fact_content fc
JOIN dim_date dd     ON dd.date_key = fc.date_key
JOIN dim_source ds   ON ds.source_key = fc.source_key
GROUP BY dd.year, dd.month, ds.news_site, ds.content_type
ORDER BY dd.year, dd.month, total_items DESC;

-- 3) Top 10 fuentes más activas (global)
SELECT
  ds.news_site,
  ds.content_type,
  COUNT(*) AS total_items
FROM fact_content fc
JOIN dim_source ds ON ds.source_key = fc.source_key
GROUP BY ds.news_site, ds.content_type
ORDER BY total_items DESC
LIMIT 10;
