-- description: Monitoreo de adopción de Smart TV. Identifica y cuantifica a los usuarios que realizan su primera reproducción histórica en dispositivos de TV para entender el crecimiento de la base instalada en pantallas grandes. 
-- domain: behaviour 
-- product: mplay 
-- use_case: device adoption / growth analysis 
-- grain: mes 
-- time_grain: monthly 
-- date_column: first_tv_ds (métrica basada en el MIN de DS) 
-- date_filter: first_tv_ds between '2025-04-01' and '2025-09-30' 
-- threshold_rule: playback_time > 20s y DEVICE_PLATFORM LIKE '/tv%' 
-- metrics: 
-- - viewers_primera_vez_tv: Cantidad de usuarios únicos cuya primera visualización en TV ocurrió en el mes analizado. 
-- tables_read: 
-- - WHOWNER.BT_MKT_MPLAY_PLAYS 
-- joins: 
-- - N/A (Self-aggregation) 
-- owner: data_team
WITH tv_viewer_plays AS (
  SELECT
    USER_ID,
    DS
  FROM WHOWNER.BT_MKT_MPLAY_PLAYS
  WHERE
    USER_ID IS NOT NULL
    AND DEVICE_PLATFORM LIKE '/tv%'
    AND PLAYBACK_TIME_MILLISECONDS > 20000
    AND DS <= DATE '2025-09-30'
),
first_tv_view AS (
  SELECT
    USER_ID,
    MIN(DS) AS first_tv_ds
  FROM tv_viewer_plays
  GROUP BY USER_ID
)
SELECT
  FORMAT_DATE('%Y-%m', first_tv_ds) AS mes,
  COUNT(DISTINCT USER_ID) AS viewers_primera_vez_tv
FROM first_tv_view
WHERE first_tv_ds BETWEEN DATE '2025-04-01' AND DATE '2025-09-30'
GROUP BY mes
ORDER BY mes;