-- description: Cálculo de crecimiento de visitors en Smart TV (LG) y atribución de campañas (TV vs Banners/Pushes) mediante proxy de crecimiento mensual
-- domain: behaviour
-- product: mplay
-- use_case: attribution_analysis
-- grain: metric
-- time_grain: aggregated_period
-- date_column: s.DS
-- date_filter: between
-- threshold_rule: playback_time >= 20s
-- metrics:
-- - VISITORS: usuarios únicos por día con sesiones válidas
-- - TSV: segundos totales reproducidos por sesión
-- - TVM: minutos totales reproducidos (threshold 20s)
-- - DOWNLOADS: instalaciones netas de la app
-- - GROWTH_TOTAL: crecimiento porcentual durante semana de campaña
-- - ATTRIBUTED_GROWTH_TV: crecimiento atribuido a campaña de TV
-- - ATTRIBUTED_GROWTH_OTHER: crecimiento atribuido a banners y pushes
-- tables_read:
-- - WHOWNER.BT_MKT_MPLAY_SESSION
-- - WHOWNER.BT_MKT_MPLAY_PLAYS
-- - WHOWNER.BT_MKT_MPLAY_INSTALLS
-- joins:
-- - SESSION.SIT_SITE_ID = PLAYS.SIT_SITE_ID
-- - SESSION.USER_ID = PLAYS.USER_ID
-- - SESSION.SESSION_ID = PLAYS.SESSION_ID
-- owner: data_team

DECLARE SITES ARRAY<STRING>;
DECLARE date_from DATE;
DECLARE date_to DATE;

SET SITES = ['MLA', 'MLB', 'MLM'];
SET date_from = '2025-04-01';
SET date_to = CURRENT_DATE();

WITH SESSIONS AS (
  SELECT
    s.SIT_SITE_ID,
    DATE_TRUNC(s.ds, WEEK(MONDAY)) AS fecha_week,
    DATE_TRUNC(s.ds, WEEK(THURSDAY)) AS fecha_week_thursday,
    EXTRACT(DAYOFWEEK FROM DS) AS day_week,
    s.ds,
    ORIGIN_PATH AS FIRST_EVENT_SOURCE,
    FIRST_TRACK AS FIRST_EVENT_PATH,
    FIRST_PLAY_DATETIME AS PLAY_TIMESTAMP,
    s.USER_ID,
    s.SESSION_ID AS MELIDATA_SESSION_ID,
    s.DEVICE_PLATFORM,
    IF(
      (
        (S.HAS_SEARCH IS TRUE OR S.HAS_VCP IS TRUE OR S.HAS_VCM IS TRUE OR HAS_PLAY IS TRUE) 
        OR TOTAL_FEED_IMPRESSIONS > 1
      ),
      TRUE,
      FALSE
    ) AS FLAG_VALID_VISIT,
    HAS_PLAY,
    S.TOTAL_SESSION_MILLISECOND/1000 AS session_time_sec
  FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_SESSION` AS s
  WHERE s.ds >= date_from 
    AND s.ds < date_to
    AND s.SIT_SITE_ID IN UNNEST(SITES)
    AND LOWER(DEVICE_PLATFORM) LIKE '/tv%'
  GROUP BY ALL
),
SESSION_PLAY AS (
  SELECT DISTINCT
    s.SIT_SITE_ID,
    s.fecha_week,
    s.fecha_week_thursday,
    s.day_week,
    S.DS,
    s.FIRST_EVENT_SOURCE,
    s.FIRST_EVENT_PATH,
    s.PLAY_TIMESTAMP,
    s.USER_ID,
    s.MELIDATA_SESSION_ID,
    s.FLAG_VALID_VISIT,
    s.HAS_PLAY,
    EXTRACT(HOUR FROM P.START_PLAY_TIMESTAMP) AS HOUR_PLAY,
    CASE
      WHEN UPPER(s.DEVICE_PLATFORM) LIKE '%TIZEN%' THEN 'SAMSUNG'
      WHEN UPPER(s.DEVICE_PLATFORM) LIKE '%ANDR%' THEN 'ANDROID'
      WHEN UPPER(s.DEVICE_PLATFORM) LIKE '%WEB%' THEN 'LG'
    ELSE 'UNKNOWN'
    END AS DEVICE_PLATFORM,
    s.session_time_sec,
    SUM(P.PLAYBACK_TIME_MILLISECONDS/1000) AS TSV,
    SUM(P.PLAYBACK_TIME_MILLISECONDS/60000) AS TVM 
  FROM SESSIONS AS S 
  LEFT JOIN `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` AS P 
    ON S.SIT_SITE_ID = P.SIT_SITE_ID
    AND s.USER_ID = P.USER_ID
    AND S.MELIDATA_SESSION_ID = P.SESSION_ID
    AND P.PLAYBACK_TIME_MILLISECONDS/1000 >= 20                                               
  GROUP BY ALL
),
DOWNLOADS AS (
  SELECT 
    SIT_SITE_ID,
    DATE_TRUNC(DS, WEEK(MONDAY)) AS WEEK_ID,
    DS,
    PLATFORM,
    SUM (NET_APP_INSTALLS) AS DOWNLOADS
  FROM meli-bi-data.WHOWNER.BT_MKT_MPLAY_INSTALLS
  WHERE SIT_SITE_ID IN ('MLA','MLB','MLM','MLC','MCO','MPE','MLU','MEC')
  AND DS BETWEEN '2025-04-01' AND CURRENT_DATE()
  GROUP BY ALL
  ORDER BY DS
),
VIEWERS AS (
  SELECT DISTINCT
    P.SIT_SITE_ID,
    DS,
    DATE_TRUNC(DS, MONTH) AS MONTH_ID,
    CASE
      WHEN UPPER(DEVICE_PLATFORM) LIKE '%TIZEN%' THEN 'SAMSUNG'
      WHEN UPPER(DEVICE_PLATFORM) LIKE '%ANDR%' THEN 'ANDROID'
      WHEN UPPER(DEVICE_PLATFORM) LIKE '%WEB%' THEN 'LG'
    ELSE 'UNKNOWN'
    END AS PLATFORM,
    COUNT(DISTINCT P.USER_ID) AS VIEWERS_SMART,
    SUM(PLAYBACK_TIME_MILLISECONDS/60000) AS TVM_SMART
  FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` P
  LEFT JOIN (
    SELECT
      USER_ID,
      SIT_SITE_ID,
      MIN(DS) AS FIRST_DATE
    FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` AS P
    WHERE P.PLAYBACK_TIME_MILLISECONDS/1000 >= 20
    GROUP BY ALL
  ) AS FP ON FP.USER_ID = P.USER_ID AND FP.SIT_SITE_ID = P.SIT_SITE_ID
  WHERE LOWER(DEVICE_PLATFORM) LIKE '/tv%'
  AND DS BETWEEN '2025-04-01' AND CURRENT_DATE()
  AND PLAYBACK_TIME_MILLISECONDS/1000 >=20
  GROUP BY ALL
  ORDER BY MONTH_ID
),
-- 1. CÁLCULO DEL PROXY DE ATRIBUCIÓN
visitors_by_day AS (
  SELECT
    ds,
    COUNT(DISTINCT s.USER_ID) AS Visitors
  FROM SESSION_PLAY s
  WHERE
    s.DEVICE_PLATFORM = 'LG'
    AND ds BETWEEN '2025-05-01' AND '2025-07-31'
  GROUP BY
    1
),
mayo_summary AS (
  SELECT
    SUM(CASE WHEN ds BETWEEN '2025-05-18' AND '2025-05-24' THEN Visitors ELSE 0 END) AS campaign_visitors,
    AVG(CASE WHEN ds NOT BETWEEN '2025-05-18' AND '2025-05-24' THEN Visitors ELSE NULL END) AS control_avg_visitors
  FROM visitors_by_day
  WHERE EXTRACT(MONTH FROM ds) = 5
),
junio_summary AS (
  SELECT
    SUM(CASE WHEN ds BETWEEN '2025-06-23' AND '2025-06-29' THEN Visitors ELSE 0 END) AS campaign_visitors,
    AVG(CASE WHEN ds NOT BETWEEN '2025-06-23' AND '2025-06-29' THEN Visitors ELSE NULL END) AS control_avg_visitors
  FROM visitors_by_day
  WHERE EXTRACT(MONTH FROM ds) = 6
),
julio_summary AS (
  SELECT
    SUM(CASE WHEN ds BETWEEN '2025-07-24' AND '2025-07-30' THEN Visitors ELSE 0 END) AS campaign_visitors,
    AVG(CASE WHEN ds NOT BETWEEN '2025-07-24' AND '2025-07-30' THEN Visitors ELSE NULL END) AS control_avg_visitors
  FROM visitors_by_day
  WHERE EXTRACT(MONTH FROM ds) = 7
),
monthly_growth AS (
  SELECT
    'Mayo' AS Mes,
    (SELECT (campaign_visitors / (control_avg_visitors * 7)) - 1 FROM mayo_summary) AS crecimiento_porcentual
  UNION ALL
  SELECT
    'Junio' AS Mes,
    (SELECT (campaign_visitors / (control_avg_visitors * 7)) - 1 FROM junio_summary) AS crecimiento_porcentual
  UNION ALL
  SELECT
    'Julio' AS Mes,
    (SELECT (campaign_visitors / (control_avg_visitors * 7)) - 1 FROM julio_summary) AS crecimiento_porcentual
),
proxy_calculation AS (
  SELECT
    AVG(crecimiento_porcentual) AS proxy_atribucion_visitors
  FROM monthly_growth
),
-- 2. ANÁLISIS DE AGOSTO
agosto_visitors AS (
  SELECT
    ds,
    COUNT(DISTINCT s.USER_ID) AS Visitors
  FROM SESSION_PLAY s
  WHERE
    s.DEVICE_PLATFORM = 'LG'
    AND ds BETWEEN '2025-08-01' AND '2025-08-31'
  GROUP BY 1
),
-- ANÁLISIS DE AGOSTO
agosto_summary AS (
  SELECT
    SUM(CASE WHEN ds BETWEEN '2025-08-21' AND '2025-08-27' THEN Visitors ELSE 0 END) AS campana_agosto_visitors,
    AVG(
      CASE WHEN ds NOT BETWEEN '2025-08-21' AND '2025-08-27' THEN Visitors ELSE NULL END
    ) AS control_agosto_avg_visitors
  FROM agosto_visitors
),
-- 3. CÁLCULO FINAL DE ATRIBUCIÓN
final_atribucion AS (
  SELECT
    proxy.proxy_atribucion_visitors,
    agosto.campana_agosto_visitors,
    agosto.control_agosto_avg_visitors,
    (agosto.campana_agosto_visitors / (agosto.control_agosto_avg_visitors * 7)) - 1 AS crecimiento_total_agosto,
    (agosto.control_agosto_avg_visitors * 7) AS total_visitors_control_agosto
  FROM proxy_calculation AS proxy, agosto_summary AS agosto
)
SELECT
  'Crecimiento total en agosto' AS Metrica,
  crecimiento_total_agosto AS Valor_Absoluto,
  FORMAT('%.2f%%', crecimiento_total_agosto * 100) AS Valor_Porcentual
FROM final_atribucion
UNION ALL
SELECT
  'Crecimiento atribuido a Banners y Pushes' AS Metrica,
  (proxy_atribucion_visitors * total_visitors_control_agosto) AS Valor_Absoluto,
  FORMAT('%.2f%%', proxy_atribucion_visitors * 100) AS Valor_Porcentual
FROM final_atribucion
UNION ALL
SELECT
  'Crecimiento atribuido a campana de TV' AS Metrica,
  (campana_agosto_visitors - (proxy_atribucion_visitors * total_visitors_control_agosto) - total_visitors_control_agosto) AS Valor_Absoluto,
  FORMAT('%.2f%%', (campana_agosto_visitors - (proxy_atribucion_visitors * total_visitors_control_agosto) - total_visitors_control_agosto) / total_visitors_control_agosto * 100) AS Valor_Porcentual
FROM final_atribucion