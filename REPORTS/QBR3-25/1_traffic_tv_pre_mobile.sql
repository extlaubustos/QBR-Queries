-- description: Análisis de navegación Cross-Device (Mobile-to-TV) enfocado en sesiones: Identifica visitantes recurrentes o nuevos en Smart TV que fueron precedidos por una sesión en Mobile el mismo día, midiendo la tasa de influencia de la App Mobile en el tráfico de TV. 
-- domain: behaviour 
-- product: mplay 
-- use_case: cross-platform session journey / mobile-to-tv traffic influence 
-- grain: month_id 
-- time_grain: monthly 
-- date_column: DS 
-- date_filter: dinámico (date_from '2025-04-01' hasta date_to 'current_date - 1') 
-- threshold_rule: 
-- - Valid TV Session: Sesión no rebotada (is_bounced is FALSE) en plataforma TV. 
-- - Mobile Influence: Sesión previa en Mobile (end_time < start_time de TV) el mismo día (same DS). 
-- metrics: 
-- - TOTAL_TV_VISITORS_PRECEDED_BY_MOBILE: Usuarios únicos que visitaron TV tras usar Mobile el mismo día. 
-- - GRAND_TOTAL_TV_VISITORS: Total de visitantes únicos en dispositivos TV. 
-- - MOBILE_TO_TV_SESSION_VISITORS: Share de visitantes de TV influenciados por una sesión previa en Mobile. 
-- tables_read: 
-- - meli-bi-data.WHOWNER.BT_MKT_MPLAY_SESSION 
-- joins: 
-- - ALL_TV_SESSIONS (TV) INNER JOIN BT_MKT_MPLAY_SESSION (S_MOBILE): Cruce por USER_ID y DS para detectar la secuencia de sesiones en diferentes dispositivos dentro de la misma fecha. 
-- owner: data_team
DECLARE date_from DATE;
DECLARE date_to DATE;
SET date_from = '2025-04-01';
SET date_to = CURRENT_DATE - 1;

WITH ALL_TV_SESSIONS AS (
  SELECT
    S.SIT_SITE_ID,
    S.USER_ID,
    S.DS,
    DATE_TRUNC(S.DS, MONTH) AS MONTH_ID, 
    S.SESSION_ID AS TV_SESSION_ID,
    S.START_TIME_USERTIMESTAMP AS TV_SESSION_START_TIMESTAMP
  FROM meli-bi-data.WHOWNER.BT_MKT_MPLAY_SESSION AS s
  WHERE S.DS >= date_from
    AND S.DS <= date_to
    AND UPPER(S.DEVICE_PLATFORM) LIKE '%TV%'
    --AND SAFE_CAST(S.USER_ID AS INT64) IS NOT NULL
    -- AND LOGGED_USER IS TRUE
    AND IS_BOUNCED IS FALSE
),
TV_SESSIONS_PRECEDED_BY_MOBILE AS (
  SELECT DISTINCT
    TV.SIT_SITE_ID,
    TV.USER_ID,
    TV.MONTH_ID,
    TV.DS,
    TV.TV_SESSION_ID
  FROM ALL_TV_SESSIONS AS TV
  INNER JOIN meli-bi-data.WHOWNER.BT_MKT_MPLAY_SESSION AS S_MOBILE
    ON TV.USER_ID = S_MOBILE.USER_ID
    AND TV.SIT_SITE_ID = S_MOBILE.SIT_SITE_ID
    AND TV.DS = S_MOBILE.DS
  WHERE
    UPPER(S_MOBILE.DEVICE_PLATFORM) LIKE '%MOBILE%'
    AND S_MOBILE.END_TIME_USERTIMESTAMP < TV.TV_SESSION_START_TIMESTAMP
    -- AND LOGGED_USER IS TRUE
    --AND SAFE_CAST(S_MOBILE.USER_ID AS INT64) IS NOT NULL
    AND IS_BOUNCED IS FALSE
),
TOTAL_TV_VISITORS_MONTH_SITE AS (
  SELECT
    SIT_SITE_ID,
    MONTH_ID,
    COUNT(DISTINCT USER_ID) AS TOTAL_TV_VISITORS
  FROM ALL_TV_SESSIONS
  GROUP BY ALL
),
INFLUENCED_VISITORS_MONTH_SITE AS (
  SELECT
    SIT_SITE_ID,
    MONTH_ID,
    COUNT(DISTINCT USER_ID) AS INFLUENCED_TV_VISITORS
  FROM TV_SESSIONS_PRECEDED_BY_MOBILE
  GROUP BY ALL
)

SELECT
  INF.MONTH_ID,
  SUM(INF.INFLUENCED_TV_VISITORS) AS TOTAL_TV_VISITORS_PRECEDED_BY_MOBILE,
  SUM(TD.TOTAL_TV_VISITORS) AS GRAND_TOTAL_TV_VISITORS,
  ROUND(SAFE_DIVIDE(SUM(INF.INFLUENCED_TV_VISITORS), SUM(TD.TOTAL_TV_VISITORS)), 3) AS MOBILE_TO_TV_SESSION_VISITORS
FROM INFLUENCED_VISITORS_MONTH_SITE AS INF
INNER JOIN TOTAL_TV_VISITORS_MONTH_SITE AS TD
  ON INF.SIT_SITE_ID = TD.SIT_SITE_ID
  AND INF.MONTH_ID = TD.MONTH_ID
GROUP BY 1
ORDER BY 1;
