-- description: Análisis de influencia Cross-Device (Mobile-to-TV) enfocado en reproducciones: Identifica a los usuarios logueados que consumieron contenido en Smart TV habiendo tenido una sesión activa en Mobile en las 24 horas previas, midiendo la recurrencia de este comportamiento de "salto" entre dispositivos. 
-- domain: behaviour 
-- product: mplay 
-- use_case: cross-platform conversion / mobile-to-tv play influence 
-- grain: month_id 
-- time_grain: monthly 
-- date_column: DS 
-- date_filter: dinámico (desde 2025-04-01 hasta ayer) 
-- threshold_rule: 
-- - Valid TV Play: reproducción >= 20s en Smart TV por usuario logueado. 
-- - Mobile Influence: Sesión en Mobile no rebotada finalizada antes del inicio de la reproducción en TV (ventana de 24 horas). 
-- metrics: 
-- - USERS_MOBILE_BEFORE_TV_PLAY: Usuarios únicos mensuales que pasaron de Mobile a TV antes de un play. 
-- - GRAND_TOTAL_TV_VIEWERS: Total de usuarios únicos logueados que reprodujeron en TV. 
-- - MOBILE_TO_TV_VIEWER_SHARE: Porcentaje de la audiencia de TV influenciada por actividad previa en Mobile. 
-- tables_read: 
-- - meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS 
-- - meli-bi-data.WHOWNER.BT_MKT_MPLAY_SESSION 
-- joins: 
-- - ALL_TV_PLAYS_LOGGED (P) INNER JOIN BT_MKT_MPLAY_SESSION (S_MOBILE): Cruce por USER_ID y SIT_SITE_ID con validación de secuencia temporal (Timestamp Mobile < Timestamp TV Play). 
-- owner: data_team
DECLARE date_from DATE;
DECLARE date_to DATE;
SET date_from = '2025-04-01';
SET date_to = CURRENT_DATE - 1;

WITH ALL_TV_PLAYS_LOGGED AS (
  SELECT
    P.SIT_SITE_ID,
    P.USER_ID,
    P.DS,
    DATE_TRUNC(P.DS, MONTH) AS MONTH_ID, 
    P.START_PLAY_TIMESTAMP
  FROM meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS AS P
  WHERE
    P.DS >= date_from
    AND P.DS <= date_to
    AND UPPER(P.DEVICE_PLATFORM) LIKE '%TV%'
    AND P.PLAYBACK_TIME_MILLISECONDS/1000 >= 20
    AND SAFE_CAST(P.USER_ID AS INT64) IS NOT NULL
    --AND P.LOGGED_USER IS TRUE
),
USERS_MOBILE_BEFORE_TV_PLAY AS (
  SELECT DISTINCT
    TV.SIT_SITE_ID,
    TV.USER_ID,
    TV.DS,
    TV.MONTH_ID
  FROM ALL_TV_PLAYS_LOGGED AS TV
  INNER JOIN meli-bi-data.WHOWNER.BT_MKT_MPLAY_SESSION AS S_MOBILE
    ON TV.USER_ID = S_MOBILE.USER_ID
    AND TV.SIT_SITE_ID = S_MOBILE.SIT_SITE_ID
    AND S_MOBILE.DS >= DATE_SUB(TV.DS, INTERVAL 1 DAY)
    AND S_MOBILE.DS <= TV.DS
  WHERE
    UPPER(S_MOBILE.DEVICE_PLATFORM) LIKE '%MOBILE%'
    AND SAFE_CAST(S_MOBILE.USER_ID AS INT64) IS NOT NULL
    --AND S_MOBILE.LOGGED_USER IS TRUE
    AND S_MOBILE.IS_BOUNCED IS FALSE
    AND S_MOBILE.END_TIME_USERTIMESTAMP < TV.START_PLAY_TIMESTAMP
),
INFLUENCED_USERS_MONTH_SITE AS (
  SELECT
    SIT_SITE_ID,
    MONTH_ID,
    COUNT(DISTINCT USER_ID) AS INFLUENCED_TV_VIEWERS
  FROM USERS_MOBILE_BEFORE_TV_PLAY
  GROUP BY ALL
),
TOTAL_TV_VIEWERS_MONTH_SITE AS (
  SELECT
    SIT_SITE_ID,
    MONTH_ID,
    COUNT(DISTINCT USER_ID) AS TOTAL_DAILY_TV_VIEWERS
  FROM ALL_TV_PLAYS_LOGGED
  GROUP BY ALL
)
SELECT
  INF.MONTH_ID,
  SUM(INF.INFLUENCED_TV_VIEWERS) AS USERS_MOBILE_BEFORE_TV_PLAY,
  SUM(TD.TOTAL_DAILY_TV_VIEWERS) AS GRAND_TOTAL_TV_VIEWERS,
  ROUND(SAFE_DIVIDE(SUM(INF.INFLUENCED_TV_VIEWERS), SUM(TD.TOTAL_DAILY_TV_VIEWERS)), 3) AS MOBILE_TO_TV_VIEWER_SHARE
FROM INFLUENCED_USERS_MONTH_SITE AS INF
INNER JOIN TOTAL_TV_VIEWERS_MONTH_SITE AS TD
  ON INF.SIT_SITE_ID = TD.SIT_SITE_ID
  AND INF.MONTH_ID = TD.MONTH_ID
GROUP BY 1
ORDER BY 1;