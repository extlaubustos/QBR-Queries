-- description: Análisis de influencia Cross-Device: Identifica usuarios que migraron de Mobile a Smart TV, cuantificando cuántos usuarios nuevos en TV tuvieron una sesión activa en Mobile en las 24 horas previas a su primer play en pantalla grande. 
-- domain: behaviour 
-- product: mplay 
-- use_case: cross-platform journey / mobile-to-tv conversion analysis 
-- grain: month_id 
-- time_grain: monthly 
-- date_column: FIRST_TV_PLAY_DS 
-- date_filter: date_from (2025-04-01) to current_date - 1 
-- threshold_rule: 
-- - Valid TV Play: playback_time >= 20s y LOGGED_USER is TRUE 
-- - Mobile Influence: Sesión no rebotada (is_bounced is FALSE) en Mobile antes del timestamp del primer play en TV. 
-- metrics: 
-- - USERS_MOBILE_BEFORE_FIRST_TV_PLAY: Usuarios influenciados por Mobile antes de su debut en TV. 
-- - GRAND_TOTAL_FIRST_TV_VIEWERS: Total de usuarios que iniciaron en Smart TV por primera vez. 
-- - MOBILE_TO_FIRST_TV_VIEWER_SHARE: Tasa de conversión o share de influencia de Mobile sobre TV. 
-- tables_read: 
-- - meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS 
-- - meli-bi-data.WHOWNER.BT_MKT_MPLAY_SESSION 
-- joins: 
-- - FIRST_TV_PLAY (F) INNER JOIN BT_MKT_MPLAY_SESSION (S_MOBILE): Para validar la existencia de una sesión previa en mobile dentro de la ventana temporal definida. 
-- - INFLUENCED_USERS INNER JOIN TOTAL_FIRST_TV_VIEWERS: Para el cálculo de proporciones (shares) por sitio y mes. 
-- owner: data_team
DECLARE date_from DATE;
DECLARE date_to DATE;
SET date_from = '2025-04-01';
SET date_to = CURRENT_DATE - 1;
WITH FIRST_TV_PLAY_HISTORY AS (
  SELECT
    SIT_SITE_ID,
    USER_ID,
    MIN(DS) AS FIRST_TV_PLAY_DS,
    MIN(START_PLAY_TIMESTAMP) AS FIRST_TV_PLAY_TIMESTAMP
  FROM meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS
  WHERE
    UPPER(DEVICE_PLATFORM) LIKE '%TV%'
    AND PLAYBACK_TIME_MILLISECONDS/1000 >= 20
    AND LOGGED_USER IS TRUE
  GROUP BY ALL
),
FIRST_TV_PLAY_DAY_OF_ANALYSIS AS (
  SELECT
    SIT_SITE_ID,
    USER_ID,
    FIRST_TV_PLAY_DS AS DS,
    DATE_TRUNC(FIRST_TV_PLAY_DS, MONTH) AS MONTH_ID,
    FIRST_TV_PLAY_TIMESTAMP
  FROM FIRST_TV_PLAY_HISTORY
  WHERE
    FIRST_TV_PLAY_DS >= date_from
    AND FIRST_TV_PLAY_DS <= date_to
),
USERS_MOBILE_BEFORE_FIRST_PLAY AS (
  SELECT DISTINCT
    F.SIT_SITE_ID,
    F.USER_ID,
    F.DS,
    F.MONTH_ID
  FROM FIRST_TV_PLAY_DAY_OF_ANALYSIS AS F
  INNER JOIN meli-bi-data.WHOWNER.BT_MKT_MPLAY_SESSION AS S_MOBILE
    ON F.USER_ID = S_MOBILE.USER_ID
    AND F.SIT_SITE_ID = S_MOBILE.SIT_SITE_ID
    AND S_MOBILE.DS >= DATE_SUB(F.DS, INTERVAL 1 DAY)
AND S_MOBILE.DS <= F.DS
  WHERE
    UPPER(S_MOBILE.DEVICE_PLATFORM) LIKE '%MOBILE%'
    AND S_MOBILE.LOGGED_USER IS TRUE
    AND IS_BOUNCED IS FALSE
    AND S_MOBILE.END_TIME_USERTIMESTAMP < F.FIRST_TV_PLAY_TIMESTAMP
),
INFLUENCED_USERS_MONTH_SITE AS (
  SELECT
    SIT_SITE_ID,
    MONTH_ID,
    COUNT(DISTINCT USER_ID) AS INFLUENCED_FIRST_TV_VIEWERS
  FROM USERS_MOBILE_BEFORE_FIRST_PLAY
  GROUP BY ALL
),
TOTAL_FIRST_TV_VIEWERS_MONTH_SITE AS (
  SELECT
    SIT_SITE_ID,
    MONTH_ID,
    COUNT(DISTINCT USER_ID) AS TOTAL_FIRST_TV_VIEWERS
  FROM FIRST_TV_PLAY_DAY_OF_ANALYSIS
  GROUP BY ALL
)
SELECT
  INF.MONTH_ID,
  SUM(INF.INFLUENCED_FIRST_TV_VIEWERS) AS USERS_MOBILE_BEFORE_FIRST_TV_PLAY,
  SUM(TD.TOTAL_FIRST_TV_VIEWERS) AS GRAND_TOTAL_FIRST_TV_VIEWERS,
  ROUND(SAFE_DIVIDE(SUM(INF.INFLUENCED_FIRST_TV_VIEWERS), SUM(TD.TOTAL_FIRST_TV_VIEWERS)), 3) AS MOBILE_TO_FIRST_TV_VIEWER_SHARE
FROM INFLUENCED_USERS_MONTH_SITE AS INF
INNER JOIN TOTAL_FIRST_TV_VIEWERS_MONTH_SITE AS TD
  ON INF.SIT_SITE_ID = TD.SIT_SITE_ID
  AND INF.MONTH_ID = TD.MONTH_ID
GROUP BY 1
ORDER BY 1;