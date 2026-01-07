-- description: Sesiones de MPlay en dispositivos TV por sitio, plataforma y origen, con métricas de visitas y vistas válidas
-- domain: behaviour
-- product: mplay
-- use_case: reporting
-- grain: site, device_platform, origin, day
-- time_grain: day / week / month
-- date_column: s.DS
-- date_filter: between (date_from, date_to)
-- threshold_rule:
--   - playback_time >= 20s para considerar vista válida
--   - visita válida cuando IS_BOUNCED = FALSE
-- metrics:
--   - SESSIONS: sesiones distintas
--   - SESSIONS_VALID_VISIT: sesiones no rebotadas
--   - SESSIONS_VALID_VIEW: sesiones con consumo >= 20s
-- tables_read:
--   - WHOWNER.BT_MKT_MPLAY_SESSION
--   - WHOWNER.BT_MKT_MPLAY_PLAYS
--   - MPLAY.LK_MPLAY_SOURCE_TYPE_ORIGIN_SESSION
-- joins:
--   - SESSION.SIT_SITE_ID = PLAYS.SIT_SITE_ID
--   - SESSION.USER_ID = PLAYS.USER_ID
--   - SESSION.SESSION_ID = PLAYS.SESSION_ID
--   - SESSION.FIRST_EVENT_SOURCE = ORIGIN.SOURCE_TYPE
-- owner: data_team

DECLARE SITES ARRAY<STRING>;
DECLARE date_from DATE;
DECLARE date_to DATE;
SET SITES = ['MLC', 'MLA', 'MLB', 'MLM', 'MCO', 'MPE', 'MLU', 'MEC'];
SET date_from = '2025-03-01';
SET date_to = current_date() - 1;

WITH SESSIONS AS ( 
  SELECT
    s.SIT_SITE_ID,
    DATE_TRUNC(s.ds, MONTH) as MONTH_ID,
    DATE_TRUNC(s.ds, WEEK(MONDAY)) as fecha_week,
    s.ds,
    ORIGIN_PATH AS FIRST_EVENT_SOURCE,
    FIRST_TRACK AS FIRST_EVENT_PATH,
    FIRST_PLAY_DATETIME AS PLAY_TIMESTAMP,
    s.USER_ID,
    s.SESSION_ID AS MELIDATA_SESSION_ID,
    s.DEVICE_PLATFORM,
    IF(IS_BOUNCED IS TRUE, FALSE, TRUE) AS FLAG_VALID_VISIT,
    HAS_PLAY,
    S.TOTAL_SESSION_MILLISECOND/1000 AS session_time_sec
  FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_SESSION` AS s
  WHERE s.ds >= date_from 
    AND s.ds < date_to
    AND LOWER(s.device_platform) LIKE '%/tv%'
    AND s.SIT_SITE_ID IN UNNEST(SITES)
  GROUP BY ALL
), 

SESSION_PLAY AS ( 
  SELECT DISTINCT
    s.SIT_SITE_ID,
    s.MONTH_ID,
    s.fecha_week,
    s.DS,
    s.FIRST_EVENT_SOURCE,
    s.FIRST_EVENT_PATH,
    s.PLAY_TIMESTAMP,
    s.USER_ID,
    s.MELIDATA_SESSION_ID,
    s.FLAG_VALID_VISIT,
    s.HAS_PLAY,
    s.DEVICE_PLATFORM,
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
)

SELECT 
  DATE_TRUNC(s.DS, MONTH) AS MONTH_ID,
  DATE_TRUNC(s.DS, WEEK(MONDAY)) AS WEEK_ID,
  S.DS,
  s.sit_site_id,
  CASE 
    WHEN LOWER(s.DEVICE_PLATFORM) LIKE '%/tv/android%' THEN 'ANDROID'
    WHEN LOWER(s.DEVICE_PLATFORM) LIKE '%/tv