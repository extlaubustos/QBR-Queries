-- description: Métricas de visitas, visitas válidas y viewers a nivel sesión por sitio y semana
-- domain: behaviour
-- product: mplay
-- use_case: reporting
-- grain: site, week
-- time_grain: weekly
-- date_column: s.DS
-- date_filter: between
-- threshold_rule: playback_time >= 20s
-- metrics:
--   - VISITORS: usuarios únicos con al menos una sesión registrada
--   - VALID_VISITORS: usuarios únicos con sesiones consideradas válidas según criterios de interacción
--   - VIEWERS: usuarios únicos con al menos una sesión con consumo de video >= 20s
-- tables_read:
--   - WHOWNER.BT_MKT_MPLAY_SESSION
--   - WHOWNER.BT_MKT_MPLAY_PLAYS
-- joins:
--   - SESSION.SIT_SITE_ID = PLAYS.SIT_SITE_ID
--   - SESSION.USER_ID = PLAYS.USER_ID
--   - SESSION.SESSION_ID = PLAYS.SESSION_ID
-- owner: data_team

DECLARE date_from DATE;
DECLARE date_to DATE;
SET SITES = ['MLC', 'MLA', 'MLB', 'MLM', 'MCO', 'MPE', 'MLU', 'MEC'];
SET date_from = '2025-01-01';
SET date_to = current_date();

WITH SESSIONS AS
              ( SELECT
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
                      IF(((S.HAS_SEARCH IS TRUE OR S.HAS_VCP IS TRUE OR S.HAS_VCM IS TRUE OR HAS_PLAY IS TRUE) OR TOTAL_FEED_IMPRESSIONS > 1),TRUE,FALSE) AS FLAG_VALID_VISIT,
                      HAS_PLAY,
                      S.TOTAL_SESSION_MILLISECOND/1000 AS session_time_sec
              FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_SESSION` AS s
              WHERE s.ds >= date_from 
              AND s.ds < date_to
              AND s.SIT_SITE_ID IN UNNEST(SITES)
              GROUP BY ALL
)
, SESSION_PLAY AS   ( 
              SELECT DISTINCT
                    s.SIT_SITE_ID,
                    s.MONTH_ID,
                    s.fecha_week,
                    S.DS,
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
              LEFT JOIN `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` AS P ON S.SIT_SITE_ID = P.SIT_SITE_ID
                                                                        AND s.USER_ID = P.USER_ID
                                                                        AND S.MELIDATA_SESSION_ID = P.SESSION_ID
                                                                        AND P.PLAYBACK_TIME_MILLISECONDS/1000 >= 20                                               
              GROUP BY ALL
)
SELECT 
      s.sit_site_id,
      DATE_TRUNC(s.DS, WEEK(MONDAY)) as WEEK_ID,
 
      COUNT(DISTINCT s.USER_ID) as Visitors,
      COUNT(DISTINCT CASE WHEN s.FLAG_VALID_VISIT IS TRUE THEN s.USER_ID ELSE NULL END) as Valid_Visitors,
      COUNT(DISTINCT CASE WHEN s.TSV >= 20 THEN s.USER_ID ELSE NULL END) as Viewers
FROM SESSION_PLAY s
GROUP BY ALL
ORDER BY WEEK_ID ASC
;