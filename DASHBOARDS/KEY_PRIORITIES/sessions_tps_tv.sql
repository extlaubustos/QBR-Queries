-- description: Métricas de sesiones y reproducciones de usuarios en plataformas TV por sitio y origen
-- domain: behaviour
-- product: mplay
-- use_case: reporting
-- grain: sit_site_id, device_platform, origin
-- time_grain: daily
-- date_column: DS
-- date_filter: entre date_from y date_to
-- threshold_rule: playback_time >= 20s, visitas válidas definidas por flag
-- metrics:
-- - Sessions: sesiones totales
-- - Sessions_valid_visit: sesiones con visita válida
-- - Sessions_valid_view: sesiones con reproducción >= 20s
-- tables_read:
-- - meli-bi-data.WHOWNER.BT_MKT_MPLAY_SESSION
-- - meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS
-- - meli-sbox.MPLAY.LK_MPLAY_SOURCE_TYPE_ORIGIN_SESSION
-- joins:
-- - SESSION_PLAY joins BT_MKT_MPLAY_PLAYS por sit_site_id, user_id y session_id
-- - LEFT JOIN LK_MPLAY_SOURCE_TYPE_ORIGIN_SESSION por FIRST_EVENT_SOURCE
-- owner: data_team

DECLARE SITES ARRAY<STRING>;
DECLARE date_from DATE;
DECLARE date_to DATE;
SET SITES = ['MLC', 'MLA', 'MLB', 'MLM', 'MCO', 'MPE', 'MLU', 'MEC'];
SET date_from = '2025-03-01';
SET date_to = current_date() - 1;

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
                      IF(IS_BOUNCED IS TRUE,FALSE,TRUE) AS FLAG_VALID_VISIT,
                      HAS_PLAY,
                      S.TOTAL_SESSION_MILLISECOND/1000 AS session_time_sec
              FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_SESSION` AS s
              WHERE s.ds >= date_from 
              AND s.ds < date_to
              and lower(s.device_platform) like '%/tv%'
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
      DATE_TRUNC(s.DS, MONTH) AS MONTH_ID,
      DATE_TRUNC(s.DS, WEEK(MONDAY)) AS WEEK_ID,
      S.DS,
      s.sit_site_id,
      CASE 
            WHEN LOWER(s.DEVICE_PLATFORM) LIKE '%/tv/android%' THEN 'ANDROID'
            WHEN LOWER(s.DEVICE_PLATFORM) LIKE '%/tv/tizen%' THEN 'SAMSUNG'
            WHEN LOWER(s.DEVICE_PLATFORM) LIKE '%/tv/web0s%' THEN 'LG'
            ELSE 'Other_TV_Platform'
        END AS DEVICE_PLATFORM,
      CONCAT(S.SIT_SITE_ID, '-', CASE 
            WHEN LOWER(s.DEVICE_PLATFORM) LIKE '%/tv/android%' THEN 'ANDROID'
            WHEN LOWER(s.DEVICE_PLATFORM) LIKE '%/tv/tizen%' THEN 'SAMSUNG'
            WHEN LOWER(s.DEVICE_PLATFORM) LIKE '%/tv/web0s%' THEN 'LG'
            ELSE 'Other_TV_Platform'
        END) AS SITE_PLATFORM,
      o.SOURCE_TYPE AS Origin,
      COUNT(DISTINCT s.melidata_session_id) Sessions,
      COUNT(DISTINCT CASE WHEN s.FLAG_VALID_VISIT IS TRUE THEN s.melidata_session_id ELSE NULL END) as Sessions_valid_visit, 
      COUNT(DISTINCT CASE WHEN s.TSV >= 20 THEN s.melidata_session_id ELSE NULL END) as Sessions_valid_view
FROM SESSION_PLAY s
LEFT JOIN `meli-sbox.MPLAY.LK_MPLAY_SOURCE_TYPE_ORIGIN_SESSION` o on coalesce(s.FIRST_EVENT_SOURCE,'NULL') = coalesce(o.SOURCE_TYPE,'NULL')
where device_platform in ('/tv/android','/tv/Tizen','/tv/Web0S')
GROUP BY ALL
;