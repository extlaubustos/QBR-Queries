DECLARE SITES ARRAY<STRING>;
DECLARE date_from DATE;
DECLARE date_to DATE;
SET SITES = ['MLC', 'MLA', 'MLB', 'MLM', 'MCO', 'MPE', 'MLU', 'MEC'];
SET date_from = '2025-06-01';
SET date_to = current_date();

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
        IF(((S.HAS_SEARCH IS TRUE OR S.HAS_VCP IS TRUE OR S.HAS_VCM IS TRUE OR HAS_PLAY IS TRUE) OR TOTAL_FEED_IMPRESSIONS > 1),TRUE,FALSE) AS FLAG_VALID_VISIT,
        HAS_PLAY,
        S.TOTAL_SESSION_MILLISECOND/1000 AS session_time_sec
    FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_SESSION` AS s
    WHERE s.ds >= date_from 
      AND s.ds < date_to
      AND s.SIT_SITE_ID IN UNNEST(SITES)
    GROUP BY ALL
),
SESSION_PLAY AS ( 
    SELECT 
        s.SIT_SITE_ID,
        s.MONTH_ID,
        s.fecha_week,
        s.DS,
        s.FIRST_EVENT_SOURCE,
        s.DEVICE_PLATFORM,
        s.USER_ID,
        s.MELIDATA_SESSION_ID,
        s.FLAG_VALID_VISIT,
        SUM(P.PLAYBACK_TIME_MILLISECONDS/1000) AS TSV,
        SUM(P.PLAYBACK_TIME_MILLISECONDS/60000) AS TVM 
    FROM SESSIONS AS S 
    LEFT JOIN `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` AS P ON S.SIT_SITE_ID = P.SIT_SITE_ID
                                                              AND s.USER_ID = P.USER_ID
                                                              AND S.MELIDATA_SESSION_ID = P.SESSION_ID
                                                              AND P.PLAYBACK_TIME_MILLISECONDS/1000 >= 20                                               
    GROUP BY ALL
),
BASE_AGREGADA AS (
    SELECT 
          s.sit_site_id,
          s.MONTH_ID,
          s.fecha_week AS WEEK_ID,
          o.Clasificacion_2 AS Clasificacion,
          CASE WHEN S.DEVICE_PLATFORM IN ('/tv/android', '/tv/Tizen', '/tv/Web0S') THEN S.DEVICE_PLATFORM
               ELSE COALESCE(o.origin,'Otros')
               END AS Origin,
          s.melidata_session_id,
          s.FLAG_VALID_VISIT,
          s.TSV,
          s.TVM,
          s.USER_ID
    FROM SESSION_PLAY s
    LEFT JOIN `meli-sbox.MPLAY.CLASIFICATION_ORIGINS` o on coalesce(s.FIRST_EVENT_SOURCE,'NULL') = coalesce(o.origin,'NULL')
    WHERE s.DEVICE_PLATFORM NOT LIKE '%/tv%'
    WHERE Clasificacion_2 IN ('Push + Ads + E&G', 'Paid', 'SEO')
)

--- Agregación Final unificando Periodos
SELECT 
    'MONTHLY' AS TIMEFRAME_TYPE,
    MONTH_ID AS DATE_ID,
    sit_site_id, Clasificacion, Origin,
    COUNT(DISTINCT melidata_session_id) Sessions,
    COUNT(DISTINCT CASE WHEN FLAG_VALID_VISIT IS TRUE THEN melidata_session_id END) as Sessions_valid_visit,
    COUNT(DISTINCT CASE WHEN TSV >= 20 THEN melidata_session_id END) as Sessions_valid_view,
    SUM(TVM) as TVM,
    COUNT(DISTINCT USER_ID) Visitors,
    COUNT(DISTINCT CASE WHEN FLAG_VALID_VISIT IS TRUE THEN USER_ID END) as Valid_Visitors,
    COUNT(DISTINCT CASE WHEN TSV >= 20 THEN USER_ID END) as Viewers
FROM BASE_AGREGADA
GROUP BY 1, 2, 3, 4, 5

UNION ALL

SELECT 
    'WEEKLY' AS TIMEFRAME_TYPE,
    WEEK_ID AS DATE_ID,
    sit_site_id, Clasificacion, Origin,
    COUNT(DISTINCT melidata_session_id) Sessions,
    COUNT(DISTINCT CASE WHEN FLAG_VALID_VISIT IS TRUE THEN melidata_session_id END) as Sessions_valid_visit,
    COUNT(DISTINCT CASE WHEN TSV >= 20 THEN melidata_session_id END) as Sessions_valid_view,
    SUM(TVM) as TVM,
    COUNT(DISTINCT USER_ID) Visitors,
    COUNT(DISTINCT CASE WHEN FLAG_VALID_VISIT IS TRUE THEN USER_ID END) as Valid_Visitors,
    COUNT(DISTINCT CASE WHEN TSV >= 20 THEN USER_ID END) as Viewers
FROM BASE_AGREGADA
GROUP BY 1, 2, 3, 4, 5
ORDER BY TIMEFRAME_TYPE, DATE_ID ASC;