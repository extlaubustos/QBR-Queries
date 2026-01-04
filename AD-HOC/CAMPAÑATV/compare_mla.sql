-- description: Análisis de impacto de campañas CTV sobre VISITORS, VIEWERS y DOWNLOADS en Smart TV
-- domain: growth_marketing
-- product: mplay
-- use_case: ctv_campaign_performance
-- grain: period_name
-- time_grain: custom_periods
-- date_column: DS
-- sites: MLA (parametrizable)
-- device_scope: Smart TV (DEVICE_PLATFORM LIKE '/tv%')

-- periods_defined:
-- - CONTROL PRE CTV: 2025-07-14 → 2025-08-15
-- - CTV1: 2025-08-18 → 2025-09-19
-- - CTV1 COMPARE CTV2: 2025-08-18 → 2025-08-27
-- - CTV1 COMPARE CTVOFF: 2025-08-23 → 2025-09-13
-- - CTVOFF COMPARE CTV1: 2025-09-20 → 2025-10-11
-- - CTV OFF: 2025-09-20 → 2025-10-19
-- - CTV OFF COMPARE CTV2: 2025-09-22 → 2025-10-01
-- - CTV2: 2025-10-20 → CURRENT_DATE

-- metrics:
-- - UNIQUE_VIEWERS: usuarios únicos con al menos un playback >= 20s
-- - UNIQUE_VISITORS: usuarios únicos con sesión válida en Smart TV
-- - TOTAL_DOWNLOADS: descargas netas de la app

-- business_rules:
-- - Viewer: PLAYBACK_TIME >= 20 segundos
-- - Visitor: sesión Smart TV con interacción válida (search, vcp, vcm, play o feed impressions)
-- - Downloads: NET_APP_INSTALLS agregados por día

-- tables_read:
-- - WHOWNER.BT_MKT_MPLAY_SESSION
-- - WHOWNER.BT_MKT_MPLAY_PLAYS
-- - WHOWNER.BT_MKT_MPLAY_INSTALLS

-- joins:
-- - SESSION ↔ PLAYS por USER_ID + SESSION_ID + SITE
-- - Agregaciones independientes por período (no join entre métricas)

-- output:
-- - period_start
-- - period_end
-- - period_name
-- - unique_viewers
-- - unique_visitors
-- - total_downloads

-- owner: growth_analytics

-- VISITORS
DECLARE SITES ARRAY<STRING>;
DECLARE date_from DATE;
DECLARE date_to DATE;

DECLARE control_start DATE;
DECLARE control_end DATE;
DECLARE ctv1_start DATE;
DECLARE ctv1_end DATE;
DECLARE ctv1_compare_ctv2_start DATE;
DECLARE ctv1_compare_ctv2_end DATE;
DECLARE ctv1_compare_ctvoff_start DATE;
DECLARE ctv1_compare_ctvoff_end DATE;
DECLARE ctvoff_compare_ctv1_start DATE;
DECLARE ctvoff_compare_ctv1_end DATE;
DECLARE ctv_off_start DATE;
DECLARE ctv_off_end DATE;
DECLARE ctvoff_compare_ctv2_start DATE;
DECLARE ctvoff_compare_ctv2_end DATE;
DECLARE ctv2_start DATE;
DECLARE ctv2_end DATE;


SET SITES = ['MLA'];
SET date_from = '2025-07-01';
SET date_to = CURRENT_DATE();

SET control_start = '2025-07-14';
SET control_end = '2025-08-15';

SET ctv1_start = '2025-08-18';
SET ctv1_end = '2025-09-19';

SET ctv1_compare_ctv2_start = '2025-08-18';
SET ctv1_compare_ctv2_end = '2025-08-27';

SET ctv1_compare_ctvoff_start = '2025-08-23';
SET ctv1_compare_ctvoff_end = '2025-09-13';

SET ctvoff_compare_ctv1_start = '2025-09-20';
SET ctvoff_compare_ctv1_end = '2025-10-11';

SET ctv_off_start = '2025-09-20';
SET ctv_off_end = '2025-10-19';

SET ctvoff_compare_ctv2_start = '2025-09-22';
SET ctvoff_compare_ctv2_end = '2025-10-01';

SET ctv2_start = '2025-10-20';
SET ctv2_end = CURRENT_DATE();


WITH SESSIONS AS (
  SELECT
    s.SIT_SITE_ID,
    DATE_TRUNC(s.ds, WEEK(MONDAY)) AS fecha_week,
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
    P.DS,
    P.USER_ID,
  FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` P
  WHERE LOWER(DEVICE_PLATFORM) LIKE '/tv%'
  AND DS BETWEEN '2025-04-01' AND CURRENT_DATE()
  AND PLAYBACK_TIME_MILLISECONDS/1000 >=20
),
CONTROL_PERIOD AS (
    SELECT
    control_start AS period_start,
    control_end AS period_end,
    'CONTROL PRE CTV' AS period_name,
    (
        -- 1. Conteo de Viewers Únicos (USER_ID from VIEWERS CTE)
        SELECT
            COUNT(DISTINCT T1.USER_ID)
        FROM VIEWERS AS T1
        WHERE
            T1.DS BETWEEN control_start AND control_end
            AND T1.SIT_SITE_ID IN UNNEST(SITES)
    ) AS unique_viewers,
    (
        -- 2. Conteo de Visitors Únicos (USER_ID from SESSION_PLAY CTE)
        SELECT
            COUNT(DISTINCT T2.USER_ID)
        FROM SESSION_PLAY AS T2
        WHERE
            T2.DS BETWEEN control_start AND control_end
            AND T2.SIT_SITE_ID IN UNNEST(SITES)
    ) AS unique_visitors,
    (
        -- 3. Conteo de Descargas (SUM(DOWNLOADS) from DOWNLOADS CTE)
        SELECT
            SUM(T3.DOWNLOADS)
        FROM DOWNLOADS AS T3
        WHERE
            T3.DS BETWEEN control_start AND control_end
            AND T3.SIT_SITE_ID IN UNNEST(SITES)
    ) AS total_downloads
),
CTV1 AS (
    SELECT
    ctv1_start AS period_start,
    ctv1_end AS period_end,
    'CTV1' AS period_name,
    (
        -- 1. Conteo de Viewers Únicos (USER_ID from VIEWERS CTE)
        SELECT
            COUNT(DISTINCT T1.USER_ID)
        FROM VIEWERS AS T1
        WHERE
            T1.DS BETWEEN ctv1_start AND ctv1_end
            AND T1.SIT_SITE_ID IN UNNEST(SITES)
    ) AS unique_viewers,
    (
        -- 2. Conteo de Visitors Únicos (USER_ID from SESSION_PLAY CTE)
        SELECT
            COUNT(DISTINCT T2.USER_ID)
        FROM SESSION_PLAY AS T2
        WHERE
            T2.DS BETWEEN ctv1_start AND ctv1_end
            AND T2.SIT_SITE_ID IN UNNEST(SITES)
    ) AS unique_visitors,
    (
        -- 3. Conteo de Descargas (SUM(DOWNLOADS) from DOWNLOADS CTE)
        SELECT
            SUM(T3.DOWNLOADS)
        FROM DOWNLOADS AS T3
        WHERE
            T3.DS BETWEEN ctv1_start AND ctv1_end
            AND T3.SIT_SITE_ID IN UNNEST(SITES)
    ) AS total_downloads
),
CTV1_COMPARE_CTV2 AS (
    SELECT
    ctv1_compare_ctv2_start AS period_start,
    ctv1_compare_ctv2_end AS period_end,
    'CTV1 COMPARE CTV2' AS period_name,
    (
        -- 1. Conteo de Viewers Únicos (USER_ID from VIEWERS CTE)
        SELECT
            COUNT(DISTINCT T1.USER_ID)
        FROM VIEWERS AS T1
        WHERE
            T1.DS BETWEEN ctv1_compare_ctv2_start AND ctv1_compare_ctv2_end
            AND T1.SIT_SITE_ID IN UNNEST(SITES)
    ) AS unique_viewers,
    (
        -- 2. Conteo de Visitors Únicos (USER_ID from SESSION_PLAY CTE)
        SELECT
            COUNT(DISTINCT T2.USER_ID)
        FROM SESSION_PLAY AS T2
        WHERE
            T2.DS BETWEEN ctv1_compare_ctv2_start AND ctv1_compare_ctv2_end
            AND T2.SIT_SITE_ID IN UNNEST(SITES)
    ) AS unique_visitors,
    (
        -- 3. Conteo de Descargas (SUM(DOWNLOADS) from DOWNLOADS CTE)
        SELECT
            SUM(T3.DOWNLOADS)
        FROM DOWNLOADS AS T3
        WHERE
            T3.DS BETWEEN ctv1_compare_ctv2_start AND ctv1_compare_ctv2_end
            AND T3.SIT_SITE_ID IN UNNEST(SITES)
    ) AS total_downloads
),
CTV1_COMPARE_CTVOFF AS (
    SELECT
    ctv1_compare_ctvoff_start AS period_start,
    ctv1_compare_ctvoff_end AS period_end,
    'CTV1 COMPARE CTVOFF' AS period_name,
    (
        -- 1. Conteo de Viewers Únicos (USER_ID from VIEWERS CTE)
        SELECT
            COUNT(DISTINCT T1.USER_ID)
        FROM VIEWERS AS T1
        WHERE
            T1.DS BETWEEN ctv1_compare_ctvoff_start AND ctv1_compare_ctvoff_end
            AND T1.SIT_SITE_ID IN UNNEST(SITES)
    ) AS unique_viewers,
    (
        -- 2. Conteo de Visitors Únicos (USER_ID from SESSION_PLAY CTE)
        SELECT
            COUNT(DISTINCT T2.USER_ID)
        FROM SESSION_PLAY AS T2
        WHERE
            T2.DS BETWEEN ctv1_compare_ctvoff_start AND ctv1_compare_ctvoff_end
            AND T2.SIT_SITE_ID IN UNNEST(SITES)
    ) AS unique_visitors,
    (
        -- 3. Conteo de Descargas (SUM(DOWNLOADS) from DOWNLOADS CTE)
        SELECT
            SUM(T3.DOWNLOADS)
        FROM DOWNLOADS AS T3
        WHERE
            T3.DS BETWEEN ctv1_compare_ctvoff_start AND ctv1_compare_ctvoff_end
            AND T3.SIT_SITE_ID IN UNNEST(SITES)
    ) AS total_downloads
),
CTVOFF_COMPARE_CTV1 AS (
    SELECT
    ctvoff_compare_ctv1_start AS period_start,
    ctvoff_compare_ctv1_end AS period_end,
    'CTVOFF COMPARE CTV1' AS period_name,
    (
        -- 1. Conteo de Viewers Únicos (USER_ID from VIEWERS CTE)
        SELECT
            COUNT(DISTINCT T1.USER_ID)
        FROM VIEWERS AS T1
        WHERE
            T1.DS BETWEEN ctvoff_compare_ctv1_start AND ctvoff_compare_ctv1_end
            AND T1.SIT_SITE_ID IN UNNEST(SITES)
    ) AS unique_viewers,
    (
        -- 2. Conteo de Visitors Únicos (USER_ID from SESSION_PLAY CTE)
        SELECT
            COUNT(DISTINCT T2.USER_ID)
        FROM SESSION_PLAY AS T2
        WHERE
            T2.DS BETWEEN ctvoff_compare_ctv1_start AND ctvoff_compare_ctv1_end
            AND T2.SIT_SITE_ID IN UNNEST(SITES)
    ) AS unique_visitors,
    (
        -- 3. Conteo de Descargas (SUM(DOWNLOADS) from DOWNLOADS CTE)
        SELECT
            SUM(T3.DOWNLOADS)
        FROM DOWNLOADS AS T3
        WHERE
            T3.DS BETWEEN ctvoff_compare_ctv1_start AND ctvoff_compare_ctv1_end
            AND T3.SIT_SITE_ID IN UNNEST(SITES)
    ) AS total_downloads
),
CTV2 AS (
    SELECT
    ctv2_start AS period_start,
    ctv2_end AS period_end,
    'CTV2' AS period_name,
    (
        -- 1. Conteo de Viewers Únicos (USER_ID from VIEWERS CTE)
        SELECT
            COUNT(DISTINCT T1.USER_ID)
        FROM VIEWERS AS T1
        WHERE
            T1.DS BETWEEN ctv2_start AND ctv2_end
            AND T1.SIT_SITE_ID IN UNNEST(SITES)
    ) AS unique_viewers,
    (
        -- 2. Conteo de Visitors Únicos (USER_ID from SESSION_PLAY CTE)
        SELECT
            COUNT(DISTINCT T2.USER_ID)
        FROM SESSION_PLAY AS T2
        WHERE
            T2.DS BETWEEN ctv2_start AND ctv2_end
            AND T2.SIT_SITE_ID IN UNNEST(SITES)
    ) AS unique_visitors,
    (
        -- 3. Conteo de Descargas (SUM(DOWNLOADS) from DOWNLOADS CTE)
        SELECT
            SUM(T3.DOWNLOADS)
        FROM DOWNLOADS AS T3
        WHERE
            T3.DS BETWEEN ctv2_start AND ctv2_end
            AND T3.SIT_SITE_ID IN UNNEST(SITES)
    ) AS total_downloads
),
CTV_OFF AS (
    SELECT
    ctv_off_start AS period_start,
    ctv_off_end AS period_end,
    'CTV OFF' AS period_name,
    (
        -- 1. Conteo de Viewers Únicos (USER_ID from VIEWERS CTE)
        SELECT
            COUNT(DISTINCT T1.USER_ID)
        FROM VIEWERS AS T1
        WHERE
            T1.DS BETWEEN ctv_off_start AND ctv_off_end
            AND T1.SIT_SITE_ID IN UNNEST(SITES)
    ) AS unique_viewers,
    (
        -- 2. Conteo de Visitors Únicos (USER_ID from SESSION_PLAY CTE)
        SELECT
            COUNT(DISTINCT T2.USER_ID)
        FROM SESSION_PLAY AS T2
        WHERE
            T2.DS BETWEEN ctv_off_start AND ctv_off_end
            AND T2.SIT_SITE_ID IN UNNEST(SITES)
    ) AS unique_visitors,
    (
        -- 3. Conteo de Descargas (SUM(DOWNLOADS) from DOWNLOADS CTE)
        SELECT
            SUM(T3.DOWNLOADS)
        FROM DOWNLOADS AS T3
        WHERE
            T3.DS BETWEEN ctv_off_start AND ctv_off_end
            AND T3.SIT_SITE_ID IN UNNEST(SITES)
    ) AS total_downloads
),
CTV_OFF_CONTROL_PERIOD AS (
    SELECT
    ctvoff_compare_ctv2_start AS period_start,
    ctvoff_compare_ctv2_end AS period_end,
    'CTV OFF COMPARE CTV2' AS period_name,
    (
        -- 1. Conteo de Viewers Únicos (USER_ID from VIEWERS CTE)
        SELECT
            COUNT(DISTINCT T1.USER_ID)
        FROM VIEWERS AS T1
        WHERE
            T1.DS BETWEEN ctvoff_compare_ctv2_start AND ctvoff_compare_ctv2_end
            AND T1.SIT_SITE_ID IN UNNEST(SITES)
    ) AS unique_viewers,
    (
        -- 2. Conteo de Visitors Únicos (USER_ID from SESSION_PLAY CTE)
        SELECT
            COUNT(DISTINCT T2.USER_ID)
        FROM SESSION_PLAY AS T2
        WHERE
            T2.DS BETWEEN ctvoff_compare_ctv2_start AND ctvoff_compare_ctv2_end
            AND T2.SIT_SITE_ID IN UNNEST(SITES)
    ) AS unique_visitors,
    (
        -- 3. Conteo de Descargas (SUM(DOWNLOADS) from DOWNLOADS CTE)
        SELECT
            SUM(T3.DOWNLOADS)
        FROM DOWNLOADS AS T3
        WHERE
            T3.DS BETWEEN ctvoff_compare_ctv2_start AND ctvoff_compare_ctv2_end
            AND T3.SIT_SITE_ID IN UNNEST(SITES)
    ) AS total_downloads
)
SELECT * FROM CTV1
UNION ALL
SELECT * FROM CTV1_COMPARE_CTV2
UNION ALL
SELECT * FROM CTV1_COMPARE_CTVOFF
UNION ALL
SELECT * FROM CTV2
UNION ALL
SELECT * FROM CTV_OFF
UNION ALL
SELECT * FROM CONTROL_PERIOD
UNION ALL
SELECT * FROM CTV_OFF_CONTROL_PERIOD
UNION ALL
SELECT * FROM CTVOFF_COMPARE_CTV1
ORDER BY period_start;