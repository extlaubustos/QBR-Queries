DECLARE date_a_start DATE DEFAULT DATE_ADD(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL -7 DAY);
DECLARE date_b_start DATE DEFAULT DATE_ADD(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL -14 DAY);
WITH NEW_RET_RECO AS
(
    SELECT
        SIT_SITE_ID,
        USER_ID,
        DATE_TRUNC(DS, MONTH) AS TIME_FRAME_ID,
        (CASE WHEN (LAG(DS,1)OVER(PARTITION BY SIT_SITE_ID,USER_ID ORDER BY START_PLAY_TIMESTAMP ASC)) IS NULL THEN 'NEW'
              WHEN DATE_DIFF(DS, (LAG(DS,1)OVER(PARTITION BY SIT_SITE_ID,USER_ID ORDER BY START_PLAY_TIMESTAMP ASC)), DAY) <= 30 THEN 'RETAINED'
              WHEN DATE_DIFF(DS, (LAG(DS,1)OVER(PARTITION BY SIT_SITE_ID,USER_ID ORDER BY START_PLAY_TIMESTAMP ASC)), DAY) > 30 THEN 'RECOVERED'
              ELSE NULL END) AS FLAG_N_R
    FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS`
    WHERE PLAYBACK_TIME_MILLISECONDS/1000 >= 20
        AND DS <= CURRENT_DATE-1
),
ATTR_TIME_FRAME_ELEGIDO AS (
    SELECT
        SIT_SITE_ID,
        USER_ID,
        TIME_FRAME_ID,
        FLAG_N_R
    FROM NEW_RET_RECO
    QUALIFY ROW_NUMBER() OVER(PARTITION BY SIT_SITE_ID,USER_ID,TIME_FRAME_ID
                                ORDER BY TIME_FRAME_ID ASC) = 1
),
SESSIONS AS (
  SELECT
    s.SIT_SITE_ID,
    DATE_TRUNC(s.ds, MONTH) AS MONTH_ID,
    DATE_TRUNC(s.ds, WEEK(MONDAY)) AS fecha_week,
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
    S.TOTAL_SESSION_MILLISECOND / 1000 AS session_time_sec
  FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_SESSION` AS s
  WHERE s.ds >= '2024-12-30'
    AND s.ds < CURRENT_DATE()
    AND s.SIT_SITE_ID IN UNNEST(['MLC', 'MLA', 'MLB', 'MLM', 'MCO', 'MPE', 'MLU', 'MEC'])
  GROUP BY ALL
),
SESSION_PLAY AS (
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
    SUM(P.PLAYBACK_TIME_MILLISECONDS / 1000) AS TSV,
    SUM(P.PLAYBACK_TIME_MILLISECONDS / 60000) AS TVM
  FROM SESSIONS AS S
  LEFT JOIN `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` AS P
    ON S.SIT_SITE_ID = P.SIT_SITE_ID
    AND s.USER_ID = P.USER_ID
    AND S.MELIDATA_SESSION_ID = P.SESSION_ID
    AND P.PLAYBACK_TIME_MILLISECONDS / 1000 >= 20
  GROUP BY ALL
),
-- CTE para obtener los datos consolidados
CONSOLIDATED_DATA AS (
    SELECT
        s.sit_site_id,
        DATE_TRUNC(s.DS, WEEK(MONDAY)) AS WEEK_ID,
        COALESCE(o.SOURCE_SESSION_L1, 'Sin Clasificacion') AS Clasificacion,
        CONCAT(o.SOURCE_SESSION_L1,'-', o.SOURCE_SESSION_L2) AS touchpoint_no_team,
        COUNT(DISTINCT s.melidata_session_id) AS Sessions,
        COUNT(DISTINCT CASE WHEN s.TSV >= 20 THEN s.melidata_session_id ELSE NULL END) AS Sessions_valid_view
    FROM SESSION_PLAY s
    LEFT JOIN `meli-sbox.MPLAY.LK_MPLAY_SOURCE_TYPE_ORIGIN_SESSION` o
        ON COALESCE(s.FIRST_EVENT_SOURCE, 'NULL') = COALESCE(o.SOURCE_TYPE, 'NULL')
    GROUP BY
        s.sit_site_id,
        WEEK_ID,
        Clasificacion,
        touchpoint_no_team
),
---
## **Análisis Macro (por Clasificación)**
---
MACRO_ANALYSIS AS (
    SELECT
        Clasificacion,
        WEEK_ID,
        SUM(Sessions) AS Sessions,
        SUM(Sessions_valid_view) AS Sessions_valid_view,
        SAFE_DIVIDE(SUM(Sessions_valid_view), SUM(Sessions)) AS CVR_Sessions
    FROM CONSOLIDATED_DATA
    GROUP BY
        Clasificacion,
        WEEK_ID
),
MACRO_PIVOTED AS (
    SELECT
        Clasificacion,
        MAX(CASE WHEN WEEK_ID = date_a_start THEN Sessions ELSE NULL END) AS Sessions_A,
        MAX(CASE WHEN WEEK_ID = date_b_start THEN Sessions ELSE NULL END) AS Sessions_B,
        MAX(CASE WHEN WEEK_ID = date_a_start THEN Sessions_valid_view ELSE NULL END) AS Sessions_valid_view_A,
        MAX(CASE WHEN WEEK_ID = date_b_start THEN Sessions_valid_view ELSE NULL END) AS Sessions_valid_view_B,
        MAX(CASE WHEN WEEK_ID = date_a_start THEN CVR_Sessions ELSE NULL END) AS CVR_Sessions_A,
        MAX(CASE WHEN WEEK_ID = date_b_start THEN CVR_Sessions ELSE NULL END) AS CVR_Sessions_B
    FROM MACRO_ANALYSIS
    GROUP BY
        Clasificacion
),
MACRO_FINAL AS (
    SELECT
        Clasificacion,
        Sessions_valid_view_A,
        Sessions_valid_view_B,
        ROUND((Sessions_valid_view_A - Sessions_valid_view_B) / NULLIF(Sessions_valid_view_B, 0), 4) * 100 AS Sessions_valid_view_WoW_Change,
        CVR_Sessions_A,
        CVR_Sessions_B,
        ROUND((CVR_Sessions_A - CVR_Sessions_B) / NULLIF(CVR_Sessions_B, 0), 4) * 100 AS CVR_Sessions_WoW_Change
    FROM MACRO_PIVOTED
),
---
## **Análisis Micro (por Touchpoint)**
---
MICRO_ANALYSIS AS (
    SELECT
        touchpoint_no_team,
        WEEK_ID,
        SUM(Sessions) AS Sessions,
        SUM(Sessions_valid_view) AS Sessions_valid_view,
        SAFE_DIVIDE(SUM(Sessions_valid_view), SUM(Sessions)) AS CVR_Sessions
    FROM CONSOLIDATED_DATA
    GROUP BY
        touchpoint_no_team,
        WEEK_ID
),
MICRO_PIVOTED AS (
    SELECT
        touchpoint_no_team,
        MAX(CASE WHEN WEEK_ID = date_a_start THEN Sessions ELSE NULL END) AS Sessions_A,
        MAX(CASE WHEN WEEK_ID = date_b_start THEN Sessions ELSE NULL END) AS Sessions_B,
        MAX(CASE WHEN WEEK_ID = date_a_start THEN Sessions_valid_view ELSE NULL END) AS Sessions_valid_view_A,
        MAX(CASE WHEN WEEK_ID = date_b_start THEN Sessions_valid_view ELSE NULL END) AS Sessions_valid_view_B,
        MAX(CASE WHEN WEEK_ID = date_a_start THEN CVR_Sessions ELSE NULL END) AS CVR_Sessions_A,
        MAX(CASE WHEN WEEK_ID = date_b_start THEN CVR_Sessions ELSE NULL END) AS CVR_Sessions_B
    FROM MICRO_ANALYSIS
    GROUP BY
        touchpoint_no_team
),
MICRO_FINAL AS (
    SELECT
        touchpoint_no_team,
        Sessions_A,
        Sessions_B,
        Sessions_valid_view_A,
        Sessions_valid_view_B,
        CVR_Sessions_A,
        CVR_Sessions_B,
        ROUND((Sessions_A - Sessions_B) / NULLIF(Sessions_B, 0), 4) * 100 AS Sessions_WoW_Change,
        ROUND((Sessions_valid_view_A - Sessions_valid_view_B) / NULLIF(Sessions_valid_view_B, 0), 4) * 100 AS Sessions_valid_view_WoW_Change,
        ROUND((CVR_Sessions_A - CVR_Sessions_B) / NULLIF(CVR_Sessions_B, 0), 4) * 100 AS CVR_WoW_Change
    FROM MICRO_PIVOTED
    WHERE Sessions_valid_view_A >= 500 -- Filtrado por relevancia
)

-- Selección final para mostrar ambos análisis
SELECT * FROM MACRO_FINAL
UNION ALL
SELECT * FROM (
    SELECT
        touchpoint_no_team AS Clasificacion,
        Sessions_valid_view_A,
        Sessions_valid_view_B,
        Sessions_valid_view_WoW_Change,
        CVR_Sessions_A,
        CVR_Sessions_B,
        CVR_WoW_Change
    FROM MICRO_FINAL
    ORDER BY Sessions_valid_view_WoW_Change DESC
    LIMIT 5
)
UNION ALL
SELECT * FROM (
    SELECT
        touchpoint_no_team AS Clasificacion,        
        Sessions_valid_view_A,
        Sessions_valid_view_B,
        Sessions_valid_view_WoW_Change,
        CVR_Sessions_A,
        CVR_Sessions_B,
        CVR_WoW_Change
    FROM MICRO_FINAL
    ORDER BY Sessions_valid_view_WoW_Change ASC
    LIMIT 5
)
ORDER BY Clasificacion