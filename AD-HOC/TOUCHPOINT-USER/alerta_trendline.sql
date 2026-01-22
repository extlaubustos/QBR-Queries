WITH RAW_DATA AS (
    SELECT
      TOUCHPOINT_NO_TEAM,
      COALESCE(SIT_SITE_ID, 'N/A') AS SITE, 
      COALESCE(PLATFORM, 'N/A') AS PLATFORM,
      TIMEFRAME_ID,
      Sessions
    FROM `meli-sbox.MPLAY.MPLAY_TOUCHPOINT_USER`
    WHERE TIMEFRAME_TYPE = 'DAILY'
      AND LOWER(TOUCHPOINT_NO_TEAM) NOT LIKE '%mplay hub%'
      AND LOWER(TOUCHPOINT_NO_TEAM) NOT LIKE '%otros%'
      AND TIMEFRAME_ID >= DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY)
  ),
  
  DSS_BASE AS (
    SELECT 
      TOUCHPOINT_NO_TEAM, 'overall' AS analysis_level, 'GLOBAL' AS site, 'ALL' AS platform,
      TIMEFRAME_ID, ROUND(SUM(Sessions), 2) AS TOTAL_SESSIONS
    FROM RAW_DATA GROUP BY ALL
    UNION ALL
    SELECT 
      TOUCHPOINT_NO_TEAM, 'site' AS analysis_level, SITE AS site, 'ALL' AS platform,
      TIMEFRAME_ID, ROUND(SUM(Sessions), 2) AS TOTAL_SESSIONS
    FROM RAW_DATA GROUP BY ALL
    UNION ALL
    SELECT 
      TOUCHPOINT_NO_TEAM, 'platform' AS analysis_level, SITE AS site, PLATFORM AS platform,
      TIMEFRAME_ID, ROUND(SUM(Sessions), 2) AS TOTAL_SESSIONS
    FROM RAW_DATA GROUP BY ALL
  ),

  ParetoCalc AS (
    SELECT
        *,
        SUM(TOTAL_SESSIONS) OVER (PARTITION BY TIMEFRAME_ID, analysis_level, site, platform) AS TotalDailySessions,
        SAFE_DIVIDE(TOTAL_SESSIONS, SUM(TOTAL_SESSIONS) OVER (PARTITION BY TIMEFRAME_ID, analysis_level, site, platform)) AS SessionShare
    FROM DSS_BASE
  ),
  DSS_FILTRADA AS (
    SELECT * FROM (
        SELECT *, SUM(SessionShare) OVER (PARTITION BY TIMEFRAME_ID, analysis_level, site, platform ORDER BY TOTAL_SESSIONS DESC) AS CumulativeShare
        FROM ParetoCalc
    ) WHERE CumulativeShare <= 0.90 
  ),

  trend_calc AS (
    SELECT
        TOUCHPOINT_NO_TEAM, analysis_level, site, platform,
        AVG(IF(TIMEFRAME_ID BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 9 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY), TOTAL_SESSIONS, NULL)) AS avg_start,
        AVG(IF(TIMEFRAME_ID BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY), TOTAL_SESSIONS, NULL)) AS avg_end,
        MAX(IF(TIMEFRAME_ID = CURRENT_DATE() - 2, TOTAL_SESSIONS, 0)) AS value_yesterday
    FROM DSS_FILTRADA
    GROUP BY ALL
    HAVING avg_start >= 1000 
  ),

  final_analysis AS (
    SELECT
        *,
        (avg_end - avg_start) AS delta_abs,
        SAFE_DIVIDE(avg_end - avg_start, avg_start) * 100 AS delta_pct
    FROM trend_calc
  )

SELECT
    CONCAT(TOUCHPOINT_NO_TEAM, '|', site, '|', platform, '|TREND_', IF(delta_pct > 0, 'UP', 'DOWN')) AS alert_key,
    CONCAT('TREND_', IF(delta_pct > 0, 'UP', 'DOWN')) AS rule_type,
    'trend' AS category,
    TOUCHPOINT_NO_TEAM AS touchpoint,
    site,
    platform,
    analysis_level,
    CAST(CURRENT_DATE() - 2 AS STRING) AS date,
    CAST(ROUND(value_yesterday, 2) AS STRING) AS value,
    CAST(ROUND(avg_start, 2) AS STRING) AS expected,
    CAST(ROUND(delta_abs, 2) AS STRING) AS delta_abs,
    CAST(ROUND(delta_pct, 2) AS STRING) AS delta_pct,
    '7_days_start_avg' AS baseline,
    CASE 
        WHEN ABS(delta_pct) >= 50 THEN 'critical'
        WHEN ABS(delta_pct) >= 40 THEN 'warning'
        ELSE 'alert'
    END AS severity_raw,
    STRUCT(
        ROUND(avg_start, 2) AS start_window_avg,
        ROUND(avg_end, 2) AS end_window_avg,
        IF(delta_pct > 0, 'growth', 'degradation') AS trend_direction
    ) AS details
FROM final_analysis
WHERE ABS(delta_pct) >= 30 