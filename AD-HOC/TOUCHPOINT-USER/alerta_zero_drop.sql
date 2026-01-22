WITH base AS (
  SELECT
    TOUCHPOINT_NO_TEAM,
    SIT_SITE_ID,
    TIMEFRAME_ID,
    SESSIONS
  FROM `meli-sbox.MPLAY.MPLAY_TOUCHPOINT_USER`
  WHERE TIMEFRAME_TYPE = 'DAILY'
    AND TIMEFRAME_ID BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 6 DAY)
                          AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND LOWER(TOUCHPOINT_NO_TEAM) NOT LIKE '%mplay hub%'
    AND LOWER(TOUCHPOINT_NO_TEAM) NOT LIKE '%otros%'
),

-- =========================
-- SITE LEVEL ELIGIBILITY
-- =========================
site_eligible AS (
  SELECT
    TOUCHPOINT_NO_TEAM,
    SIT_SITE_ID,
    SUM(SESSIONS) AS sessions_5d,
    COUNT(DISTINCT TIMEFRAME_ID) AS days_present,
    'site' AS analysis_level
  FROM base
  GROUP BY TOUCHPOINT_NO_TEAM, SIT_SITE_ID
  HAVING
    COUNT(DISTINCT TIMEFRAME_ID) = 5
    AND SUM(SESSIONS) / 5 >= 1000
),

-- =========================
-- GENERAL LEVEL ELIGIBILITY
-- =========================
general_eligible AS (
  SELECT
    TOUCHPOINT_NO_TEAM,
    'ALL' AS SIT_SITE_ID,
    SUM(SESSIONS) AS sessions_5d,
    COUNT(DISTINCT TIMEFRAME_ID) AS days_present,
    'overall' AS analysis_level
  FROM base
  GROUP BY TOUCHPOINT_NO_TEAM
  HAVING
    COUNT(DISTINCT TIMEFRAME_ID) = 5
    AND SUM(SESSIONS) / 5 >= 1000
),

eligible AS (
  SELECT * FROM site_eligible
  UNION ALL
  SELECT * FROM general_eligible
),

-- =========================
-- YESTERDAY PRESENCE
-- =========================
yesterday AS (
  SELECT
    TOUCHPOINT_NO_TEAM,
    SIT_SITE_ID
  FROM `meli-sbox.MPLAY.MPLAY_TOUCHPOINT_USER`
  WHERE TIMEFRAME_TYPE = 'DAILY'
    AND TIMEFRAME_ID = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  GROUP BY TOUCHPOINT_NO_TEAM, SIT_SITE_ID

  UNION ALL

  SELECT
    TOUCHPOINT_NO_TEAM,
    'ALL' AS SIT_SITE_ID
  FROM `meli-sbox.MPLAY.MPLAY_TOUCHPOINT_USER`
  WHERE TIMEFRAME_TYPE = 'DAILY'
    AND TIMEFRAME_ID = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  GROUP BY TOUCHPOINT_NO_TEAM
)

SELECT
  CONCAT(
    e.TOUCHPOINT_NO_TEAM, '|',
    e.SIT_SITE_ID, '|ALL|ZERO_DROP'
  ) AS alert_key,
  'ZERO_DROP' AS rule_type,
  'warning' AS category,
  e.TOUCHPOINT_NO_TEAM AS touchpoint,
  e.SIT_SITE_ID AS site,
  'ALL' AS platform,
  e.analysis_level,
  DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS date,
  0 AS value,
  ROUND(e.sessions_5d / 5, 0) AS expected,
  -ROUND(e.sessions_5d / 5, 0) AS delta_abs,
  -100 AS delta_pct,
  'last_5_days_avg' AS baseline,
  'critical' AS severity_raw,
  STRUCT(
    e.sessions_5d AS sessions_last_5_days,
    ROUND(e.sessions_5d / 5, 0) AS avg_sessions
  ) AS details
FROM eligible e
LEFT JOIN yesterday y
  ON e.TOUCHPOINT_NO_TEAM = y.TOUCHPOINT_NO_TEAM
 AND e.SIT_SITE_ID = y.SIT_SITE_ID
WHERE y.TOUCHPOINT_NO_TEAM IS NULL
