WITH base AS (
  SELECT
    TOUCHPOINT_NO_TEAM,
    SIT_SITE_ID,
    TIMEFRAME_ID,
    SUM(SESSIONS) AS sessions
  FROM `meli-sbox.MPLAY.MPLAY_TOUCHPOINT_USER`
  WHERE TIMEFRAME_TYPE = 'DAILY'
    AND TIMEFRAME_ID IN (
      DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY),
      DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY)
    )
    AND LOWER(TOUCHPOINT_NO_TEAM) NOT LIKE '%mplay hub%'
    AND LOWER(TOUCHPOINT_NO_TEAM) NOT LIKE '%otros%'
  GROUP BY TOUCHPOINT_NO_TEAM, SIT_SITE_ID, TIMEFRAME_ID
),

pivoted AS (
  SELECT
    TOUCHPOINT_NO_TEAM,
    SIT_SITE_ID,
    MAX(IF(TIMEFRAME_ID = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), sessions, NULL)) AS sessions_yesterday,
    MAX(IF(TIMEFRAME_ID = DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY), sessions, NULL)) AS sessions_wow
  FROM base
  GROUP BY TOUCHPOINT_NO_TEAM, SIT_SITE_ID
),

-- Nivel General
general AS (
  SELECT
    TOUCHPOINT_NO_TEAM,
    'ALL' AS SIT_SITE_ID,
    SUM(sessions_yesterday) AS sessions_yesterday,
    SUM(sessions_wow) AS sessions_wow
  FROM pivoted
  GROUP BY TOUCHPOINT_NO_TEAM
),

final_base as (
  
SELECT
  CONCAT(TOUCHPOINT_NO_TEAM, '|', SIT_SITE_ID, '|WOW') AS alert_key,
  'WOW' AS rule_type,
  'warning' AS category,
  TOUCHPOINT_NO_TEAM AS touchpoint,
  SIT_SITE_ID AS site,
  'ALL' AS platform,
  CASE WHEN SIT_SITE_ID = 'ALL' THEN 'general' ELSE 'site' END AS analysis_level,
  DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS date,
  sessions_yesterday AS value,
  sessions_wow AS expected,
  sessions_yesterday - sessions_wow AS delta_abs,
  SAFE_DIVIDE(sessions_yesterday - sessions_wow, sessions_wow) * 100 AS delta_pct,
  'WoW' AS baseline,
  CASE
    WHEN SAFE_DIVIDE(sessions_yesterday - sessions_wow, sessions_wow) <= -0.7 THEN 'critical'
    WHEN SAFE_DIVIDE(sessions_yesterday - sessions_wow, sessions_wow) <= -0.5 THEN 'warning'
    WHEN SAFE_DIVIDE(sessions_yesterday - sessions_wow, sessions_wow) <= -0.3 THEN 'alert'
    ELSE 'ignore'
  END AS severity_raw,
  STRUCT(
    sessions_yesterday AS yesterday,
    sessions_wow AS last_week_same_day
  ) AS details
FROM pivoted

UNION ALL

SELECT
  CONCAT(TOUCHPOINT_NO_TEAM, '|', SIT_SITE_ID, '|WOW') AS alert_key,
  'WOW' AS rule_type,
  'warning' AS category,
  TOUCHPOINT_NO_TEAM AS touchpoint,
  SIT_SITE_ID AS site,
  'ALL' AS platform,
  'general' AS analysis_level,
  DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS date,
  sessions_yesterday AS value,
  sessions_wow AS expected,
  sessions_yesterday - sessions_wow AS delta_abs,
  SAFE_DIVIDE(sessions_yesterday - sessions_wow, sessions_wow) * 100 AS delta_pct,
  'WoW' AS baseline,
  CASE
    WHEN SAFE_DIVIDE(sessions_yesterday - sessions_wow, sessions_wow) <= -0.7 THEN 'critical'
    WHEN SAFE_DIVIDE(sessions_yesterday - sessions_wow, sessions_wow) <= -0.5 THEN 'warning'
    WHEN SAFE_DIVIDE(sessions_yesterday - sessions_wow, sessions_wow) <= -0.3 THEN 'alert'
    ELSE 'ignore'
  END AS severity_raw,
  STRUCT(
    sessions_yesterday AS yesterday,
    sessions_wow AS last_week_same_day
  ) AS details
FROM general
WHERE sessions_yesterday IS NOT NULL AND sessions_wow IS NOT NULL
  AND SAFE_DIVIDE(sessions_yesterday - sessions_wow, sessions_wow) <= -0.3
)
select *
from final_base
where severity_raw != 'ignore'
and expected >= 1000;