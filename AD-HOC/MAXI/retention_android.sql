WITH base AS (
  SELECT
    USER_ID,
    DATE_TRUNC(DS, MONTH) AS activity_month
  FROM WHOWNER.BT_MKT_MPLAY_PLAYS
  WHERE
    DS BETWEEN '2025-01-01' AND CURRENT_DATE - 1
    AND LOWER(DEVICE_PLATFORM) LIKE '%tv/android%'
    AND PLAYBACK_TIME_MILLISECONDS >= 20000
    AND USER_ID IS NOT NULL
  GROUP BY USER_ID, activity_month
),
cohorts AS (
  SELECT
    USER_ID,
    MIN(activity_month) AS cohort_month
  FROM base
  WHERE activity_month BETWEEN DATE '2025-01-01' AND DATE '2025-12-31'
  GROUP BY USER_ID
),
activity AS (
  SELECT
    b.USER_ID,
    c.cohort_month,
    b.activity_month,
    DATE_DIFF(b.activity_month, c.cohort_month, MONTH) AS month_offset
  FROM base b
  JOIN cohorts c USING (USER_ID)
  WHERE b.activity_month BETWEEN c.cohort_month AND DATE_ADD(c.cohort_month, INTERVAL 12 MONTH)
),
user_offsets AS (
  SELECT USER_ID, cohort_month, month_offset
  FROM activity
  GROUP BY USER_ID, cohort_month, month_offset
),
retention_user AS (
  SELECT
    u.USER_ID,
    u.cohort_month,
    x AS mx,
    COUNT(DISTINCT CASE WHEN uo.month_offset <= x THEN uo.month_offset END) AS months_present
  FROM (
    SELECT DISTINCT USER_ID, cohort_month FROM user_offsets
  ) u
  CROSS JOIN UNNEST(GENERATE_ARRAY(0,12)) AS x
  LEFT JOIN user_offsets uo
    ON uo.USER_ID = u.USER_ID
   AND uo.cohort_month = u.cohort_month
  GROUP BY u.USER_ID, u.cohort_month, x
),
retention AS (
  SELECT
    cohort_month,
    mx,
    COUNTIF(months_present = mx + 1) AS retained_users
  FROM retention_user
  GROUP BY cohort_month, mx
),
cohort_sizes AS (
  SELECT
    cohort_month,
    COUNT(DISTINCT USER_ID) AS cohort_users
  FROM cohorts
  GROUP BY cohort_month
)
SELECT
  cohort_month,
  CONCAT('M', CAST(mx AS STRING)) AS mx,
  retained_users,
  SAFE_DIVIDE(retained_users, cohort_users) AS retention_rate
FROM retention
JOIN cohort_sizes USING (cohort_month)
ORDER BY cohort_month, mx