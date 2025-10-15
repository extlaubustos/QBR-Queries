WITH tv_viewer_plays AS (
  SELECT
    USER_ID,
    DS
  FROM WHOWNER.BT_MKT_MPLAY_PLAYS
  WHERE
    USER_ID IS NOT NULL
    AND DEVICE_PLATFORM LIKE '/tv%'
    AND PLAYBACK_TIME_MILLISECONDS > 20000
    AND DS <= DATE '2025-09-30'
),
first_tv_view AS (
  SELECT
    USER_ID,
    MIN(DS) AS first_tv_ds
  FROM tv_viewer_plays
  GROUP BY USER_ID
)
SELECT
  FORMAT_DATE('%Y-%m', first_tv_ds) AS mes,
  COUNT(DISTINCT USER_ID) AS viewers_primera_vez_tv
FROM first_tv_view
WHERE first_tv_ds BETWEEN DATE '2025-04-01' AND DATE '2025-09-30'
GROUP BY mes
ORDER BY mes;