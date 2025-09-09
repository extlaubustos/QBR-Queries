WITH semanas_relevantes AS (
  SELECT
    MAX(week_id) AS semana_actual,
    MAX(week_id) - 7 AS semana_anterior
  FROM `meli-sbox.MPLAY.MPLAY_TOUCHPOINT_USER`
),

agg AS (
  SELECT
    clasificacion,
    week_id,
    SUM(Sessions) as total_sessions
  FROM `meli-sbox.MPLAY.MPLAY_TOUCHPOINT_USER`
  WHERE week_id IN (
    (SELECT semana_actual FROM semanas_relevantes),
    (SELECT semana_anterior FROM semanas_relevantes)
  )
  GROUP BY clasificacion, week_id
),

pivot AS (
  SELECT
    clasificacion,
    MAX(CASE WHEN week_id = (SELECT semana_actual FROM semanas_relevantes) THEN total_sessions END) AS sesiones_semana_actual,
    MAX(CASE WHEN week_id = (SELECT semana_anterior FROM semanas_relevantes) THEN total_sessions END) AS sesiones_semana_anterior
  FROM agg
  GROUP BY clasificacion
)

SELECT
  clasificacion,
  sesiones_semana_actual,
  sesiones_semana_anterior,
  sesiones_semana_actual - sesiones_semana_anterior AS variacion_abs,
  ROUND(SAFE_DIVIDE(sesiones_semana_actual - sesiones_semana_anterior, sesiones_semana_anterior) * 100, 2) AS variacion_pct
FROM pivot
ORDER BY ABS(variacion_pct) DESC

