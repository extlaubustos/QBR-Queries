-- Selecciona las últimas 6 semanas de datos
WITH semanas_ultimas AS (
  SELECT DISTINCT week_id
  FROM `meli-sbox.MPLAY.MPLAY_TOUCHPOINT_USER`
  ORDER BY week_id DESC
  LIMIT 6
),

-- Agrupa datos por touchpoint y semana
ultimas_semanas AS (
  SELECT
    touchpoint_no_team,
    week_id,
    SUM(sessions) AS total_sessions
  FROM `meli-sbox.MPLAY.MPLAY_TOUCHPOINT_USER`
  WHERE week_id IN (SELECT week_id FROM semanas_ultimas)
  GROUP BY touchpoint_no_team, week_id
),

-- Calcula media, desviación, sesiones de semana actual y anterior y cuenta semanas con datos, si no tiene 6 semanas con datos se lo desprecia
stats AS (
  SELECT
    touchpoint_no_team,
    AVG(total_sessions) AS media,
    STDDEV(total_sessions) AS desviacion,
    ARRAY_AGG(total_sessions ORDER BY week_id DESC LIMIT 1)[OFFSET(0)] AS semana_actual,
    ARRAY_AGG(total_sessions ORDER BY week_id DESC LIMIT 2)[OFFSET(1)] AS semana_anterior,
    COUNT(week_id) as semanas_con_datos
  FROM ultimas_semanas
  GROUP BY touchpoint_no_team
  HAVING semanas_con_datos >= 6
),

-- Analiza sólo touchpoints con suficientes semanas
analisis AS (
  SELECT
    touchpoint_no_team,
    semana_actual,
    semana_anterior,
    media,
    desviacion,
    semana_actual - semana_anterior AS variacion_abs,
    SAFE_DIVIDE(semana_actual - semana_anterior, semana_anterior) * 100 AS variacion_pct,
    SAFE_DIVIDE(semana_actual - media, desviacion) AS z_score,
    SAFE_DIVIDE(semana_anterior - media, desviacion) AS z_score_anterior
  FROM stats
  WHERE semanas_con_datos >= 2
)

-- solo touchpoints realmente relevantes
SELECT
  touchpoint_no_team,
  semana_actual AS sesiones_semana_actual,
  semana_anterior AS sesiones_semana_anterior,
  media AS promedio_6_semanas,
  desviacion,
  variacion_abs,
  ROUND(variacion_pct,2) AS variacion_pct,
  ROUND(z_score,2) AS z_score_actual,
  ROUND(z_score_anterior,2) AS z_score_anterior
FROM analisis
WHERE desviacion > 0
  AND ABS(z_score) >= 1
  AND semana_anterior > 20
ORDER BY ABS(z_score) DESC
LIMIT 15









