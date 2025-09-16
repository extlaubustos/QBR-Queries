-- 1. Identifica últimos 30 días hábiles y últimos 4 fines de semana con datos
WITH DSS_BASE AS (
  SELECT
    TOUCHPOINT_NO_TEAM,
    DS,
    EXTRACT(DAYOFWEEK FROM DS) AS DAY_OF_WEEK, -- 1=Domingo, 7=Sábado
    ROUND(SUM(Sessions), 2) AS TOTAL_SESSIONS
  FROM `meli-sbox.MPLAY.MPLAY_TOUCHPOINT_USER`
  GROUP BY ALL
),

-- Determina si la ds es finde o hábil
ETIQUETADAS AS (
  SELECT
    TOUCHPOINT_NO_TEAM, DS, TOTAL_SESSIONS,
    CASE WHEN DAY_OF_WEEK IN (1,7) THEN 'finde' ELSE 'habil' END AS TIPO_DIA
  FROM DSS_BASE
),

ultimos_habiles AS (
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY touchpoint_no_team ORDER BY ds DESC) AS rn
    FROM etiquetadas
    WHERE tipo_dia = 'habil'
      AND ds < CURRENT_DATE()
  )
  WHERE rn <= 30
),
ultimos_findes AS (
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY touchpoint_no_team ORDER BY ds DESC) AS rn
    FROM etiquetadas
    WHERE tipo_dia = 'finde'
      AND ds < CURRENT_DATE()
  )
  WHERE rn <= 8
),
-- Día análisis: hoy
hoy_tag AS (
  SELECT
    touchpoint_no_team,
    ds,
    total_sessions,
    CASE WHEN EXTRACT(DAYOFWEEK FROM ds) IN (1,7) THEN 'finde' ELSE 'habil' END AS tipo_dia
  FROM dss_base
  WHERE ds = CURRENT_DATE()-1
),

-- para hábiles:
stats_habiles AS (
  SELECT
    touchpoint_no_team,
    COUNT(*) AS n_habiles,
    ROUND(AVG(total_sessions), 2) AS media_hab,
    ROUND(STDDEV(total_sessions), 2) AS desviacion_hab
  FROM ultimos_habiles
  GROUP BY touchpoint_no_team
  HAVING n_habiles >= 30
),
-- para findes:
stats_findes AS (
  SELECT
    touchpoint_no_team,
    COUNT(*) AS n_findes,
    ROUND(AVG(total_sessions), 2) AS media_find,
    ROUND(STDDEV(total_sessions), 2) AS desviacion_find
  FROM ultimos_findes
  GROUP BY touchpoint_no_team
  HAVING n_findes >= 8
),

-- Junta stats con el día actual según tipo
join_stats AS (
  SELECT
    hoy_tag.*,
    stats_habiles.media_hab, stats_habiles.desviacion_hab,
    stats_findes.media_find, stats_findes.desviacion_find
  FROM hoy_tag
  LEFT JOIN stats_habiles USING (touchpoint_no_team)
  LEFT JOIN stats_findes USING (touchpoint_no_team)
),

-- Calcula z-score según tipo de día
final AS (
  SELECT
    touchpoint_no_team,
    ds,
    tipo_dia,
    total_sessions,
    CASE
      WHEN tipo_dia = 'habil' THEN SAFE_DIVIDE(total_sessions - media_hab, desviacion_hab)
      ELSE SAFE_DIVIDE(total_sessions - media_find, desviacion_find)
    END AS z_score,
    CASE
      WHEN tipo_dia = 'habil' THEN desviacion_hab
      ELSE desviacion_find
    END AS desviacion_usada,
    CASE
      WHEN tipo_dia = 'habil' THEN media_hab
      ELSE media_find
    END AS media_usada
  FROM join_stats
)

SELECT
  touchpoint_no_team,
  ds AS fecha,
  tipo_dia,
  total_sessions AS sesiones_ayer,
  media_usada AS media_ventana,
  desviacion_usada AS desviacion_ventana,
  CASE
    WHEN tipo_dia = 'habil' THEN 'media_hab'
    ELSE 'media_find'
  END AS tipo_de_media,
  ROUND(z_score, 2) AS z_score
FROM final
WHERE ABS(z_score) >= 2
  AND desviacion_usada > 0
ORDER BY ABS(z_score) DESC