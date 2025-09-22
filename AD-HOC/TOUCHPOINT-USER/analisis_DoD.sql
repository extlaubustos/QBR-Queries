-- Base filtrada para últimos 40 días
WITH DSS_BASE AS (
  SELECT
    TOUCHPOINT_NO_TEAM,
    DAY_ID,
    EXTRACT(DAYOFWEEK FROM DAY_ID) AS DAY_OF_WEEK, -- 1=Domingo, 7=Sábado
    ROUND(SUM(Sessions), 2) AS TOTAL_SESSIONS
  FROM `meli-sbox.MPLAY.MPLAY_TOUCHPOINT_USER`
  WHERE DAY_ID >= DATE_SUB(CURRENT_DATE(), INTERVAL 40 DAY)
  GROUP BY ALL
),

-- Determina día de la semana de cada fecha
ETIQUETADAS AS (
  SELECT
    TOUCHPOINT_NO_TEAM, DAY_ID, DAY_OF_WEEK, TOTAL_SESSIONS
  FROM DSS_BASE
),

-- Día de análisis: ayer
hoy_tag AS (
  SELECT
    touchpoint_no_team,
    DAY_ID,
    total_sessions,
    day_of_week
  FROM DSS_BASE
  WHERE DAY_ID = CURRENT_DATE()-1
),

-- Para cada touchpoint y día, trae los últimos 4 días de semana iguales anteriores a ayer
ultimos_similares AS (
  SELECT
    t1.touchpoint_no_team,
    t1.DAY_ID,
    t1.total_sessions,
    t1.day_of_week,
    hoy.DAY_ID AS fecha_analisis,
    hoy.day_of_week AS dia_semanal_analisis,
    ROW_NUMBER() OVER (
      PARTITION BY t1.touchpoint_no_team, t1.day_of_week
      ORDER BY t1.DAY_ID DESC
    ) AS rn
  FROM ETIQUETADAS t1
  JOIN hoy_tag hoy
    ON t1.touchpoint_no_team = hoy.touchpoint_no_team
    AND t1.day_of_week = hoy.day_of_week
    AND t1.DAY_ID < hoy.DAY_ID -- solo fechas anteriores a la de análisis
),

ultimos_4_similares AS (
  SELECT *
  FROM ultimos_similares
  WHERE rn <= 4
),

-- Calcula stats usando solo los últimos 4 días "similares"
stats_similares AS (
  SELECT
    touchpoint_no_team,
    day_of_week,
    COUNT(*) AS n_similares,
    ROUND(AVG(total_sessions), 2) AS media_similar,
    ROUND(STDDEV(total_sessions), 2) AS desviacion_similar
  FROM ultimos_4_similares
  GROUP BY touchpoint_no_team, day_of_week
  HAVING n_similares = 4
),

-- Unimos stats con el día de análisis (ayer)
join_stats AS (
  SELECT
    h.touchpoint_no_team, h.DAY_ID, h.total_sessions, h.day_of_week,
    s.media_similar, s.desviacion_similar
  FROM hoy_tag h
  LEFT JOIN stats_similares s
    ON h.touchpoint_no_team = s.touchpoint_no_team
   AND h.day_of_week = s.day_of_week
),

final AS (
  SELECT
    touchpoint_no_team,
    DAY_ID AS fecha,
    day_of_week,
    total_sessions AS sesiones_ayer,
    media_similar AS media_ventana,
    desviacion_similar AS desviacion_ventana,
    -- Agregado: variación porcentual respecto al promedio de los días similares
    ROUND(SAFE_DIVIDE(total_sessions - media_similar, media_similar) * 100, 2) AS variacion_pct,
    -- También mantenemos el z_score por si filtras por ahí
    ROUND(SAFE_DIVIDE(total_sessions - media_similar, desviacion_similar), 2) AS z_score
  FROM join_stats
)

SELECT
  *,
  CASE day_of_week
    WHEN 1 THEN 'Domingo'
    WHEN 2 THEN 'Lunes'
    WHEN 3 THEN 'Martes'
    WHEN 4 THEN 'Miércoles'
    WHEN 5 THEN 'Jueves'
    WHEN 6 THEN 'Viernes'
    WHEN 7 THEN 'Sábado'
    ELSE CAST(day_of_week AS STRING)
  END AS dia_nombre
FROM final
WHERE ABS(z_score) >= 2
  AND desviacion_ventana > 0
ORDER BY ABS(z_score) DESC
