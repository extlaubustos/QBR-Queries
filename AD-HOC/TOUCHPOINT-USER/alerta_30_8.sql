WITH RAW_DATA AS (
    SELECT
      TOUCHPOINT_NO_TEAM,
      COALESCE(SIT_SITE_ID, 'N/A') AS SITE, 
      COALESCE(PLATFORM, 'N/A') AS PLATFORM,
      TIMEFRAME_ID,
      EXTRACT(DAYOFWEEK FROM TIMEFRAME_ID) AS DAY_OF_WEEK,
      Sessions
    FROM `meli-sbox.MPLAY.MPLAY_TOUCHPOINT_USER`
    WHERE TIMEFRAME_TYPE = 'DAILY'
      AND LOWER(TOUCHPOINT_NO_TEAM) NOT LIKE '%mplay hub%'
      AND LOWER(TOUCHPOINT_NO_TEAM) NOT LIKE '%otros%'
      -- Filtramos un rango amplio para cubrir los 30 días hábiles y 8 fines de semana
      AND TIMEFRAME_ID >= DATE_SUB(CURRENT_DATE(), INTERVAL 120 DAY)
  ),
  
  -- 1. Agrupación por los 3 niveles de análisis
  DSS_BASE AS (
    -- Nivel GLOBAL
    SELECT 
      TOUCHPOINT_NO_TEAM, 'overall' AS analysis_level, 'GLOBAL' AS site, 'ALL' AS platform,
      TIMEFRAME_ID, DAY_OF_WEEK, ROUND(SUM(Sessions), 2) AS TOTAL_SESSIONS
    FROM RAW_DATA GROUP BY ALL
    UNION ALL
    -- Nivel SITE
    SELECT 
      TOUCHPOINT_NO_TEAM, 'site' AS analysis_level, SITE AS site, 'ALL' AS platform,
      TIMEFRAME_ID, DAY_OF_WEEK, ROUND(SUM(Sessions), 2) AS TOTAL_SESSIONS
    FROM RAW_DATA GROUP BY ALL
    UNION ALL
    -- Nivel PLATFORM
    SELECT 
      TOUCHPOINT_NO_TEAM, 'platform' AS analysis_level, SITE AS site, PLATFORM AS platform,
      TIMEFRAME_ID, DAY_OF_WEEK, ROUND(SUM(Sessions), 2) AS TOTAL_SESSIONS
    FROM RAW_DATA GROUP BY ALL
  ),

  -- 2. Filtro de Pareto (80%) por nivel de análisis para evitar ruido de touchpoints pequeños
  ParetoCalc AS (
    SELECT
        *,
        SUM(TOTAL_SESSIONS) OVER (PARTITION BY TIMEFRAME_ID, analysis_level, site, platform) AS TotalDailySessions,
        SAFE_DIVIDE(TOTAL_SESSIONS, SUM(TOTAL_SESSIONS) OVER (PARTITION BY TIMEFRAME_ID, analysis_level, site, platform)) AS SessionShare
    FROM DSS_BASE
  ),
  ParetoCumulative AS (
    SELECT
        *,
        SUM(SessionShare) OVER (
            PARTITION BY TIMEFRAME_ID, analysis_level, site, platform
            ORDER BY TOTAL_SESSIONS DESC
            ROWS UNBOUNDED PRECEDING
        ) AS CumulativeShare
    FROM ParetoCalc
  ),
  DSS_FILTRADA AS (
    SELECT * FROM ParetoCumulative WHERE CumulativeShare <= 0.85 -- Un poco más flexible que el 80% para no perder touchpoints en crecimiento
  ),

  -- 3. Clasificación y Ventanas (30 hábiles / 8 findes)
  ETIQUETADAS AS (
    SELECT
      *,
      CASE WHEN DAY_OF_WEEK IN (1,7) THEN 'finde' ELSE 'habil' END AS tipo_dia
    FROM DSS_FILTRADA
  ),
  
  stats_precalc AS (
    SELECT
      TOUCHPOINT_NO_TEAM, analysis_level, site, platform, tipo_dia,
      AVG(TOTAL_SESSIONS) AS media_ventana,
      STDDEV(TOTAL_SESSIONS) AS desviacion_ventana,
      COUNT(*) AS n_puntos
    FROM (
        SELECT *, 
               ROW_NUMBER() OVER(PARTITION BY TOUCHPOINT_NO_TEAM, analysis_level, site, platform, tipo_dia ORDER BY TIMEFRAME_ID DESC) as rn
        FROM ETIQUETADAS
        WHERE TIMEFRAME_ID < CURRENT_DATE() - 2
    )
    WHERE (tipo_dia = 'habil' AND rn <= 30) OR (tipo_dia = 'finde' AND rn <= 8)
    GROUP BY ALL
    HAVING (tipo_dia = 'habil' AND n_puntos >= 20) OR (tipo_dia = 'finde' AND n_puntos >= 6) -- Flexibilidad mínima
  ),

  hoy AS (
    SELECT 
        *,
        CASE WHEN DAY_OF_WEEK IN (1,7) THEN 'finde' ELSE 'habil' END AS tipo_dia
    FROM DSS_BASE 
    WHERE TIMEFRAME_ID = CURRENT_DATE() - 2
  ),

  final_analysis AS (
    SELECT
      h.TOUCHPOINT_NO_TEAM AS touchpoint,
      h.site,
      h.platform,
      h.analysis_level,
      h.TIMEFRAME_ID AS date,
      h.TOTAL_SESSIONS AS value,
      s.media_ventana AS expected,
      (h.TOTAL_SESSIONS - s.media_ventana) AS delta_abs,
      SAFE_DIVIDE(h.TOTAL_SESSIONS - s.media_ventana, s.media_ventana) * 100 AS delta_pct,
      SAFE_DIVIDE(h.TOTAL_SESSIONS - s.media_ventana, NULLIF(s.desviacion_ventana, 0)) AS z_score,
      s.desviacion_ventana,
      h.tipo_dia
    FROM hoy h
    INNER JOIN stats_precalc s 
      USING (TOUCHPOINT_NO_TEAM, analysis_level, site, platform, tipo_dia)
  ),
   final_base as (
    SELECT
    CONCAT(touchpoint, '|', site, '|', platform, '|PREV_30H_8F') AS alert_key,
    'PREV_30H_8F' AS rule_type,
    'prevention' AS category,
    touchpoint,
    site,
    platform,
    analysis_level,
    CAST(date AS STRING) AS date,
    CAST(value AS STRING) AS value,
    CAST(ROUND(expected, 2) AS STRING) AS expected,
    CAST(ROUND(delta_abs, 2) AS STRING) AS delta_abs,
    CAST(ROUND(delta_pct, 2) AS STRING) AS delta_pct,
    CONCAT('avg_last_', IF(tipo_dia='habil', '30_business', '8_weekend')) AS baseline,
    CASE 
        WHEN z_score <= -3 THEN 'critical'
        WHEN z_score <= -2 THEN 'warning'
        WHEN z_score <= -1.5 THEN 'alert'
        ELSE 'ignore'
    END AS severity_raw,
    STRUCT(
        ROUND(z_score, 2) AS z_score,
        ROUND(desviacion_ventana, 2) AS std_dev,
        tipo_dia AS day_type
    ) AS details
FROM final_analysis
WHERE ABS(z_score) >= 1.5
  )
  select *
  from final_base
  where severity_raw != 'ignore'
  and abs(details.z_score) >= 2
  order by severity_raw desc, details.z_score desc