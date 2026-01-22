WITH RAW_DATA AS (
    SELECT
      TOUCHPOINT_NO_TEAM,
      COALESCE(SIT_SITE_ID, 'N/A') AS SITE, 
      COALESCE(PLATFORM, 'N/A') AS PLATFORM,
      TIMEFRAME_ID,
      EXTRACT(DAYOFWEEK FROM TIMEFRAME_ID) AS DAY_OF_WEEK,
      Sessions
    FROM `meli-sbox.MPLAY.MPLAY_TOUCHPOINT_USER`
    WHERE TIMEFRAME_ID >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY) -- Suficiente para 4 días similares
      AND TIMEFRAME_TYPE = 'DAILY'
      AND LOWER(TOUCHPOINT_NO_TEAM) NOT LIKE '%mplay-hub%'
      AND LOWER(TOUCHPOINT_NO_TEAM) NOT LIKE '%otros%'
  ),
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
  stats_similares AS (
    SELECT
      TOUCHPOINT_NO_TEAM, analysis_level, site, platform, DAY_OF_WEEK,
      AVG(TOTAL_SESSIONS) AS media_similar,
      STDDEV(TOTAL_SESSIONS) AS desviacion_similar,
      COUNT(*) AS n_dias
    FROM DSS_BASE
    WHERE TIMEFRAME_ID < CURRENT_DATE() - 1 -- Excluimos el día a analizar
    GROUP BY ALL
    HAVING n_dias >= 4
  ),
  hoy AS (
    SELECT * FROM DSS_BASE WHERE TIMEFRAME_ID = CURRENT_DATE() - 1
  ),
  final_analysis AS (
    SELECT
      h.TOUCHPOINT_NO_TEAM AS touchpoint,
      h.site,
      h.platform,
      h.analysis_level,
      h.TIMEFRAME_ID AS date,
      h.TOTAL_SESSIONS AS value,
      s.media_similar AS expected,
      (h.TOTAL_SESSIONS - s.media_similar) AS delta_abs,
      SAFE_DIVIDE(h.TOTAL_SESSIONS - s.media_similar, s.media_similar) * 100 AS delta_pct,
      SAFE_DIVIDE(h.TOTAL_SESSIONS - s.media_similar, NULLIF(s.desviacion_similar, 0)) AS z_score,
      s.desviacion_similar
    FROM hoy h
    INNER JOIN stats_similares s 
      USING (TOUCHPOINT_NO_TEAM, analysis_level, site, platform, DAY_OF_WEEK)
  ),
  FINAL_BASE AS (
    SELECT
    -- Alert Key única: Touchpoint|Site|Platform|Rule
    CONCAT(touchpoint, '|', site, '|', platform, '|PREV_AVG_4D') AS alert_key,
    'PREV_AVG_4D' AS rule_type,
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
    '4_similar_days_avg' AS baseline,
    -- Definición de severidad basada en Z-Score
    CASE 
        WHEN z_score <= -3 THEN 'critical'
        WHEN z_score <= -2 THEN 'warning'
        WHEN z_score <= -1.5 THEN 'alert'
        ELSE 'ignore'
    END AS severity_raw,
    -- Estructura de detalles para el JSON
    STRUCT(
        ROUND(z_score, 2) AS z_score,
        ROUND(desviacion_similar, 2) AS std_dev,
        '4_days_avg' AS method
    ) AS details
FROM final_analysis
WHERE ABS(z_score) >= 1.5 -- Solo traemos lo que al menos sea 'alert' para no saturar n8n
  )
SELECT * FROM FINAL_BASE
WHERE severity_raw != 'ignore'
AND ABS(details.z_score) >= 2;