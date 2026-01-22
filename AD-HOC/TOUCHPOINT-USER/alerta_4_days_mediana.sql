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
      AND LOWER(TOUCHPOINT_NO_TEAM) NOT LIKE '%mplay-hub%'
      AND LOWER(TOUCHPOINT_NO_TEAM) NOT LIKE '%otros%'
      AND TIMEFRAME_ID >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
  ),
  
  -- 1. Agrupación Multinivel
  DSS_BASE AS (
    SELECT 
      TOUCHPOINT_NO_TEAM, 'overall' AS analysis_level, 'GLOBAL' AS site, 'ALL' AS platform,
      TIMEFRAME_ID, DAY_OF_WEEK, ROUND(SUM(Sessions), 2) AS TOTAL_SESSIONS
    FROM RAW_DATA GROUP BY ALL
    UNION ALL
    SELECT 
      TOUCHPOINT_NO_TEAM, 'site' AS analysis_level, SITE AS site, 'ALL' AS platform,
      TIMEFRAME_ID, DAY_OF_WEEK, ROUND(SUM(Sessions), 2) AS TOTAL_SESSIONS
    FROM RAW_DATA GROUP BY ALL
    UNION ALL
    SELECT 
      TOUCHPOINT_NO_TEAM, 'platform' AS analysis_level, SITE AS site, PLATFORM AS platform,
      TIMEFRAME_ID, DAY_OF_WEEK, ROUND(SUM(Sessions), 2) AS TOTAL_SESSIONS
    FROM RAW_DATA GROUP BY ALL
  ),

  -- 2. Filtro de Pareto (85%)
  ParetoCalc AS (
    SELECT
        *,
        SUM(TOTAL_SESSIONS) OVER (PARTITION BY TIMEFRAME_ID, analysis_level, site, platform) AS TotalDailySessions,
        SAFE_DIVIDE(TOTAL_SESSIONS, SUM(TOTAL_SESSIONS) OVER (PARTITION BY TIMEFRAME_ID, analysis_level, site, platform)) AS SessionShare
    FROM DSS_BASE
  ),
  DSS_FILTRADA AS (
    SELECT * FROM (
        SELECT *, SUM(SessionShare) OVER (PARTITION BY TIMEFRAME_ID, analysis_level, site, platform ORDER BY TOTAL_SESSIONS DESC) AS CumulativeShare
        FROM ParetoCalc
    ) WHERE CumulativeShare <= 0.85
  ),

  -- 3. Identificación de Ventana (Últimos 4 días similares)
  hoy AS (
    SELECT * FROM DSS_BASE WHERE TIMEFRAME_ID = CURRENT_DATE() - 1
  ),
  
  ventana_similares AS (
    SELECT
      f.*,
      ROW_NUMBER() OVER (PARTITION BY f.TOUCHPOINT_NO_TEAM, f.analysis_level, f.site, f.platform, f.DAY_OF_WEEK ORDER BY f.TIMEFRAME_ID DESC) AS rn
    FROM DSS_FILTRADA f
    INNER JOIN hoy h USING (TOUCHPOINT_NO_TEAM, analysis_level, site, platform, DAY_OF_WEEK)
    WHERE f.TIMEFRAME_ID < CURRENT_DATE() - 1
  ),

  -- 4. Cálculo de Mediana y MAD
  stats_mediana AS (
    SELECT
      TOUCHPOINT_NO_TEAM, analysis_level, site, platform, DAY_OF_WEEK,
      -- Mediana de los últimos 4 días
      APPROX_QUANTILES(TOTAL_SESSIONS, 2)[OFFSET(1)] AS mediana_similar,
      COUNT(*) AS n_dias
    FROM ventana_similares
    WHERE rn <= 4
    GROUP BY ALL
    HAVING n_dias = 4
  ),

  stats_mad AS (
    SELECT
      v.TOUCHPOINT_NO_TEAM, v.analysis_level, v.site, v.platform, v.DAY_OF_WEEK,
      -- MAD: Mediana de las desviaciones absolutas respecto a la mediana
      APPROX_QUANTILES(ABS(v.TOTAL_SESSIONS - s.mediana_similar), 2)[OFFSET(1)] AS mad_similar
    FROM ventana_similares v
    INNER JOIN stats_mediana s USING (TOUCHPOINT_NO_TEAM, analysis_level, site, platform, DAY_OF_WEEK)
    WHERE v.rn <= 4
    GROUP BY ALL
  ),

  -- 5. Análisis Final
  final_analysis AS (
    SELECT
      h.TOUCHPOINT_NO_TEAM AS touchpoint,
      h.site,
      h.platform,
      h.analysis_level,
      h.TIMEFRAME_ID AS date,
      h.TOTAL_SESSIONS AS value,
      s.mediana_similar AS expected,
      (h.TOTAL_SESSIONS - s.mediana_similar) AS delta_abs,
      SAFE_DIVIDE(h.TOTAL_SESSIONS - s.mediana_similar, s.mediana_similar) * 100 AS delta_pct,
      SAFE_DIVIDE(h.TOTAL_SESSIONS - s.mediana_similar, NULLIF(m.mad_similar, 0)) AS r_score,
      m.mad_similar
    FROM hoy h
    INNER JOIN stats_mediana s USING (TOUCHPOINT_NO_TEAM, analysis_level, site, platform, DAY_OF_WEEK)
    INNER JOIN stats_mad m USING (TOUCHPOINT_NO_TEAM, analysis_level, site, platform, DAY_OF_WEEK)
  )
FINAL_BASE AS (
  
SELECT
    CONCAT(touchpoint, '|', site, '|', platform, '|PREV_MED_4D') AS alert_key,
    'PREV_MED_4D' AS rule_type,
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
    '4_similar_days_median' AS baseline,
    -- Definición de severidad basada en R-Score (Robust Z-Score)
    CASE 
        WHEN r_score <= -3.5 THEN 'critical'
        WHEN r_score <= -2.5 THEN 'warning'
        WHEN r_score <= -1.8 THEN 'alert'
        ELSE 'ignore'
    END AS severity_raw,
    STRUCT(
        ROUND(r_score, 2) AS r_score,
        ROUND(mad_similar, 2) AS mad,
        '4_days_median' AS method
    ) AS details
FROM final_analysis
WHERE ABS(r_score) >= 1.8
)
SELECT * FROM FINAL_BASE
WHERE severity_raw != 'ignore'
AND ABS(details.r_score) >= 1.8;