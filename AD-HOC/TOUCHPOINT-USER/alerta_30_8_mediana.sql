-- 1. Base de datos completa con dimensiones adicionales
WITH TIMEFRAME_IDS_BASE AS (
  SELECT
    TOUCHPOINT_NO_TEAM,
    SIT_SITE_ID, -- Dimensión agregada
    PLATFORM,    -- Dimensión agregada
    TIMEFRAME_ID,
    EXTRACT(DAYOFWEEK FROM TIMEFRAME_ID) AS DAY_OF_WEEK,
    ROUND(SUM(Sessions), 2) AS TOTAL_SESSIONS
  FROM `meli-sbox.MPLAY.MPLAY_TOUCHPOINT_USER`
  WHERE TIMEFRAME_TYPE = 'DAILY'
    AND LOWER(TOUCHPOINT_NO_TEAM) NOT LIKE '%mplay-hub%'
  GROUP BY ALL
),

---------------------------------------------------
-- 2. Análisis de Pareto (80% de las sesiones)
---------------------------------------------------

TouchpointData AS (
    SELECT
        TIMEFRAME_ID,
        TOUCHPOINT_NO_TEAM,
        SIT_SITE_ID,
        PLATFORM,
        TOTAL_SESSIONS AS SessionsByTouchpoint,
        SUM(TOTAL_SESSIONS) OVER (PARTITION BY TIMEFRAME_ID) AS TotalDailySessions
    FROM TIMEFRAME_IDS_BASE
),

ParetoSessions AS (
    SELECT
        *,
        (SessionsByTouchpoint * 1.0 / TotalDailySessions) AS SessionShare,
        SUM(SessionsByTouchpoint * 1.0 / TotalDailySessions) OVER (
            PARTITION BY TIMEFRAME_ID
            ORDER BY SessionsByTouchpoint DESC
            ROWS UNBOUNDED PRECEDING
        ) AS CumulativeSessionShare
    FROM TouchpointData
),

ParetoTouchpoints AS (
    SELECT DISTINCT
        TIMEFRAME_ID,
        TOUCHPOINT_NO_TEAM,
        SIT_SITE_ID,
        PLATFORM
    FROM ParetoSessions
    WHERE CumulativeSessionShare <= 0.80
),

-- 3. Base Filtrada por Pareto
DSS_BASE_FILTRADA AS (
    SELECT T1.*
    FROM TIMEFRAME_IDS_BASE T1
    INNER JOIN ParetoTouchpoints T2
        ON T1.TIMEFRAME_ID = T2.TIMEFRAME_ID
        AND T1.TOUCHPOINT_NO_TEAM = T2.TOUCHPOINT_NO_TEAM
        AND T1.SIT_SITE_ID = T2.SIT_SITE_ID
        AND T1.PLATFORM = T2.PLATFORM
),

---------------------------------------------------
-- 4. Clasificación y Cálculo de Mediana/MAD
---------------------------------------------------

ETIQUETADAS AS (
  SELECT
    *,
    CASE WHEN DAY_OF_WEEK IN (1,7) THEN 'finde' ELSE 'habil' END AS TIPO_DIA
  FROM DSS_BASE_FILTRADA
),

-- Separación de muestras (30 hábiles / 8 findes)
ultimos_habiles AS (
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY touchpoint_no_team, SIT_SITE_ID, PLATFORM ORDER BY TIMEFRAME_ID DESC) AS rn
    FROM ETIQUETADAS WHERE tipo_dia = 'habil' AND TIMEFRAME_ID < CURRENT_DATE()
  ) WHERE rn <= 30
),
ultimos_findes AS (
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY touchpoint_no_team, SIT_SITE_ID, PLATFORM ORDER BY TIMEFRAME_ID DESC) AS rn
    FROM ETIQUETADAS WHERE tipo_dia = 'finde' AND TIMEFRAME_ID < CURRENT_DATE()
  ) WHERE rn <= 8
),

-- Estadísticas Robustas Hábiles
median_temp_hab AS (
  SELECT
    touchpoint_no_team, SIT_SITE_ID, PLATFORM,
    ARRAY_AGG(total_sessions ORDER BY total_sessions) AS arr_sessions
  FROM ultimos_habiles GROUP BY 1, 2, 3
),
stats_habiles AS (
  SELECT
    *,ARRAY_LENGTH(arr_sessions)  AS n_habiles,
    IF(MOD(ARRAY_LENGTH(arr_sessions),2)=0,
       0.5*(arr_sessions[OFFSET(DIV(ARRAY_LENGTH(arr_sessions),2)-1)] + arr_sessions[OFFSET(DIV(ARRAY_LENGTH(arr_sessions),2))]),
       arr_sessions[OFFSET(DIV(ARRAY_LENGTH(arr_sessions),2))]) AS mediana_hab
  FROM median_temp_hab
  where ARRAY_LENGTH(arr_sessions) = 30
),
mad_final_hab AS (
  SELECT
    touchpoint_no_team, SIT_SITE_ID, PLATFORM,
    IF(MOD(ARRAY_LENGTH(arr_mad),2)=0,
       0.5*(arr_mad[OFFSET(DIV(ARRAY_LENGTH(arr_mad),2)-1)] + arr_mad[OFFSET(DIV(ARRAY_LENGTH(arr_mad),2))]),
       arr_mad[OFFSET(DIV(ARRAY_LENGTH(arr_mad),2))]) AS mad_hab
  FROM (
    SELECT touchpoint_no_team, SIT_SITE_ID, PLATFORM, ARRAY_AGG(ABS(u.total_sessions - s.mediana_hab) ORDER BY ABS(u.total_sessions - s.mediana_hab)) AS arr_mad
    FROM ultimos_habiles u JOIN stats_habiles s USING (touchpoint_no_team, SIT_SITE_ID, PLATFORM)
    GROUP BY 1, 2, 3
  )
),

-- Estadísticas Robustas Findes
median_temp_findes AS (
  SELECT
    touchpoint_no_team, SIT_SITE_ID, PLATFORM,
    ARRAY_AGG(total_sessions ORDER BY total_sessions) AS arr_sessions
  FROM ultimos_findes GROUP BY 1, 2, 3
),
stats_findes AS (
  SELECT
    *, ARRAY_LENGTH(arr_sessions) AS n_findes,
    IF(MOD(ARRAY_LENGTH(arr_sessions),2)=0,
       0.5*(arr_sessions[OFFSET(DIV(ARRAY_LENGTH(arr_sessions),2)-1)] + arr_sessions[OFFSET(DIV(ARRAY_LENGTH(arr_sessions),2))]),
       arr_sessions[OFFSET(DIV(ARRAY_LENGTH(arr_sessions),2))]) AS mediana_findes
  FROM median_temp_findes where ARRAY_LENGTH(arr_sessions) = 8
),
mad_final_findes AS (
  SELECT
    touchpoint_no_team, SIT_SITE_ID, PLATFORM,
    IF(MOD(ARRAY_LENGTH(arr_mad),2)=0,
       0.5*(arr_mad[OFFSET(DIV(ARRAY_LENGTH(arr_mad),2)-1)] + arr_mad[OFFSET(DIV(ARRAY_LENGTH(arr_mad),2))]),
       arr_mad[OFFSET(DIV(ARRAY_LENGTH(arr_mad),2))]) AS mad_findes
  FROM (
    SELECT touchpoint_no_team, SIT_SITE_ID, PLATFORM, ARRAY_AGG(ABS(u.total_sessions - s.mediana_findes) ORDER BY ABS(u.total_sessions - s.mediana_findes)) AS arr_mad
    FROM ultimos_findes u JOIN stats_findes s USING (touchpoint_no_team, SIT_SITE_ID, PLATFORM)
    GROUP BY 1, 2, 3
  )
),

---------------------------------------------------
-- 5. Día de Análisis y Join Final
---------------------------------------------------

hoy_tag AS (
  SELECT
    touchpoint_no_team, SIT_SITE_ID, PLATFORM, TIMEFRAME_ID, total_sessions,
    CASE WHEN EXTRACT(DAYOFWEEK FROM TIMEFRAME_ID) IN (1,7) THEN 'finde' ELSE 'habil' END AS tipo_dia
  FROM DSS_BASE_FILTRADA WHERE TIMEFRAME_ID = CURRENT_DATE()-1
),

join_stats AS (
  SELECT
    h.*, sh.mediana_hab, mfh.mad_hab, sf.mediana_findes, mff.mad_findes
  FROM hoy_tag h
  LEFT JOIN stats_habiles sh USING (touchpoint_no_team, SIT_SITE_ID, PLATFORM)
  LEFT JOIN mad_final_hab mfh USING (touchpoint_no_team, SIT_SITE_ID, PLATFORM)
  LEFT JOIN stats_findes sf USING (touchpoint_no_team, SIT_SITE_ID, PLATFORM)
  LEFT JOIN mad_final_findes mff USING (touchpoint_no_team, SIT_SITE_ID, PLATFORM)
),

final AS (
  SELECT
    touchpoint_no_team, SIT_SITE_ID, PLATFORM, TIMEFRAME_ID AS fecha, tipo_dia, total_sessions,
    CASE WHEN tipo_dia = 'habil' THEN ROUND(SAFE_DIVIDE(total_sessions - mediana_hab, mad_hab), 2) ELSE ROUND(SAFE_DIVIDE(total_sessions - mediana_findes, mad_findes), 2) END AS r_score,
    CASE WHEN tipo_dia = 'habil' THEN mad_hab ELSE mad_findes END AS mad_usada,
    CASE WHEN tipo_dia = 'habil' THEN mediana_hab ELSE mediana_findes END AS mediana_usada,
    ROUND(CASE WHEN tipo_dia = 'habil' THEN SAFE_DIVIDE(total_sessions - mediana_hab, mediana_hab) * 100 ELSE SAFE_DIVIDE(total_sessions - mediana_findes, mediana_findes) * 100 END, 2) AS variacion_pct
  FROM join_stats
)

SELECT *, 'MED_30_8_SITE_PLATFORM' AS analysis FROM final
WHERE ABS(r_score) >= 2 AND mad_usada > 0 AND touchpoint_no_team NOT LIKE '%Otros%'
ORDER BY ABS(r_score) DESC;