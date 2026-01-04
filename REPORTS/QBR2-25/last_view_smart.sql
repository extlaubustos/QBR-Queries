-- description: Análisis de migración de plataforma: Identifica el primer consumo en Smart TV y rastrea la plataforma previa utilizada por el usuario para entender el flujo de adopción de dispositivos de pantalla grande. 
-- domain: behaviour 
-- product: mplay 
-- use_case: device adoption / platform migration analysis 
-- grain: month_id, platform_prev, flag_log 
-- time_grain: monthly 
-- date_column: FIRST_SMART_MONTH 
-- date_filter: dinámico (5 meses hacia atrás desde 2025-07-01) 
-- threshold_rule: playback_time >= 20s y filtrado por DEVICE_PLATFORM LIKE '%TV%' para el evento de activación. 
-- metrics: 
-- - TOTAL_USERS: Cantidad de usuarios únicos que tuvieron su primera reproducción en Smart TV en el mes analizado. 
-- tables_read: 
-- - meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS 
-- joins: 
-- - Self-join (CTE LAST_VIEW_MONTH_BEFORE_SMART): Cruza el primer evento en Smart TV con la reproducción inmediatamente anterior en el tiempo para determinar el origen del usuario. 
-- owner: data_team
DECLARE mes_inicial DATE DEFAULT DATE '2025-07-01';
DECLARE meses_hacia_atras INT64 DEFAULT 5;

WITH MESES_ANALISIS AS (
  SELECT month_id
  FROM UNNEST(
    GENERATE_DATE_ARRAY(
      DATE_SUB(mes_inicial, INTERVAL meses_hacia_atras - 1 MONTH),
      mes_inicial,
      INTERVAL 1 MONTH
    )
  ) AS month_id
),

FIRST_SMART_VIEW AS (
  SELECT
    SIT_SITE_ID,
    USER_ID,
    MIN_BY(
      STRUCT(
        START_PLAY_TIMESTAMP AS FIRST_SMART_TIMESTAMP,
        DATE_TRUNC(DS, MONTH) AS FIRST_SMART_MONTH
      ),
      START_PLAY_TIMESTAMP
    ) AS FIRST_SMART_STRUCT
  FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS`
  WHERE PLAYBACK_TIME_MILLISECONDS / 1000 >= 20
    AND UPPER(DEVICE_PLATFORM) LIKE '%TV%'
    AND DS <= CURRENT_DATE() - 1
  GROUP BY SIT_SITE_ID, USER_ID
),

FILTERED_FIRST_SMART AS (
  SELECT
    SIT_SITE_ID,
    USER_ID,
    FIRST_SMART_STRUCT.FIRST_SMART_TIMESTAMP,
    FIRST_SMART_STRUCT.FIRST_SMART_MONTH
  FROM FIRST_SMART_VIEW
  JOIN MESES_ANALISIS M ON FIRST_SMART_STRUCT.FIRST_SMART_MONTH = M.month_id
),

LAST_VIEW_MONTH_BEFORE_SMART AS (
  SELECT
    F.SIT_SITE_ID,
    F.USER_ID,
    F.FIRST_SMART_MONTH,
    MAX_BY(
      STRUCT(
        START_PLAY_TIMESTAMP AS LAST_TIMESTAMP,
        UPPER(DEVICE_PLATFORM) AS LAST_PLATFORM
      ),
      START_PLAY_TIMESTAMP
    ) AS LAST_BEFORE_STRUCT
  FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` P
  JOIN FILTERED_FIRST_SMART F
    ON P.USER_ID = F.USER_ID AND P.SIT_SITE_ID = F.SIT_SITE_ID
  WHERE
    PLAYBACK_TIME_MILLISECONDS / 1000 >= 20
    AND P.START_PLAY_TIMESTAMP < F.FIRST_SMART_TIMESTAMP
    AND P.DS <= CURRENT_DATE() - 1
  GROUP BY F.SIT_SITE_ID, F.USER_ID, F.FIRST_SMART_MONTH
),

SUMMARY AS (
  SELECT
    F.FIRST_SMART_MONTH AS MONTH_ID,
    CASE
      WHEN L.LAST_BEFORE_STRUCT.LAST_PLATFORM LIKE '%TV%' THEN 'SMART'
      WHEN L.LAST_BEFORE_STRUCT.LAST_PLATFORM LIKE '%MOBILE%' THEN 'MOBILE'
      WHEN L.LAST_BEFORE_STRUCT.LAST_PLATFORM LIKE '%DESKTOP%' THEN 'DESKTOP'
      WHEN L.LAST_BEFORE_STRUCT.LAST_PLATFORM LIKE '%CAST%' THEN 'CAST'
      WHEN L.LAST_BEFORE_STRUCT.LAST_PLATFORM IS NULL THEN 'NO_PREV_CONSUMPTION'
      ELSE 'UNKNOWN'
    END AS PLATFORM_PREV,
    CASE WHEN SAFE_CAST(USER_ID AS INT64) IS NULL THEN 'NOT_LOG'
       ELSE 'LOG' 
    END AS FLAG_LOG,
    COUNT(DISTINCT F.USER_ID) AS TOTAL_USERS
  FROM FILTERED_FIRST_SMART F
  LEFT JOIN LAST_VIEW_MONTH_BEFORE_SMART L
    ON F.USER_ID = L.USER_ID AND F.SIT_SITE_ID = L.SIT_SITE_ID AND F.FIRST_SMART_MONTH = L.FIRST_SMART_MONTH
  GROUP BY 1, 2, 3
)

SELECT * FROM SUMMARY
ORDER BY MONTH_ID, TOTAL_USERS DESC;

