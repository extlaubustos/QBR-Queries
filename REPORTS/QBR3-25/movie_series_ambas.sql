-- description: Segmentación de la audiencia según preferencia de contenido (Películas vs. Series). Clasifica a los usuarios en categorías excluyentes (Movies Only, Series Only, Both) para analizar patrones de consumo y volumen de TVMs por perfil. 
-- domain: behaviour 
-- product: mplay 
-- use_case: audience profiling / content preference analysis 
-- grain: month_id, viewer_type 
-- time_grain: monthly 
-- date_column: DS 
-- date_filter: >= '2025-01-01' 
-- threshold_rule: playback_time >= 20s 
-- metrics: 
-- - VIEWERS: Cantidad de usuarios únicos clasificados en cada perfil de preferencia. 
-- - TOTAL_TVMs: Minutos totales reproducidos por cada segmento de usuarios. 
-- tables_read: 
-- - WHOWNER.BT_MKT_MPLAY_PLAYS 
-- - WHOWNER.LK_MKT_MPLAY_CATALOGUE 
-- joins: 
-- - PLAYS (PL) LEFT JOIN CATALOGUE (C): Para mapear el CONTENT_TYPE de cada reproducción y derivar la categoría de contenido. 
-- owner: data_team
-- Paso 1: Obtener todas las reproducciones válidas y enriquecerlas con la categoría de contenido (Movie, Serie, Other).
-- Una reproducción es válida si dura al menos 20 segundos (20,000 milisegundos).
WITH PLAYS_WITH_CATEGORY AS (
  SELECT 
      DATE_TRUNC(PL.DS, MONTH) AS MONTH_ID,
      PL.USER_ID,
      PL.PLAYBACK_TIME_MILLISECONDS,
      -- Clasificamos cada contenido en 'MOVIE', 'SERIE' u 'OTHER' para simplificar.
      CASE 
        WHEN C.CONTENT_TYPE = 'MOVIE' THEN 'MOVIE'
        WHEN C.CONTENT_TYPE IN ('SHOW', 'EPISODE') THEN 'SERIE'
        ELSE 'OTHER' 
      END AS CONTENT_CATEGORY
  FROM `WHOWNER.BT_MKT_MPLAY_PLAYS` AS PL
  -- Unimos con el catálogo para obtener el tipo de contenido de lo que se reprodujo.
  LEFT JOIN `WHOWNER.LK_MKT_MPLAY_CATALOGUE` AS C 
      ON PL.CONTENT_ID = C.CONTENT_ID AND PL.SIT_SITE_ID = C.SIT_SITE_ID
  WHERE 
      PL.DS >= '2025-01-01'
      AND PL.PLAYBACK_TIME_MILLISECONDS >= 20000 -- Filtro de tiempo mínimo de reproducción
),

-- Paso 2: Analizar el comportamiento de cada usuario por mes.
-- Para cada usuario y mes, determinamos si vio películas, si vio series y calculamos su tiempo total de visionado.
USER_MONTHLY_BEHAVIOR AS (
  SELECT
    MONTH_ID,
    USER_ID,
    -- Sumamos el tiempo total de visionado por usuario al mes y lo convertimos a minutos.
    SAFE_DIVIDE(SUM(PLAYBACK_TIME_MILLISECONDS), 60000) AS TOTAL_USER_TVMs,
    -- Creamos "flags" (banderas booleanas) para saber qué tipo de contenido vio el usuario.
    -- LOGICAL_OR es una forma eficiente de verificar si al menos una fila en el grupo cumple la condición.
    LOGICAL_OR(CONTENT_CATEGORY = 'MOVIE') AS WATCHED_MOVIE,
    LOGICAL_OR(CONTENT_CATEGORY = 'SERIE') AS WATCHED_SERIE,
    LOGICAL_OR(CONTENT_CATEGORY = 'OTHER') AS WATCHED_OTHER
  FROM PLAYS_WITH_CATEGORY
  GROUP BY 
    MONTH_ID, 
    USER_ID
),

-- Paso 3: Asignar a cada usuario su categoría final (MOVIES_ONLY, SERIES_ONLY, BOTH, OTHERS).
USER_FINAL_CATEGORY AS (
  SELECT
    MONTH_ID,
    USER_ID,
    TOTAL_USER_TVMs,
    -- Usamos los flags del paso anterior para clasificar al usuario.
    CASE
      WHEN WATCHED_MOVIE AND WATCHED_SERIE THEN 'BOTH'
      WHEN WATCHED_MOVIE AND NOT WATCHED_SERIE THEN 'MOVIES_ONLY'
      WHEN NOT WATCHED_MOVIE AND WATCHED_SERIE THEN 'SERIES_ONLY'
      -- Si no vio ni películas ni series, pero sí vio algo, lo clasificamos como 'OTHERS'.
      WHEN WATCHED_OTHER THEN 'OTHERS'
      -- Este ELSE es por si un usuario no tiene ninguna categoría (poco probable con la lógica actual).
      ELSE 'UNKNOWN' 
    END AS VIEWER_TYPE
  FROM USER_MONTHLY_BEHAVIOR
)

-- Paso 4: Agregar los resultados finales.
-- Contamos cuántos usuarios únicos hay en cada categoría y sumamos su tiempo de visionado.
SELECT
  MONTH_ID,
  VIEWER_TYPE,
  COUNT(DISTINCT USER_ID) AS VIEWERS,
  SUM(TOTAL_USER_TVMs) AS TOTAL_TVMs
FROM USER_FINAL_CATEGORY
WHERE VIEWER_TYPE != 'UNKNOWN' -- Excluimos casos no clasificados
GROUP BY
  MONTH_ID,
  VIEWER_TYPE
ORDER BY
  MONTH_ID,
  VIEWER_TYPE;