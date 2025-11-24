-- CTE 1: Plays Calificados (CON TÍTULO)
-- Obtenemos todos los plays calificados de Sept y Oct, y unimos con el catálogo.
WITH QualifiedPlaysWithTitle AS (
  SELECT
    P.CUS_CUST_ID,
    P.SIT_SITE_ID,
    P.CONTENT_ID,
    P.START_PLAY_TIMESTAMP,
    DATE_TRUNC(P.DS, MONTH) AS view_month,
    C.TITLE_ADJUSTED
  FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` P
  LEFT JOIN `meli-bi-data.WHOWNER.LK_MKT_MPLAY_CATALOGUE` C
    ON P.SIT_SITE_ID = C.SIT_SITE_ID
    AND P.CONTENT_ID = C.CONTENT_ID
  WHERE P.DS BETWEEN '2025-09-01' AND '2025-10-31'
  AND P.PLAYBACK_TIME_MILLISECONDS >= 20000
),

-- CTE 2: Usuarios Target
-- Identificamos a los usuarios que:
-- 1. Tuvieron actividad en Septiembre 2025 (vieron algo)
-- 2. Vieron 'Deadly Honeymoon' en Octubre 2025 (no importa si fue lo primero o no)
TargetUsers AS (
  SELECT
    CUS_CUST_ID
  FROM QualifiedPlaysWithTitle
  GROUP BY 1
  HAVING
    -- Condición 1: Vio algo en Septiembre
    COUNT(DISTINCT IF(view_month = '2025-09-01', view_month, NULL)) > 0
    AND
    -- Condición 2: Vio 'Deadly Honeymoon' en Octubre
    COUNT(DISTINCT IF(view_month = '2025-10-01' AND TITLE_ADJUSTED = 'Deadly Honeymoon', view_month, NULL)) > 0
)

-- ----
-- CONSULTA FINAL (Validación con ranking)
-- Traemos TODO el historial de Sept/Oct para esos usuarios
-- y rankeamos sus plays dentro de cada mes.
-- ----
SELECT
  P.CUS_CUST_ID,
  P.view_month,
  P.START_PLAY_TIMESTAMP,
  P.TITLE_ADJUSTED,
  -- ### CAMBIO AQUÍ ###
  -- Añadimos un ranking de plays por usuario y mes
  ROW_NUMBER() OVER(
    PARTITION BY P.CUS_CUST_ID, P.view_month
    ORDER BY P.START_PLAY_TIMESTAMP ASC
  ) AS play_rank_in_month
FROM QualifiedPlaysWithTitle P
-- Filtramos solo para los usuarios que cumplen ambas condiciones
INNER JOIN TargetUsers T
  ON P.CUS_CUST_ID = T.CUS_CUST_ID
ORDER BY
  P.CUS_CUST_ID,
  P.START_PLAY_TIMESTAMP;