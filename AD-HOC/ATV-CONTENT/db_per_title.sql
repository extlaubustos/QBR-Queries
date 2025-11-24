-- CTE 1: Agregamos los milisegundos calificados POR PLAY
-- Usamos GROUP BY en la tabla de Plays para "colapsar" los plays por usuario, contenido y día.
-- ESTE ES EL CAMBIO MÁS IMPORTANTE.
WITH QualifiedPlaysAgg AS (
  SELECT
    P.CUS_CUST_ID,
    P.SIT_SITE_ID,
    P.CONTENT_ID,
    P.CHANNEL_ID,
    DATE_TRUNC(P.DS, MONTH) AS view_month,
    -- Sumamos solo los milisegundos calificados (>= 20s)
    SUM(IF(P.PLAYBACK_TIME_MILLISECONDS >= 20000, P.PLAYBACK_TIME_MILLISECONDS, 0)) AS qualified_milliseconds
  FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` P
  WHERE P.DS BETWEEN '2024-12-01' AND '2025-10-31'
  GROUP BY 1, 2, 3, 4, 5
),

-- CTE 2: Ahora sí, unimos con el catálogo
-- Como ya sumamos, si hay duplicados en C, no importa, no inflará el TVM.
BaseMonthlyViews AS (
  SELECT
    P.CUS_CUST_ID,
    P.view_month,
    C.TITLE_ADJUSTED,
    C.CONTENT_TYPE,
    CASE WHEN C.CONTENT_TYPE = 'MOVIE' THEN 'MOVIE' ELSE 'SERIE' END AS CONTENT_TYPE_AJUSTADO,
    C.CONTENT_PROVIDER,
    CASE
      WHEN C.CONTENT_PROVIDER = 'SONY' THEN 'SONY'
      WHEN C.CONTENT_PROVIDER IN ('PARAMOUNT', 'CBS','PARAMOUNTTL','PARAMOUNBR') THEN 'PARAMOUNT'
      WHEN C.CONTENT_PROVIDER = 'MPLAYORIGINALS' THEN 'MPLAYORIGINALS'
      WHEN C.CONTENT_PROVIDER IN ('NBCUAVOD', 'UNIVERSALPLUS') OR P.CHANNEL_ID IN ('3062aa4b18ff46ed8d59fe6c3f088bc6','167e27fd5bb84dfabde7103648b9a96b','3f1cc2cccea34b9490b00d6ac514e225','63b08af757040aebdb2d23246fcf71a','9e95f70f53e54e3b86850c2d9a7b1df5','24d2994c2807438eb89c817534ed76db','bb853ee1e9264f018031baab6ea1e3d2') THEN 'NBCU'
      ELSE C.CONTENT_PROVIDER 
    END AS CONTENT_PROVIDER_AJUSTADO,
    -- Sumamos los milisegundos (ya pre-agregados) y los convertimos a minutos
    SAFE_DIVIDE(SUM(P.qualified_milliseconds), 60000) AS TVM
  FROM QualifiedPlaysAgg P
  LEFT JOIN `meli-bi-data.WHOWNER.LK_MKT_MPLAY_CATALOGUE` C 
    ON P.SIT_SITE_ID = C.SIT_SITE_ID
    AND P.CONTENT_ID = C.CONTENT_ID
  -- Filtramos aquí cualquier fila que no tenga TVM calificado (ahorra cómputo)
  WHERE P.qualified_milliseconds > 0
  GROUP BY 1, 2, 3, 4, 5, 6, 7
),

-- CTE 3: Identificar la presencia mensual (ahora lee de la base correcta)
UserMonthlyPresence AS (
  SELECT DISTINCT
    CUS_CUST_ID,
    view_month
  FROM BaseMonthlyViews -- Ya solo contiene usuarios con vistas calificadas
),

-- CTE 4: Identificar usuarios retenidos (esta lógica no cambia)
RetainedUsers AS (
  SELECT
    T1.CUS_CUST_ID,
    T2.view_month AS retention_month
  FROM UserMonthlyPresence T1
  JOIN UserMonthlyPresence T2
    ON T1.CUS_CUST_ID = T2.CUS_CUST_ID
    AND T2.view_month = DATE_ADD(T1.view_month, INTERVAL 1 MONTH)
)

-- ----
-- CONSULTA FINAL
-- ----
SELECT
  R.retention_month, 
  B.TITLE_ADJUSTED,
  B.CONTENT_TYPE_AJUSTADO,
  B.CONTENT_PROVIDER_AJUSTADO,
  COUNT(DISTINCT B.CUS_CUST_ID) AS retained_users_who_watched_title,
  SUM(B.TVM) AS total_tvm_from_retained_users
FROM BaseMonthlyViews B
INNER JOIN RetainedUsers R
  ON B.CUS_CUST_ID = R.CUS_CUST_ID
  AND B.view_month = R.retention_month
GROUP BY 1, 2, 3, 4
ORDER BY
  retained_users_who_watched_title DESC,
  total_tvm_from_retained_users DESC;


-- En el mes Q el contenido A tuvo X cantidad de viewers retenidos