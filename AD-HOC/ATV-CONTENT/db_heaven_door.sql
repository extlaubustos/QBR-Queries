-- CTE 1: Plays Calificados (SIN AGREGAR)
WITH QualifiedPlays AS (
  SELECT
    P.CUS_CUST_ID, -- <-- CORREGIDO
    P.SIT_SITE_ID,
    P.CONTENT_ID,
    P.CHANNEL_ID,
    P.START_PLAY_TIMESTAMP,
    DATE_TRUNC(P.DS, MONTH) AS view_month,
    P.PLAYBACK_TIME_MILLISECONDS AS qualified_milliseconds
  FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` P
  WHERE P.DS BETWEEN '2024-12-01' AND '2025-10-31'
  AND P.PLAYBACK_TIME_MILLISECONDS >= 20000
),

-- CTE 2: Agregamos TVM por mes y contenido
BaseMonthlyViews AS (
  SELECT
    P.CUS_CUST_ID, -- <-- CORREGIDO
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
    SAFE_DIVIDE(SUM(P.qualified_milliseconds), 60000) AS TVM
  FROM QualifiedPlays P
  LEFT JOIN `meli-bi-data.WHOWNER.LK_MKT_MPLAY_CATALOGUE` C 
    ON P.SIT_SITE_ID = C.SIT_SITE_ID
    AND P.CONTENT_ID = C.CONTENT_ID
  GROUP BY 1, 2, 3, 4, 5, 6, 7
),

-- CTE 3: Identificar la presencia mensual
UserMonthlyPresence AS (
  SELECT DISTINCT
    CUS_CUST_ID, -- <-- CORREGIDO
    view_month
  FROM BaseMonthlyViews
),

-- CTE 4: Identificar usuarios retenidos
RetainedUsers AS (
  SELECT
    T1.CUS_CUST_ID, -- <-- CORREGIDO
    T2.view_month AS retention_month
  FROM UserMonthlyPresence T1
  JOIN UserMonthlyPresence T2
    ON T1.CUS_CUST_ID = T2.CUS_CUST_ID -- <-- CORREGIDO
    AND T2.view_month = DATE_ADD(T1.view_month, INTERVAL 1 MONTH)
),

-- ---
-- CTE 5: Identificar el PRIMER TÍTULO VISTO en el mes
-- ---
UserFirstPlayInMonth AS (
  SELECT
    P.CUS_CUST_ID, -- <-- CORREGIDO
    P.view_month,
    P.SIT_SITE_ID,
    P.CONTENT_ID,
    P.CHANNEL_ID
  FROM QualifiedPlays P
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY P.CUS_CUST_ID, P.view_month -- <-- CORREGIDO
    ORDER BY P.START_PLAY_TIMESTAMP ASC
  ) = 1
)

-- ----
-- CONSULTA FINAL
-- ----
SELECT
  R.retention_month, 
  C.TITLE_ADJUSTED,
  CASE WHEN C.CONTENT_TYPE = 'MOVIE' THEN 'MOVIE' ELSE 'SERIE' END AS CONTENT_TYPE_AJUSTADO,
  CASE
    WHEN C.CONTENT_PROVIDER = 'SONY' THEN 'SONY'
    WHEN C.CONTENT_PROVIDER IN ('PARAMOUNT', 'CBS','PARAMOUNTTL','PARAMOUNBR') THEN 'PARAMOUNT'
    WHEN C.CONTENT_PROVIDER = 'MPLAYORIGINALS' THEN 'MPLAYORIGINALS'
    WHEN C.CONTENT_PROVIDER IN ('NBCUAVOD', 'UNIVERSALPLUS') OR F.CHANNEL_ID IN ('3062aa4b18ff46ed8d59fe6c3f088bc6','167e27fd5bb84dfabde7103648b9a96b','3f1cc2cccea34b9490b00d6ac514e225','63b08af757040aebdb2d23246fcf71a','9e95f70f53e54e3b86850c2d9a7b1df5','24d2994c2807438eb89c817534ed76db','bb853ee1e9264f018031baab6ea1e3d2') THEN 'NBCU'
    ELSE C.CONTENT_PROVIDER 
  END AS CONTENT_PROVIDER_AJUSTADO,
  COUNT(DISTINCT R.CUS_CUST_ID) AS retained_users_with_this_first_title -- <-- CORREGIDO
FROM RetainedUsers R
-- Unimos con el primer play del mes de retención
JOIN UserFirstPlayInMonth F
  ON R.CUS_CUST_ID = F.CUS_CUST_ID -- <-- CORREGIDO
  AND R.retention_month = F.view_month
-- Unimos con el catálogo para obtener los detalles del título
LEFT JOIN `meli-bi-data.WHOWNER.LK_MKT_MPLAY_CATALOGUE` C 
  ON F.SIT_SITE_ID = C.SIT_SITE_ID
  AND F.CONTENT_ID = C.CONTENT_ID
GROUP BY 1, 2, 3, 4
ORDER BY
  retained_users_with_this_first_title DESC;

-- En el mes M tenes X cantidad de usuarios retenidos que vieron en el mes M+1 por primera vez este titulo