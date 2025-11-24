-- CTE 1: Plays Calificados (Base)
WITH QualifiedPlays AS (
  SELECT
    P.CUS_CUST_ID,
    P.SIT_SITE_ID,
    P.CONTENT_ID,
    P.CHANNEL_ID,
    P.START_PLAY_TIMESTAMP,
    DATE_TRUNC(P.DS, MONTH) AS view_month
  FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` P
  WHERE P.DS BETWEEN '2024-12-01' AND '2025-10-31'
  AND P.PLAYBACK_TIME_MILLISECONDS >= 20000
),

-- CTE 2: Primer Play de cada usuario en cada mes
-- (Nos dice qué vieron PRIMERO en el mes M)
UserFirstPlayInMonth AS (
  SELECT
    P.CUS_CUST_ID,
    P.view_month,
    P.SIT_SITE_ID,
    P.CONTENT_ID,
    P.CHANNEL_ID
  FROM QualifiedPlays P
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY P.CUS_CUST_ID, P.view_month 
    ORDER BY P.START_PLAY_TIMESTAMP ASC
  ) = 1
),

-- CTE 3: Plays por Título y Mes
-- (Base para el Denominador y para saber qué vieron en M-1)
PlaysByTitleMonth AS (
  SELECT DISTINCT
    view_month,
    CUS_CUST_ID,
    SIT_SITE_ID,
    CONTENT_ID,
    CHANNEL_ID
  FROM QualifiedPlays
)

-- ----
-- CONSULTA FINAL
-- ----
SELECT
  D.view_month,
  C.TITLE_ADJUSTED,
  CASE WHEN C.CONTENT_TYPE = 'MOVIE' THEN 'MOVIE' ELSE 'SERIE' END AS CONTENT_TYPE_AJUSTADO,
  CASE
    WHEN C.CONTENT_PROVIDER = 'SONY' THEN 'SONY'
    WHEN C.CONTENT_PROVIDER IN ('PARAMOUNT', 'CBS','PARAMOUNTTL','PARAMOUNBR') THEN 'PARAMOUNT'
    WHEN C.CONTENT_PROVIDER = 'MPLAYORIGINALS' THEN 'MPLAYORIGINALS'
    WHEN C.CONTENT_PROVIDER IN ('NBCUAVOD', 'UNIVERSALPLUS') OR D.CHANNEL_ID IN ('3062aa4b18ff46ed8d59fe6c3f088bc6','167e27fd5bb84dfabde7103648b9a96b','3f1cc2cccea34b9490b00d6ac514e225','63b08af757040aebdb2d23246fcf71a','9e95f70f53e54e3b86850c2d9a7b1df5','24d2994c2807438eb89c817534ed76db','bb853ee1e9264f018031baab6ea1e3d2') THEN 'NBCU'
    ELSE C.CONTENT_PROVIDER 
  END AS CONTENT_PROVIDER_AJUSTADO,
  
  -- DENOMINADOR: Total de viewers únicos del título en el mes M
  COUNT(DISTINCT D.CUS_CUST_ID) AS total_viewers,
  
  -- NUMERADOR: Viewers que vieron el título en M-1 Y su primer play en M fue ese título
  COUNT(DISTINCT 
    CASE 
      -- Condición 1: Su primer play en el mes M fue este título
      WHEN F.CONTENT_ID = D.CONTENT_ID 
      -- Condición 2: También vio este título en el mes M-1
      AND P_PrevMonth.CONTENT_ID = D.CONTENT_ID
      THEN D.CUS_CUST_ID
      ELSE NULL
    END
  ) AS continuing_retention_viewers,
  
  -- SHARE de Continuación
  SAFE_DIVIDE(
    COUNT(DISTINCT CASE 
        WHEN F.CONTENT_ID = D.CONTENT_ID AND P_PrevMonth.CONTENT_ID = D.CONTENT_ID
        THEN D.CUS_CUST_ID ELSE NULL
      END),
    COUNT(DISTINCT D.CUS_CUST_ID)
  ) AS continuing_retention_share

-- DENOMINADOR: Empezamos con todos los viewers (D) de un título en un mes M
FROM PlaysByTitleMonth D

-- JOIN 1: Traemos su PRIMER PLAY (F) en ese mismo mes M
LEFT JOIN UserFirstPlayInMonth F
  ON D.CUS_CUST_ID = F.CUS_CUST_ID
  AND D.view_month = F.view_month

-- JOIN 2: Traemos sus plays del MES ANTERIOR (P_PrevMonth)
LEFT JOIN PlaysByTitleMonth P_PrevMonth
  ON D.CUS_CUST_ID = P_PrevMonth.CUS_CUST_ID
  AND D.view_month = DATE_ADD(P_PrevMonth.view_month, INTERVAL 1 MONTH)

-- JOIN 3: Traemos los detalles del catálogo
LEFT JOIN `meli-bi-data.WHOWNER.LK_MKT_MPLAY_CATALOGUE` C 
  ON D.SIT_SITE_ID = C.SIT_SITE_ID
  AND D.CONTENT_ID = C.CONTENT_ID
  
WHERE
  D.view_month >= '2025-01-01' -- Mostramos solo meses de retención
  
GROUP BY 1, 2, 3, 4
ORDER BY
  D.view_month,
  total_viewers DESC,
  continuing_retention_share DESC;


  -- Ejemplo, en enero de 2025 tenes X cantidad de viewers cuyo primer play de enero fue el contenido A, de esa X cantidad tenes Y cantidad que el primer contenido visto en M+1 es A de vuelta