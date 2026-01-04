-- description: Proporción de usuarios insatisfechos por categoría (variedad, publicidad, facilidad de uso, desempeño, notificaciones) de MPlay por sitio y trimestre, con trimestre calculado a partir de la fecha de cierre de NPS
-- domain: customer_experience
-- product: mplay
-- use_case: reporting
-- grain: site, quarter, user
-- time_grain: quarterly
-- date_column: NPS_REL_RES_END_DATE
-- date_filter: awareness = 'Si' AND SIT_SITE_ID IN ('MLB','MLM','MLA','MLC','MCO')
-- threshold_rule: CSAT_* = 0 indica insatisfacción
-- metrics:
-- - PCT_USERS_VARIETY: % de usuarios insatisfechos con variedad
-- - PCT_USERS_ADVERTISING: % de usuarios insatisfechos con publicidad
-- - PCT_USERS_USABILITY: % de usuarios insatisfechos con facilidad de uso
-- - PCT_USERS_PERFORMANCE: % de usuarios insatisfechos con desempeño
-- - PCT_USERS_RECOMMENDATIONS: % de usuarios insatisfechos con notificaciones
-- tables_read:
-- - WHOWNER.BT_CX_NPS_REL_MPLAY
-- joins:
-- - TOTAL.SIT_SITE_ID = VARIEDAD.SIT_SITE_ID AND TOTAL.QUARTER = VARIEDAD.QUARTER
-- - TOTAL.SIT_SITE_ID = PUBLICIDAD.SIT_SITE_ID AND TOTAL.QUARTER = PUBLICIDAD.QUARTER
-- - TOTAL.SIT_SITE_ID = FACILIDAD.SIT_SITE_ID AND TOTAL.QUARTER = FACILIDAD.QUARTER
-- - TOTAL.SIT_SITE_ID = FUNCIONAMIENTO.SIT_SITE_ID AND TOTAL.QUARTER = FUNCIONAMIENTO.QUARTER
-- - TOTAL.SIT_SITE_ID = NOTIFICACIONES.SIT_SITE_ID AND TOTAL.QUARTER = NOTIFICACIONES.QUARTER
-- owner: data_team

WITH BASE AS (
  
  SELECT 

    NPS_REL_RES_END_DATE, NPS_REL_RES_END_MONTH, 

    PARSE_DATE('%Y-%m-%d', FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(NPS_REL_RES_END_DATE, QUARTER))) AS QUARTER, 
    NPS_REL_CUS_CUST_ID, 
    SIT_SITE_ID, 
    NPS_REL_NPS_SURVEY_ID, 
    NPS_REL_QUALTRICS_RESPONSE_ID, 
    NPS_REL_QUALTRICS_SURVEY_ID, 
    AWARENESS, 
    NPS_REL_NOTA_NPS, 
    NPS_VALUE, 

    case 
    when NPS_REL_MPROM = ' A plataforma é muito completa' then 'La plataforma es muy completa' 
    when NPS_REL_MPROM = 'La plataforma es muy completa' then 'La plataforma es muy completa'
    when NPS_REL_MPROM = 'A variedade do conteúdo é ótima' then 'La variedad de contenido es muy buena' 
    when NPS_REL_MPROM = 'La variedad de contenido es muy buena' then 'La variedad de contenido es muy buena'
    when NPS_REL_MPROM = 'A plataforma é fácil de usar' then 'Es fácil de usar' 
    when NPS_REL_MPROM = 'Es fácil de usar' then 'Es fácil de usar'
    when NPS_REL_MPROM = 'Ótima qualidade de imagem e de conteúdo' then 'La calidad de imagen del contenido es muy bueno' 
    when NPS_REL_MPROM = 'La calidad de imagen del contenido es muy bueno' then 'La calidad de imagen del contenido es muy bueno'
    when NPS_REL_MPROM = 'O fato de ser uma plataforma grátis' then 'Que es gratuito' 
    when NPS_REL_MPROM = 'Que es gratuito' then 'Que es gratuito'
    ELSE 'Otros' 
    end as NPS_REL_MPROM,

    NPS_REL_SUBPROM, 
    NPS_REL_MDET, 
    NPS_REL_SUBDET, 
    NPS_REL_MNEUTRO, 
    NPS_REL_SUBNEUTRO, 
    NPS_REL_MPROM_GROUP, 

    CASE 
    WHEN CSAT_VARIETY = '5. Muy de Acuerdo' THEN 1
    WHEN CSAT_VARIETY = '5.Concordo totalmente' THEN 1
    WHEN CSAT_VARIETY = '4' THEN 1
    ELSE 0 
    END AS CSAT_VARIETY, 

    CASE 
    WHEN CSAT_ADVERTISING = '5. Muy de Acuerdo' THEN 1
    WHEN CSAT_ADVERTISING = '5.Concordo totalmente' THEN 1
    WHEN CSAT_ADVERTISING = '4' THEN 1
    ELSE 0 
    END AS CSAT_ADVERTISING, 

    CASE 
    WHEN CSAT_USABILITY = '5. Muy de Acuerdo' THEN 1
    WHEN CSAT_USABILITY = '5.Concordo totalmente' THEN 1
    WHEN CSAT_USABILITY = '4' THEN 1
    ELSE 0 
    END AS CSAT_USABILITY, 

    CASE 
    WHEN CSAT_PERFORMANCE = '5. Muy de Acuerdo' THEN 1
    WHEN CSAT_PERFORMANCE = '5.Concordo totalmente' THEN 1
    WHEN CSAT_PERFORMANCE = '4' THEN 1
    ELSE 0 
    END AS CSAT_PERFORMANCE, 

    CASE 
    WHEN CSAT_RECOMMENDATIONS = '5. Muy de Acuerdo' THEN 1
    WHEN CSAT_RECOMMENDATIONS = '5.Concordo totalmente' THEN 1
    WHEN CSAT_RECOMMENDATIONS = '4' THEN 1
    ELSE 0 
    END AS CSAT_RECOMMENDATIONS, 

    SAT_VARIETY, 
    INSAT_VARIETY, 
    INSAT_VARIETY_DETAIL, 
    INSAT_VARIETY_TYPE, 
    INSAT_ADVERTISING, 
    INSAT_ADVERTISING_DETAIL, 
    INSAT_USABILITY, 
    INSAT_USABILITY_DETAIL, 
    INSAT_PERFORMANCE, 
    INSAT_PERFORMANCE_DETAIL, 
    INSAT_RECOMMENDATIONS, 
    INSAT_RECOMMENDATIONS_DETAIL, 
    ADDITIONAL_RESPONSES, 
    PLATFORMS, 
    CASE 
    WHEN MAIN_PLATFORM = 'Si' THEN 'Si'
    WHEN MAIN_PLATFORM = 'Sim' THEN 'Si'
    ELSE 'No'
    END AS MAIN_PLATFORM, 
    BENEFITS_OTHER_PLATFORMS, 
    NPS_REL_COMMENT 

  FROM `meli-bi-data.WHOWNER.BT_CX_NPS_REL_MPLAY`

    where AWARENESS IN ('Si')
    AND SIT_SITE_ID IN ('MLB','MLM','MLA','MLC','MCO')
),
VARIEDAD AS (
  SELECT 
    SIT_SITE_ID,
    QUARTER,
    INSAT_VARIETY,
    COUNT(DISTINCT NPS_REL_CUS_CUST_ID) AS TOTAL_USERS
  FROM BASE
  WHERE CSAT_VARIETY = 0
  GROUP BY ALL
),
PUBLICIDAD AS (
  SELECT 
    SIT_SITE_ID,
    QUARTER,
    INSAT_ADVERTISING,
    COUNT(DISTINCT NPS_REL_CUS_CUST_ID) AS TOTAL_USERS
  FROM BASE
  WHERE CSAT_ADVERTISING = 0
  GROUP BY ALL
),
FACILIDAD AS (
  SELECT 
    SIT_SITE_ID,
    QUARTER,
    INSAT_USABILITY,
    COUNT(DISTINCT NPS_REL_CUS_CUST_ID) AS TOTAL_USERS
  FROM BASE
  WHERE CSAT_USABILITY = 0
  GROUP BY ALL
),
FUNCIONAMIENTO AS (
  SELECT 
    SIT_SITE_ID,
    QUARTER,
    INSAT_PERFORMANCE,
    COUNT(DISTINCT NPS_REL_CUS_CUST_ID) AS TOTAL_USERS
  FROM BASE
  WHERE CSAT_PERFORMANCE = 0
  GROUP BY ALL
),
NOTIFICACIONES AS (
  SELECT 
    SIT_SITE_ID,
    QUARTER,
    INSAT_RECOMMENDATIONS,
    COUNT(DISTINCT NPS_REL_CUS_CUST_ID) AS TOTAL_USERS
  FROM BASE
  WHERE CSAT_RECOMMENDATIONS = 0
  GROUP BY ALL
),
TOTAL AS (
  SELECT
    SIT_SITE_ID,
    QUARTER,
    COUNT(DISTINCT NPS_REL_CUS_CUST_ID) AS TOTAL_USERS
  FROM BASE
  GROUP BY ALL
)
SELECT
  T.SIT_SITE_ID,
  T.QUARTER,
  V.INSAT_VARIETY,
  ROUND(SAFE_DIVIDE(V.TOTAL_USERS, T.TOTAL_USERS)*100,4) AS PCT_USERS_VARIETY,
  A.INSAT_ADVERTISING,
  ROUND(SAFE_DIVIDE(A.TOTAL_USERS, T.TOTAL_USERS)*100,4) AS PCT_USERS_ADVERTISING,
  F.INSAT_USABILITY,
  ROUND(SAFE_DIVIDE(F.TOTAL_USERS, T.TOTAL_USERS)*100,4) AS PCT_USERS_USABILITY,
  FU.INSAT_PERFORMANCE,
  ROUND(SAFE_DIVIDE(FU.TOTAL_USERS, T.TOTAL_USERS)*100,4) AS PCT_USERS_PERFORMANCE,
  N.INSAT_RECOMMENDATIONS,
  ROUND(SAFE_DIVIDE(N.TOTAL_USERS, T.TOTAL_USERS)*100,4) AS PCT_USERS_RECOMMENDATIONS
FROM TOTAL T
LEFT JOIN VARIEDAD V
  ON T.SIT_SITE_ID = V.SIT_SITE_ID
 AND T.QUARTER = V.QUARTER
LEFT JOIN PUBLICIDAD A
  ON T.SIT_SITE_ID = A.SIT_SITE_ID
 AND T.QUARTER = A.QUARTER
LEFT JOIN FACILIDAD F
  ON T.SIT_SITE_ID = F.SIT_SITE_ID
  AND T.QUARTER = F.QUARTER
LEFT JOIN FUNCIONAMIENTO FU
  ON T.SIT_SITE_ID = FU.SIT_SITE_ID
  AND T.QUARTER = FU.QUARTER
LEFT JOIN NOTIFICACIONES N
  ON T.SIT_SITE_ID = N.SIT_SITE_ID  
  AND T.QUARTER = N.QUARTER