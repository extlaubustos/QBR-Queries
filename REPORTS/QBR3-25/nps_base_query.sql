SELECT 

NPS_REL_RES_END_DATE, NPS_REL_RES_END_MONTH, 

FORMAT_DATE('%Y-Q%Q',NPS_REL_RES_END_QUARTER) AS QUARTER, 
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