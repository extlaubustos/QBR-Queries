-- description: Métricas de retención de usuarios por título, género, tipo y proveedor de contenido a nivel mensual
-- domain: behaviour
-- product: mplay
-- use_case: analysis
-- grain: month, title, genres, content_type, content_provider
-- time_grain: monthly
-- date_column: P.DS
-- date_filter: last_3_complete_months
-- threshold_rule: playback_time >= 20s
-- metrics:
--   - TOTAL_USUARIOS_RETENIDOS_CONTENT: usuarios únicos retenidos que consumieron el título en el mes
--   - TOTAL_VIEWERS_TITULO: usuarios únicos que consumieron el título en el mes
--   - RATIO_RETENCION: proporción de usuarios retenidos sobre el total de viewers del título
-- tables_read:
--   - WHOWNER.BT_MKT_MPLAY_PLAYS
--   - WHOWNER.LK_MKT_MPLAY_CATALOGUE
--   - WHOWNER.DM_MKT_MPLAY_RAW_PLAYS
-- joins:
--   - PLAYS.SIT_SITE_ID = CATALOGUE.SIT_SITE_ID
--   - PLAYS.CONTENT_ID = CATALOGUE.CONTENT_ID
--   - PLAYS.USER_ID = RAW_PLAYS.USER_ID
-- owner: data_team

WITH CATALOGO_LIMPIO AS (
    SELECT 
        CONTENT_ID,
        SIT_SITE_ID,
        TITLE_ADJUSTED,
        CONTENT_TYPE,
        CONTENT_PROVIDER,
        UPPER(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    NORMALIZE(GENRE, NFD), r'\pM', '' 
                ), 
                r'[\[\]"]', '' 
            )
        ) as genre_clean
    FROM `meli-bi-data.WHOWNER.LK_MKT_MPLAY_CATALOGUE`
),
CATALOGO_AGREGADO AS (
    SELECT 
        CONTENT_ID,
        SIT_SITE_ID,
        TITLE_ADJUSTED,
        CONTENT_TYPE,
        CONTENT_PROVIDER,
        STRING_AGG(DISTINCT genre_clean, ', ' ORDER BY genre_clean) AS GENRES
    FROM CATALOGO_LIMPIO
    GROUP BY ALL
),
BASE_DATA AS (
    SELECT
        DATE_TRUNC(P.DS, MONTH) AS MONTH_ID,
        C.TITLE_ADJUSTED,
        C.GENRES,
        CASE 
            WHEN C.CONTENT_TYPE = 'MOVIE' THEN 'MOVIE' 
            ELSE 'SERIES' 
        END AS CONTENT_TYPE_ADJUSTED,
        CASE
            WHEN C.CONTENT_PROVIDER = 'SONY' THEN 'SONY'
            WHEN C.CONTENT_PROVIDER IN ('PARAMOUNT', 'CBS','PARAMOUNTTL','PARAMOUNBR') THEN 'PARAMOUNT'
            WHEN C.CONTENT_PROVIDER = 'MPLAYORIGINALS' THEN 'MPLAYORIGINALS'
            WHEN C.CONTENT_PROVIDER IN ('NBCUAVOD', 'UNIVERSALPLUS') 
                 OR P.CHANNEL_ID IN ('3062aa4b18ff46ed8d59fe6c3f088bc6','167e27fd5bb84dfabde7103648b9a96b','3f1cc2cccea34b9490b00d6ac514e225','63b08af757040aebdb2d23246fcf71a','9e95f70f53e54e3b86850c2d9a7b1df5','24d2994c2807438eb89c817534ed76db','bb853ee1e9264f018031baab6ea1e3d2') THEN 'NBCU'
            ELSE C.CONTENT_PROVIDER 
        END AS CONTENT_PROVIDER_AJUSTADO,
        P.USER_ID,
        R.LIFE_CYCLE
    FROM meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS AS P
    LEFT JOIN CATALOGO_AGREGADO AS C
        ON P.SIT_SITE_ID = C.SIT_SITE_ID
        AND P.CONTENT_ID = C.CONTENT_ID
    LEFT JOIN meli-bi-data.WHOWNER.DM_MKT_MPLAY_RAW_PLAYS AS R
        ON R.USER_ID = P.USER_ID 
        AND DATE_TRUNC(P.DS, MONTH) = DATE_TRUNC(R.TIM_DAY, MONTH)
        AND R.TIME_FRAME = 'MONTHLY'
    WHERE P.PLAYBACK_TIME_MILLISECONDS/1000 >= 20
      AND P.DS >= DATE_TRUNC(DATE_SUB(CURRENT_DATE, INTERVAL 3 MONTH), MONTH)
        AND P.DS < DATE_TRUNC(CURRENT_DATE, MONTH)
)
SELECT
    MONTH_ID,
    TITLE_ADJUSTED,
    GENRES,
    CONTENT_TYPE_ADJUSTED,
    CONTENT_PROVIDER_AJUSTADO,
    COUNT(DISTINCT CASE WHEN LIFE_CYCLE = 'RETAINED' THEN USER_ID END) AS TOTAL_USUARIOS_RETENIDOS_CONTENT,
    COUNT(DISTINCT USER_ID) AS TOTAL_VIEWERS_TITULO,
    SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN LIFE_CYCLE = 'RETAINED' THEN USER_ID END), COUNT(DISTINCT USER_ID)) AS RATIO_RETENCION
FROM BASE_DATA
GROUP BY 1, 2, 3, 4, 5
HAVING TOTAL_VIEWERS_TITULO >= 1000
ORDER BY MONTH_ID DESC, RATIO_RETENCION DESC;