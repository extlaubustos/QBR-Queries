-- description: Métricas de consumo y completitud de contenido por título y temporada, incluyendo viewers, reproducciones, TVM y completion rate
-- domain: behaviour
-- product: mplay
-- use_case: reporting
-- grain: title, season_number
-- time_grain: aggregated_period
-- date_column: P.DS
-- date_filter: rolling_window (últimos ~5 años acotados a últimos ~6 meses)
-- threshold_rule: playback_time >= 20s
-- metrics:
-- - VIEWERS: usuarios únicos con playback >= 20s
-- - CANTIDAD_CONTENIDOS: cantidad de contenidos distintos consumidos
-- - REPRODUCCIONES: combinaciones únicas de contenido y usuario con playback >= 20s
-- - TVM: minutos reproducidos con threshold 20s
-- - COMPLETION_RATE: TVM / (VIEWERS * RUNTIME)
-- tables_read:
-- - WHOWNER.BT_MKT_MPLAY_PLAYS
-- - WHOWNER.LK_MKT_MPLAY_CATALOGUE
-- joins:
-- - PLAYS.CONTENT_ID = CATALOGUE.CONTENT_ID
-- - PLAYS.SIT_SITE_ID = CATALOGUE.SIT_SITE_

WITH BASE AS (
    SELECT
    C.SEASON_NUMBER AS SEASON_NUMBER,
    C.TITLE_ADJUSTED AS TITLE_ADJUSTED,
    C.RUNTIME AS RUNTIME,
    COUNT(DISTINCT CASE WHEN(P.PLAYBACK_TIME_MILLISECONDS >= 20000) THEN P.USER_ID ELSE NULL END) AS VIEWERS,
    COUNT(DISTINCT P.CONTENT_ID) AS CANTIDAD_CONTENIDOS,
    COUNT(DISTINCT CONCAT( P.CONTENT_ID  , ' ',  P.USER_ID)) AS REPRODUCCIONES,
    SAFE_DIVIDE(SUM(IF((P.PLAYBACK_TIME_MILLISECONDS ) >= 20000, P.PLAYBACK_TIME_MILLISECONDS , NULL)), 60000) AS TVM
FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` AS P
LEFT JOIN `meli-bi-data.WHOWNER.LK_MKT_MPLAY_CATALOGUE` AS C 
    ON P.SIT_SITE_ID = C.SIT_SITE_ID
    AND  P.CONTENT_ID  = C.CONTENT_ID
WHERE (P.PLAYBACK_TIME_MILLISECONDS ) >= 20000 
AND (((P.DS) >= ((DATE_ADD(DATE_TRUNC(CURRENT_DATE('America/Buenos_Aires'), YEAR), INTERVAL -4 YEAR))) 
AND (P.DS) < ((DATE_ADD(DATE_ADD(DATE_TRUNC(CURRENT_DATE('America/Buenos_Aires'), YEAR), INTERVAL -4 YEAR), INTERVAL 5 YEAR))))) 
AND (((P.DS) >= ((DATE_ADD(DATE_TRUNC(CURRENT_DATE('America/Buenos_Aires'), MONTH), INTERVAL -5 MONTH))) 
AND (P.DS) < ((DATE_ADD(DATE_ADD(DATE_TRUNC(CURRENT_DATE('America/Buenos_Aires'), MONTH), INTERVAL -5 MONTH), INTERVAL 6 MONTH))))) 
AND (C..SEASON_NUMBER ) >= 1
GROUP BY 1, 2
LIMIT 30000
)
SELECT 
*,
SAFE_DIVIDE(TVM, VIEWERS * RUNTIME) AS COMPLETION_RATE
FROM BASE