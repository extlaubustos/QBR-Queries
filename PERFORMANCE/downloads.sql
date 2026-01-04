-- description: Descargas netas de la app MPlay por sitio, plataforma y fecha
-- domain: growth
-- product: mplay
-- use_case: reporting
-- grain: site, platform, day
-- time_grain: daily
-- date_column: DS
-- date_filter: none
-- threshold_rule: none
-- metrics:
--   - DOWNLOADS: cantidad de instalaciones netas de la aplicación
-- tables_read:
--   - WHOWNER.BT_MKT_MPLAY_INSTALLS
-- joins:
--   - none
-- owner: data_team

SELECT 
SIT_SITE_ID,
DATE_TRUNC(DS, MONTH) AS MONTH_ID,
DATE_TRUNC(DS, WEEK(MONDAY)) AS WEEK_ID,
DS,
PLATFORM,
SUM (NET_APP_INSTALLS) AS DOWNLOADS
FROM meli-bi-data.WHOWNER.BT_MKT_MPLAY_INSTALLS
WHERE SIT_SITE_ID IN ('MLA','MLB','MLM','MLC','MCO','MPE','MLU','MEC')
GROUP BY ALL
ORDER BY DS;