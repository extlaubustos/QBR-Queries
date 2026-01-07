-- description: Descargas de la app MPlay por sitio y plataforma, con agregaciones diarias, semanales y mensuales
-- domain: acquisition
-- product: mplay
-- use_case: reporting
-- grain: site, platform, day
-- time_grain: day / week / month
-- date_column: DS
-- date_filter: none
-- metrics:
--   - DOWNLOADS: total de instalaciones netas de la app
-- tables_read:
--   - WHOWNER.BT_MKT_MPLAY_INSTALLS
-- joins:
--   - N/A
-- owner: data_team

SELECT 
  SIT_SITE_ID,
  DATE_TRUNC(DS, MONTH) AS MONTH_ID,
  DATE_TRUNC(DS, WEEK(MONDAY)) AS WEEK_ID,
  DS,
  PLATFORM,
  CONCAT(SIT_SITE_ID, '-', PLATFORM) AS SITE_PLATFORM, 
  SUM(NET_APP_INSTALLS) AS DOWNLOADS,
FROM meli-bi-data.WHOWNER.BT_MKT_MPLAY_INSTALLS
WHERE SIT_SITE_ID IN ('MLA','MLB','MLM','MLC','MCO','MPE','MLU','MEC')
GROUP BY ALL
ORDER BY MONTH_ID DESC, SIT_SITE_ID ASC, PLATFORM ASC