-- description: Métricas de viewers y consumo (TVM) en plataformas Smart TV por sitio y semana
-- domain: behaviour
-- product: mplay
-- use_case: reporting
-- grain: site, week, platform
-- time_grain: weekly
-- date_column: DS
-- date_filter: none
-- threshold_rule: playback_time >= 20s
-- metrics:
--   - VIEWERS_SMART: usuarios únicos con consumo en dispositivos Smart TV
--   - TVM_SMART: minutos totales reproducidos en plataformas Smart TV con threshold 20s
-- tables_read:
--   - WHOWNER.BT_MKT_MPLAY_PLAYS
-- joins:
--   - PLAYS.USER_ID = FIRST_PLAY.USER_ID
--   - PLAYS.SIT_SITE_ID = FIRST_PLAY.SIT_SITE_ID
-- owner: data_team

SELECT DISTINCT
P.SIT_SITE_ID,
DATE_TRUNC(DS, WEEK(MONDAY)) as WEEK_ID,
LOWER(DEVICE_PLATFORM) PLATFORM,
COUNT(DISTINCT P.USER_ID) AS VIEWERS_SMART,
SUM(PLAYBACK_TIME_MILLISECONDS/60000) AS TVM_SMART
FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` P
LEFT JOIN  (
      select
      USER_ID,
      SIT_SITE_ID,
      MIN(DS) AS FIRST_DATE
      FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` AS P
      WHERE P.PLAYBACK_TIME_MILLISECONDS/1000 >= 20
      GROUP BY ALL
) AS FP ON FP.USER_ID = P.USER_ID AND FP.SIT_SITE_ID = P.SIT_SITE_ID
WHERE LOWER(DEVICE_PLATFORM) LIKE '/tv%'
AND PLAYBACK_TIME_MILLISECONDS/1000 >=20
GROUP BY ALL
ORDER BY WEEK_ID;