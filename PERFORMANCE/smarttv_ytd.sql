-- description: Métricas agregadas de viewers y consumo (TVM) en plataformas Smart TV
-- domain: behaviour
-- product: mplay
-- use_case: reporting
-- grain: aggregated
-- time_grain: aggregated_period
-- date_column: DS
-- date_filter: optional
-- threshold_rule: playback_time >= 20s
-- metrics:
--   - VIEWERS: usuarios únicos con consumo en dispositivos Smart TV
--   - TVM: minutos totales reproducidos en Smart TV con threshold 20s
-- tables_read:
--   - WHOWNER.BT_MKT_MPLAY_PLAYS
-- joins:
--   - PLAYS.USER_ID = FIRST_PLAY.USER_ID
--   - PLAYS.SIT_SITE_ID = FIRST_PLAY.SIT_SITE_ID
-- owner: data_team

SELECT DISTINCT
COUNT(DISTINCT P.USER_ID) AS VIEWERS,
SUM(PLAYBACK_TIME_MILLISECONDS/60000) AS TVM
FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` AS P
LEFT JOIN  (
      SELECT
      USER_ID,
      SIT_SITE_ID,
      MIN(DS) AS FIRST_DATE
      FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` AS P
      WHERE p.PLAYBACK_TIME_MILLISECONDS/1000 >= 20
      GROUP BY ALL
) AS FP ON FP.USER_ID =  P.USER_ID AND FP.SIT_SITE_ID = P.SIT_SITE_ID
WHERE LOWER(DEVICE_PLATFORM) LIKE '/tv%'
AND PLAYBACK_TIME_MILLISECONDS/1000 >=20
--AND DS <= '2025-03-31'
GROUP BY ALL;