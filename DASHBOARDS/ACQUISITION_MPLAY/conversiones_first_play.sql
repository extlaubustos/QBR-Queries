-- description: Métricas de conversiones y retención de nuevos usuarios por plataforma, sitio, origen y equipo
-- domain: behaviour
-- product: mplay
-- use_case: reporting
-- grain: device_platform, sit_site_id, origin_path, team
-- time_grain: daily
-- date_column: FECHA_CONV
-- date_filter: no específico (según rango de la tabla)
-- threshold_rule: none
-- metrics:
-- - CONVERTIONS: suma de conversiones por usuario
-- - RET_1_30: retención 1-30 días ponderada por conversiones
-- - RET_31_60: retención 31-60 días ponderada por conversiones
-- - AHA_MOMENT: indicador de AHA moment ponderado por conversiones
-- tables_read:
-- - meli-sbox.MPLAY.MPLAY_NEGOCIO_ATT_NEW_USERS
-- - meli-sbox.MPLAY.LK_MPLAY_SOURCE_TYPE_ORIGIN_SESSION
-- joins:
-- - MPLAY_NEGOCIO_ATT_NEW_USERS join LK_MPLAY_SOURCE_TYPE_ORIGIN_SESSION por origin_path
-- owner: data_team

SELECT
DEVICE_PLATFORM,
SIT_SITE_ID,
COALESCE(OM.SOURCE_SESSION_L2,'NULO') AS ORIGIN_PATH,
FECHA_CONV,
COALESCE(OM.SOURCE_SESSION_L1,'NULO') AS NEGOCIO,
COALESCE(OM.TEAM,'NULO') AS TEAM,
SUM(CONVERTION_W) AS CONVERTIONS,
SUM(COALESCE(A.RET_1_30,0)*CONVERTION_W) AS RET_1_30,
SUM(COALESCE(A.RET_31_60,0)*CONVERTION_W) AS RET_31_60,
SUM(COALESCE(A.AHA_MOMENT,0)*CONVERTION_W) AS AHA_MOMENT
FROM `meli-sbox.MPLAY.MPLAY_NEGOCIO_ATT_NEW_USERS` as A
LEFT JOIN `meli-sbox.MPLAY.LK_MPLAY_SOURCE_TYPE_ORIGIN_SESSION` AS OM   ON OM.SOURCE_TYPE = a.ORIGIN_PATH
GROUP BY ALL
