-- description: Métricas de conversiones de usuarios por plataforma, sitio, origen y equipo
-- domain: behaviour
-- product: mplay
-- use_case: reporting
-- grain: device_platform, sit_site_id, origin_path, team
-- time_grain: daily
-- date_column: FIRST_DS_USER
-- date_filter: no específico (según rango de la tabla)
-- threshold_rule: none
-- metrics:
-- - CONVERTIONS: suma de conversiones por usuario
-- tables_read:
-- - meli-sbox.MPLAY.MPLAY_NEGOCIO_ATT_AHA_MOMENT_USERS
-- - meli-sbox.MPLAY.LK_MPLAY_SOURCE_TYPE_ORIGIN_SESSION
-- joins:
-- - MPLAY_NEGOCIO_ATT_AHA_MOMENT_USERS join LK_MPLAY_SOURCE_TYPE_ORIGIN_SESSION por origin_path
-- owner: data_team

SELECT
DEVICE_PLATFORM,
SIT_SITE_ID,
COALESCE(OM.SOURCE_SESSION_L2,'NULO') AS ORIGIN_PATH,
FIRST_DS_USER,
FECHA_CUMPLE_AHA,
COALESCE(OM.SOURCE_SESSION_L1,'NULO') AS NEGOCIO,
COALESCE(OM.TEAM,'NULO') AS TEAM,
SUM(CONVERTION_W) AS CONVERTIONS

from meli-sbox.MPLAY.MPLAY_NEGOCIO_ATT_AHA_MOMENT_USERS as A
LEFT JOIN `meli-sbox.MPLAY.LK_MPLAY_SOURCE_TYPE_ORIGIN_SESSION` AS OM   ON OM.SOURCE_TYPE = a.ORIGIN_PATH
GROUP BY ALL
