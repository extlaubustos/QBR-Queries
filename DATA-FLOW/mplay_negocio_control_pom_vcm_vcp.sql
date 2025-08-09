-- Crea o reemplaza una tabla temporal en el sANDbox de MPLAY llamada MPLAY_NEGOCIO_CONTROL_POM_VCM_VCP_STAGE
CREATE OR REPLACE TABLE `meli-sbox.MPLAY.MPLAY_NEGOCIO_CONTROL_POM_VCM_VCP_STAGE` AS (
-- Con esta CTE vamos a traer todo el trafico que provenga de MATT siempre y cuando sea de MERCADO PLAY
WITH trafico_matt AS  ( 
  SELECT 
    SITE AS SIT_SITE_ID,
    DS AS DS,
    usr.melidata_session_id AS SESSION_ID,
    --COALESCE(IF (SAFE_CAST(usr.user_id AS INT64) IS NOT NULL,USR.USER_ID,NULL),USR.UID) AS USER_ID,
    (SAFE_CAST(user_timestamp AS TIMESTAMP)) AS USER_TIMESTAMP,
    -- Con JSON_VALUE extraemos de event_data la clave go dentro del JSON
    JSON_VALUE(event_data, '$.go')  AS go,
    device.platform AS device_platform,
    application.version,
    CAST(MATT_TOOL AS INT64) AS MATT_TOOL,
    CAST(MATT_ID AS INT64) AS MATT_ID,
    -- El JSON_VALUE extrae el valor de la clave original_go dentro de event_data. De lo obtenido toma todos los carecteres hasta encontrar un & o el final
    REGEXP_EXTRACT(JSON_VALUE(event_data, '$.original_go'), r"origin=([^&]*)") AS ORIGIN,
    device.connectivity_type AS CONNECTIVITY_TYPE,
    device.device_name AS DEVICE_NAME,
    device.os_version AS OS_VERSION,
    device.device_manufacturer AS DEVICE_MANUFACTURER
  FROM `meli-bi-data.MELIDATA.TRACKS` AS A
  -- Nos conectamos para traer solo los touchpoints de MPLAY
  INNER JOIN `meli-bi-data.WHOWNER.BT_MATT_METADATA`  AS B
  -- Primero extrae el valor de la clave tool de event_data. Con REGEXP_REPLACE reemplaza \? por '' hasta el final de la cadena. Por último, con REGEXP_SUBSTR extrae aquellos valores que coincidan con la expresion regular [0-9]
    ON REGEXP_SUBSTR(REGEXP_REPLACE(JSON_EXTRACT_SCALAR(A.event_data, '$.tool'), r'\?.*$', ''),"[0-9]+") = CAST(B.MATT_TOOL AS STRING)
    AND MATT_BUSINESS_UNIT = 'MERCADO PLAY'
    AND B.TIM_DAY = CURRENT_DATE-1
  WHERE ds >= CURRENT_DATE-7
    -- Este path levanta eventos disparados por matt tool ids
    AND path = '/traffic/inbound/matt'
    AND usr.melidata_session_id IS NOT NULL
),
-- Nos quedamos con una fila por session_id y sit_site_id
TRAFICO_MATT_1 AS (
  SELECT *
  FROM TRAFICO_MATT
  QUALIFY ROW_NUMBER()OVER(PARTITION BY SESSION_ID, /*USER_ID, */ SIT_SITE_ID ORDER BY USER_TIMESTAMP ASC ) = 1
),
-- Vamos a verificar por cada sesion si hay interacción con VCP o VCM
VCP_VCM  AS (
  SELECT 
    SITE AS SIT_SITE_ID,
    usr.melidata_session_id AS SESSION_ID,
    -- COALESCE(IF (SAFE_CAST(usr.user_id AS INT64) IS NOT NULL,USR.USER_ID,NULL),USR.UID) AS USER_ID,
    MAX(1) AS FLAG_MPLAY,
    -- Flagueamos por VCP y VCM
    MAX(CASE WHEN PATH LIKE '%/vcp' THEN 1 ELSE 0 END ) AS FLAG_VCP,
    MAX(CASE WHEN PATH LIKE '%/vcm' THEN 1 ELSE 0 END ) AS FLAG_VCM,
    MAX(CASE WHEN PATH LIKE '%/vcp/player' THEN 1 ELSE 0 END ) AS FLAG_VCP_PLAYER,
    -- Los regexp_contains a continuacion se utilizan para ir flagueando segun corresponda
    MAX(CASE WHEN (regexp_contains(path,'/mercadoplay/')) AND (regexp_contains(path,'playback')) THEN 1 ELSE 0 END ) AS FLAG_PLAY,
    MAX(CASE WHEN regexp_contains(path,'/mercadoplay/') AND regexp_contains(path,'playback') AND regexp_contains(path,'/ad') is false THEN 1 ELSE 0 END ) AS FLAG_PLAYBACK_NOT_ADS,    
    MAX(CASE WHEN (PATH =  ('/mercadoplay/player/playback') AND JSON_VALUE(EVENT_DATA, "$.type") = 'continuous') THEN 1 ELSE 0 END ) AS FLAG_PLAY_20
  FROM `meli-bi-data.MELIDATA.TRACKS` AS A
  -- Pedimos que en el path se especifique que es de Mercado Play
  WHERE regexp_contains(path,'/mercadoplay/')
    AND ds >= CURRENT_DATE-7
  GROUP BY ALL
)
-- Unificamos el trafico MATT con las FLAGs que creamos en el CTE VCP_VCM uniendo por SITE_ID y SESSION_ID
SELECT 
  A.*,
  MAX(CASE WHEN B.FLAG_MPLAY = 1 THEN 1 ELSE 0 END) AS FLAG_MPLAY,
  MAX(CASE WHEN B.FLAG_VCP = 1 THEN 1 ELSE 0 END) AS FLAG_VCP,
  MAX(CASE WHEN B.FLAG_VCM = 1 THEN 1 ELSE 0 END) AS FLAG_VCM,
  MAX(CASE WHEN B.FLAG_VCP_PLAYER=  1 THEN 1 ELSE 0 END) AS FLAG_VCP_PLAYER,
  MAX(CASE WHEN B.FLAG_PLAYBACK_NOT_ADS=  1 THEN 1 ELSE 0 END) AS FLAG_PLAYBACK_NOT_ADS,
  MAX(CASE WHEN B.FLAG_PLAY = 1 THEN 1 ELSE 0 END) AS FLAG_PLAY,
  MAX(CASE WHEN B.FLAG_PLAY_20 = 1 THEN 1 ELSE 0 END) AS FLAG_PLAY_20,
FROM TRAFICO_MATT_1 AS A
LEFT JOIN VCP_VCM AS B 
  ON B.SIT_SITE_ID = A.SIT_SITE_ID
  AND B.SESSION_ID  = A.SESSION_ID
  -- AND B.USER_ID = A.USER_ID
GROUP BY ALL );
-- Insertamos a MPLAY_NEGOCIO_CONTROL_POM_VCM_VCP los valores definidos en la tabla STAGE (recordar que todo el proceso anterior se encontraba dentro del create table de MPLAY_NEGOCIO_CONTROL_POM_VCM_VCP_STAGE). Se insertan exclusivamente los datos QUE NO EXISTEN
INSERT INTO `meli-sbox.MPLAY.MPLAY_NEGOCIO_CONTROL_POM_VCM_VCP`
  SELECT 
    S.*
  FROM `meli-sbox.MPLAY.MPLAY_NEGOCIO_CONTROL_POM_VCM_VCP_STAGE` AS S
  LEFT JOIN `meli-sbox.MPLAY.MPLAY_NEGOCIO_CONTROL_POM_VCM_VCP` AS A 
    ON S.SESSION_ID =  A.SESSION_ID
    -- AND S.USER_ID = A.USER_ID
    AND S.SIT_SITE_ID = A.SIT_SITE_ID
WHERE A.SESSION_ID IS NULL --> INSERTO LOS QUE NO EXISTEN
;
-- Se crea la tabla que alimentara el dashboard de FUNNELS MPLAY
CREATE OR REPLACE TABLE  `meli-sbox.MPLAY.MPLAY_NEGOCIO_CONTROL_POM_VCM_VCP_DASHBOARD` AS (
  SELECT 
    A.SIT_SITE_ID,
    DS,
    --CASE WHEN SAFE_CAST(USER_ID AS INT64) IS NOT NULL THEN 'LOG'ELSE 'NOT LOG' END AS FLAG_LOG,
    EXTRACT( HOUR FROM DATETIME(USER_TIMESTAMP)) AS HOUR_MELIDATA,
    DEVICE_PLATFORM,
    VERSION,
    CONNECTIVITY_TYPE,
    OS_VERSION,
    B.MATT_IMPLEMENTATION,
    DEVICE_MANUFACTURER,
    -- Se flaguea por si vio contenido o no
    CASE WHEN (LOWER(go) LIKE '%/assistir%' OR LOWER(go) LIKE '%/ver%') THEN 'CAMP_CONTENT' ELSE 'CAMP_HUB' END AS FLAG_TOOL,
    -- Se flaguea analizando si llego a ver contenido teniendo en cuenta el valor en GO
    CASE WHEN REGEXP_EXTRACT(GO, r'([a-f0-9]{32})')  IS NULL THEN 'not_content' ELSE 'content' END AS flag_Content,
    -- Se flague por POM o no
    CASE WHEN LOWER(go) LIKE '%pom-%' THEN 'pom-' ELSE 'n_a' END AS flag_pom,
    -- Se generan las metricas sumando los valores de la tabla MPLAY_NEGOCIO_CONTROL_POM_VCM_VCP
    SUM(1) AS TRAFICO,
    SUM(FLAG_MPLAY) AS MPLAY,
    SUM(FLAG_VCP) AS VCP,
    SUM(FLAG_VCM) AS VCM,
    SUM(CASE WHEN FLAG_VCP = 1 OR FLAG_VCM = 1 THEN 1 ELSE 0 END) AS CONVERTION_VCP_VCM,
    SUM(FLAG_PLAYBACK_NOT_ADS) AS PLAYER_NOT_AD,
    SUM(FLAG_VCP_PLAYER) AS VCP_PLAYER,
    SUM(FLAG_PLAY) AS PLAYER,
    SUM(FLAG_PLAY_20) AS PLAY_20S,
  FROM `meli-sbox.MPLAY.MPLAY_NEGOCIO_CONTROL_POM_VCM_VCP` AS A
  -- Se hace un join con BT_MATT_METADATA para poder traer los valores de MATT_IMPLEMENTATION
  LEFT JOIN `meli-bi-data.WHOWNER.BT_MATT_METADATA`  AS B 
    ON B.MATT_TOOL = A.MATT_TOOL
    AND MATT_BUSINESS_UNIT = 'MERCADO PLAY'
    AND B.TIM_DAY = CURRENT_DATE-1
  GROUP BY ALL
)
