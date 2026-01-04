-- description: Análisis de impacto de campañas de push en consumo de mplay TV, segmentando usuarios según interacción con el push y estado de login
-- domain: marketing
-- product: mplay
-- use_case: campaign_performance
-- grain: site, has_logged, push_segment
-- time_grain: aggregated_period
-- date_column: DS
-- date_filter: parameters (push_sent_date, deanonimized_snp_date)
-- threshold_rule: playback_time >= 20s, device_platform IN (TV devices)
-- metrics:
-- - CUST_ID_DEANONYMIZED: cantidad de usuarios únicos deanonimizados
-- - TVM: minutos totales reproducidos post push
-- - ATV: minutos promedio reproducidos por usuario (TVM / usuarios)
-- - AVG_PPU: promedio de contenidos distintos consumidos por usuario
-- - AVG_FREQUENCY: promedio de días distintos con consumo por usuario
-- dimensions:
-- - PUSH_SEGMENT: nivel máximo de interacción con el push (opened, shown, arrived, sent, control)
-- - HAS_LOGGED: indicador de si el usuario se logueó post deanonimización
-- tables_read:
-- - SBOX_MARKETING.BT_OC_PUSH_CUST_EVENT
-- - SBOX_MARKETING.LK_OC_METADATA
-- - WHOWNER.BT_MKT_MPLAY_PLAYS
-- - WHOWNER.BT_MPLAY_LOGIN_TRANSACTIONS
-- - MPLAY.SNP_BASE_CUST_ID_DEANONYMIZED_TV
-- joins:
-- - PUSH_EVENT.HASH_ID = METADATA.HASH_ID
-- - PUSH_EVENT.SIT_SITE_ID = METADATA.SIT_SITE_ID
-- - DEANONYMIZED.CUST_ID_DEANONYMIZED = PUSH.CUS_CUST_ID
-- - LOGIN.CUS_CUST_ID = PLAYS.USER_ID
-- owner: data_team

DECLARE site_param STRING DEFAULT 'MLM';
DECLARE deanonimized_snp_date DATE DEFAULT '2025-08-04';
DECLARE push_sent_date DATE DEFAULT '2025-08-06';
DECLARE push_id STRING DEFAULT '110853';


WITH PUSH AS
  (
    SELECT 
      A.SIT_SITE_ID
    , CUS_CUST_ID
    , A.COMMUNICATION_ID
    , SENT_DATE
    , COUNT(DISTINCT IF(EVENT_TYPE='control', CUS_CUST_ID, NULL) ) AS HAS_CONTROL
    , COUNT(DISTINCT IF(EVENT_TYPE='test', CUS_CUST_ID, NULL) ) AS HAS_TEST
    , COUNT(DISTINCT IF(EVENT_TYPE='arrived', CUS_CUST_ID, NULL) ) AS HAS_ARRIVED
    , COUNT(DISTINCT IF(EVENT_TYPE='shown', CUS_CUST_ID, NULL) ) AS HAS_SHOWN
    , COUNT(DISTINCT IF(EVENT_TYPE='optout', CUS_CUST_ID, NULL) ) AS HAS_OPTOUT
    , COUNT(DISTINCT IF(EVENT_TYPE='open', CUS_CUST_ID, NULL) ) AS HAS_OPEN

    FROM  `meli-bi-data.SBOX_MARKETING.BT_OC_PUSH_CUST_EVENT` AS A

    INNER JOIN `meli-bi-data.SBOX_MARKETING.LK_OC_METADATA` AS B
    ON A.HASH_ID = B.HASH_ID 
    AND A.SIT_SITE_ID = B.SIT_SITE_ID


    WHERE SENT_DATE = push_sent_date
    AND A.SIT_SITE_ID = site_param
    AND A.COMMUNICATION_ID = push_id

    AND CUS_CUST_ID > 0
    GROUP BY ALL
  )

, PLAYS AS
  (
    SELECT 
        SIT_SITE_ID
      , USER_ID
      , SUM(PLAYBACK_TIME_MILLISECONDS)/1000/60 AS TVM
      , COUNT(DISTINCT CONTENT_ID) AS DISTINCT_CONTENTS
      , COUNT(DISTINCT DS) AS DISTINCT_DAYS

    FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS`

    WHERE SIT_SITE_ID = site_param
    AND DS >= deanonimized_snp_date
    AND DEVICE_PLATFORM IN ('/tv/android','/tv/Tizen','/tv/Web0S')
    AND PLAYBACK_TIME_MILLISECONDS/1000 >= 20 

    GROUP BY ALL
  )


SELECT 
  A.SIT_SITE_ID
, LOGGED.CUS_CUST_ID IS NOT NULL AS HAS_LOGGED
, (CASE WHEN HAS_OPEN = 1 THEN  'has opened'
        WHEN HAS_SHOWN = 1 THEN 'has shown'
        WHEN HAS_ARRIVED = 1 THEN 'has arrived'
        WHEN HAS_TEST = 1 THEN 'has sent'
        WHEN HAS_CONTROL = 1 THEN 'control group' END) AS PUSH_SEGMENT
, COUNT(DISTINCT CUST_ID_DEANONYMIZED) AS CUST_ID_DEANONYMIZED
, SUM(TVM) AS TVM
, SAFE_DIVIDE(SUM(TVM), COUNT(DISTINCT CUST_ID_DEANONYMIZED)) AS ATV
, AVG(DISTINCT_CONTENTS) AS AVG_PPU
, AVG(DISTINCT_DAYS) AS AVG_FREQUENCY

FROM `meli-sbox.MPLAY.SNP_BASE_CUST_ID_DEANONYMIZED_TV` AS A

LEFT JOIN `meli-bi-data.WHOWNER.BT_MPLAY_LOGIN_TRANSACTIONS` AS LOGGED
ON TX_DATE >= deanonimized_snp_date 
AND FLAG_GRANTED_TX = 1 
AND A.SIT_SITE_ID = UPPER(LOGGED.SIT_SITE_ID)
AND LOGGED.CUS_CUST_ID = SAFE_CAST(A.CUST_ID_DEANONYMIZED AS INT64)

LEFT JOIN PLAYS AS P
ON UPPER(LOGGED.SIT_SITE_ID) = P.SIT_SITE_ID
AND LOGGED.CUS_CUST_ID = SAFE_CAST(P.USER_ID AS INT64)

LEFT JOIN PUSH AS B
ON A.SIT_SITE_ID = B.SIT_SITE_ID
AND SAFE_CAST(A.CUST_ID_DEANONYMIZED AS INT64) = B.CUS_CUST_ID


-- LEFT JOIN `meli-sbox.MPLAY.LAST_MPLAY_USER_LIFECYCLE_SNAPSHOT` AS C
-- ON A.CUST_ID_DEANONYMIZED = C.USER_ID
-- AND A.SIT_SITE_ID = C.SIT_SITE_ID

WHERE A.SIT_SITE_ID = site_param
AND SNP_DATE = deanonimized_snp_date

GROUP BY ALL
ORDER BY 1,2,3

