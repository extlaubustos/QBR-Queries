-- description: Análisis de usuarios impactados por campañas de push segmentados por estado New / Retained / Recovered según consumo de mplay
-- domain: marketing
-- product: mplay
-- use_case: campaign_analysis
-- grain: campaign_name, push_type, strategy, n_r_flag
-- time_grain: monthly
-- date_column: DS
-- date_filter: up_to (hasta current_date - 1)
-- threshold_rule: playback_time >= 20s
-- metrics:
-- - CANTIDAD_USUARIOS_EN_CAMPANA: usuarios únicos impactados por campaña, segmentados por estado New / Retained / Recovered
-- dimensions:
-- - FLAG_N_R: clasificación del usuario según recurrencia de consumo (NEW, RETAINED, RECOVERED)
-- - CAMPAIGN_NAME: nombre de la campaña de push
-- - PUSH_TYPE: tipo de push enviado
-- - STRATEGY: estrategia de la campaña
-- tables_read:
-- - SBOX_MARKETING.BT_OC_PUSH_CUST_EVENT
-- - SBOX_MARKETING.LK_OC_METADATA
-- - WHOWNER.BT_MKT_MPLAY_PLAYS
-- joins:
-- - PUSH_EVENT.HASH_ID = METADATA.HASH_ID
-- - PUSH_EVENT.SIT_SITE_ID = METADATA.SIT_SITE_ID
-- - PUSH_EVENT.CUS_CUST_ID = PLAYS.CUS_CUST_ID
-- owner: data_team

WITH PUSH_METADATA AS (
  SELECT
      A.CUS_CUST_ID,
      A.EVENT_TYPE,
      B.CAMPAIGN_NAME,
      B.PUSH_TYPE,
      B.STRATEGY
  FROM
      `meli-bi-data.SBOX_MARKETING.BT_OC_PUSH_CUST_EVENT` AS A
  JOIN
      `meli-bi-data.SBOX_MARKETING.LK_OC_METADATA` AS B
  ON
      A.HASH_ID = B.HASH_ID
      AND A.SIT_SITE_ID = B.SIT_SITE_ID
  WHERE
      A.EVENT_TYPE IN ('open', 'test', 'control', 'shown')
),

NEW_RET_RECO AS (
  SELECT
      *,
      DATE_TRUNC(DS,MONTH) AS TIME_FRAME_ID
    , LAG(DS, 1) OVER(PARTITION BY SIT_SITE_ID, USER_ID ORDER BY START_PLAY_TIMESTAMP ASC) AS DS_ANT
    , CASE 
        WHEN (LAG(DS, 1) OVER(PARTITION BY SIT_SITE_ID, USER_ID ORDER BY START_PLAY_TIMESTAMP ASC)) IS NULL THEN 'NEW'
        WHEN DATE_DIFF(DS, (LAG(DS, 1) OVER(PARTITION BY SIT_SITE_ID, USER_ID ORDER BY START_PLAY_TIMESTAMP ASC)), DAY) <= 30 THEN 'RETAINED'
        WHEN DATE_DIFF(DS, (LAG(DS, 1) OVER(PARTITION BY SIT_SITE_ID, USER_ID ORDER BY START_PLAY_TIMESTAMP ASC)), DAY) > 30 THEN 'RECOVERED'
        ELSE NULL 
      END AS FLAG_N_R
  FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS`
  WHERE
      PLAYBACK_TIME_MILLISECONDS/1000 >= 20
      AND DS <= CURRENT_DATE() - 1
),

PLAYS_SEGMENTED AS (
  SELECT
      SIT_SITE_ID,
      USER_ID,
      CUS_CUST_ID,
      TIME_FRAME_ID,
      FLAG_N_R
  FROM NEW_RET_RECO
  QUALIFY 
    ROW_NUMBER() OVER(PARTITION BY SIT_SITE_ID, USER_ID, TIME_FRAME_ID ORDER BY START_PLAY_TIMESTAMP ASC) = 1
)

SELECT
    P.CAMPAIGN_NAME,
    P.PUSH_TYPE,
    P.STRATEGY,
    PS.FLAG_N_R,
    COUNT(DISTINCT P.CUS_CUST_ID) AS cantidad_usuarios_en_campana
FROM PUSH_METADATA AS P
LEFT JOIN PLAYS_SEGMENTED AS PS
ON P.CUS_CUST_ID = PS.CUS_CUST_ID
GROUP BY
    P.CAMPAIGN_NAME,
    P.PUSH_TYPE,
    P.STRATEGY,
    PS.FLAG_N_R
ORDER BY
    P.CAMPAIGN_NAME,
    PS.FLAG_N_R;