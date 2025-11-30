with USER_MONTHS AS (
        SELECT 
        SN.SIT_SITE_ID,
        SN.USER_ID,z
--        SN.LAST_SESSION_PLAY_CONTENT_ID,
        UM.TYPE_USER,
        UM.SEGMENT_LIFE_CYCLE,
  --      UM.BUYER_CATEGORY,
        case when date_diff(current_date,UM.LAST_PLAY_DATE,month) <= 6 then date_diff(current_date,UM.LAST_PLAY_DATE,month) else 7 end as AGING_MONTHS,  
-- date_diff(current_date(),UM.FIRST_PLAY_DATE,month) as edad_en_play,
date_diff(current_date(),case when UM.FIRST_PLAY_DATE <= date'2024-08-01' 
                              then date'2024-08-01' 
                              else UM.FIRST_PLAY_DATE end
                         ,month) as total_meses_posible_Actividad,
        COUNT(DISTINCT CASE WHEN (SN.SEGMENT_LIFE_CYCLE LIKE '%RETENT%' OR SN.SEGMENT_LIFE_CYCLE LIKE '%EARLY%') 
                             AND  SN.TYPE_USER LIKE 'HIGH%' 
                             THEN SN.SNAPSHOT_DATE ELSE NULL END) AS TOTAL_HIGH,
        COUNT(DISTINCT CASE WHEN (SN.SEGMENT_LIFE_CYCLE LIKE '%RETENT%' OR SN.SEGMENT_LIFE_CYCLE LIKE '%EARLY%') 
                             AND  SN.TYPE_USER LIKE 'MED%' 
                             THEN SN.SNAPSHOT_DATE ELSE NULL END) AS TOTAL_MED,
        COUNT(DISTINCT CASE WHEN (SN.SEGMENT_LIFE_CYCLE LIKE '%RETENT%' OR SN.SEGMENT_LIFE_CYCLE LIKE '%EARLY%') 
                             AND  SN.TYPE_USER LIKE 'LOW%' 
                             THEN SN.SNAPSHOT_DATE ELSE NULL END) AS TOTAL_LOW                                                          
        FROM `meli-sbox.MPLAY.MPLAY_USER_LIFECYCLE_SNAPSHOT` AS SN
            INNER JOIN  `meli-sbox.MPLAY.LAST_MPLAY_USER_LIFECYCLE_SNAPSHOT` as UM  ON UM.SIT_SITE_ID = sn.SIT_SITE_ID
                                                                                   AND UM.USER_ID = sn.USER_ID
                                                                                   AND SN.SNAPSHOT_DATE >= UM.FIRST_PLAY_DATE
                                                                                   AND UM.FIRST_PLAY_DATE <= CURRENT_DATE()

        WHERE SN.SNAPSHOT_DATE <= CURRENT_DATE()
        AND SN.SIT_SITE_ID IN ('MLB', 'MLM')
        AND DATE_DIFF(CURRENT_DATE, UM.LAST_PLAY_DATE, MONTH) <= 3
        GROUP BY ALL
),
FINAL_RESULTS AS (
    SELECT 
SIT_SITE_ID,
USER_ID,
TYPE_USER,
SEGMENT_LIFE_CYCLE,
AGING_MONTHS,
-- CONTENT_ID,
CASE WHEN SCORE_ENG >= 0.35 THEN 'A_HIGH_ENG'
     WHEN SCORE_ENG >= 0.20 AND SCORE_ENG < 0.35 THEN 'B_MEDIUM_ENG'
     ELSE 'C_LOW_ENG' END AS CALIDAD_ABSOLUTA,
--COUNT(DISTINCT USER_ID) AS TOTAL_USERS,
FROM 
      (SELECT 
      A.*,
      safe_divide((TOTAL_HIGH*3+TOTAL_MED*2+TOTAL_LOW*1),(TOTAL_MESES_POSIBLE_ACTIVIDAD*3)) AS SCORE_ENG,
      P.CONTENT_ID,
      FROM USER_MONTHS AS A
        LEFT JOIN `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` AS P ON P.USER_ID = A.USER_ID
      where SEGMENT_LIFE_CYCLE NOT IN ('1.ACQUISITION', '2.CONSIDERATION', '6.CHURN','7.LATENT','8.STOCK') AND AGING_MONTHS <= 3
      QUALIFY ROW_NUMBER()OVER(PARTITION BY A.USER_ID,A.SIT_SITE_ID ORDER BY P.START_PLAY_TIMESTAMP DESC)=1)   
GROUP BY ALL
)

SELECT 
DISTINCT SIT_SITE_ID, USER_ID
FROM FINAL_RESULTS
WHERE CALIDAD_ABSOLUTA IN ('A_HIGH_ENG')



--- ESTA OK