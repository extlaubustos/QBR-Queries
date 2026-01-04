-- description: Clasificación de usuarios por tipo (new, retained, recovered), nivel de consumo y plataforma dominante, agregada por semana
-- domain: behaviour
-- product: mplay
-- use_case: reporting
-- grain: site, week, tvm_bucket, customer_type, login_flag, platform
-- time_grain: weekly
-- date_column: DS
-- date_filter: <= current_date - 1
-- threshold_rule: playback_time >= 20s
-- metrics:
--   - TVM_TOTAL: minutos totales reproducidos por usuario en la semana
--   - TOTAL_USERS: cantidad de usuarios únicos por segmento
-- tables_read:
--   - WHOWNER.BT_MKT_MPLAY_PLAYS
-- joins:
--   - none (uso de CTEs y funciones de ventana)
-- owner: data_team

  WITH NEW_RET_RECO AS
    (
      SELECT 
          *
        , DATE_TRUNC(DS,WEEK(MONDAY)) AS TIME_FRAME_ID --> ACA SOLAMENTE ELEGIMOS EL TIMEFRAME QUE SE QUIERE VER, WEEK,MONTH,DAY 
        , LAG(DS,1)OVER(PARTITION BY SIT_SITE_ID,USER_ID ORDER BY START_PLAY_TIMESTAMP ASC) AS DS_ANT 
        , (CASE WHEN (LAG(DS,1)OVER(PARTITION BY SIT_SITE_ID,USER_ID ORDER BY START_PLAY_TIMESTAMP ASC)) IS NULL THEN 'NEW' 
                WHEN DATE_DIFF(DS, (LAG(DS,1)OVER(PARTITION BY SIT_SITE_ID,USER_ID ORDER BY START_PLAY_TIMESTAMP ASC)), DAY) <= 30 THEN 'RETAINED'
                WHEN DATE_DIFF(DS, (LAG(DS,1)OVER(PARTITION BY SIT_SITE_ID,USER_ID ORDER BY START_PLAY_TIMESTAMP ASC)), DAY) > 30  THEN 'RECOVERED'
                ELSE NULL END) AS FLAG_N_R

      FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS`

      WHERE  PLAYBACK_TIME_MILLISECONDS/1000 >= 20 
        AND DS <= CURRENT_DATE-1
    ),
    ATTR_TIME_FRAME_ELEGIDO AS (

            SELECT 
            SIT_SITE_ID,
            USER_ID,
            TIME_FRAME_ID,
            FLAG_N_R
            FROM NEW_RET_RECO
            QUALIFY ROW_NUMBER()  OVER(PARTITION BY SIT_SITE_ID,USER_ID,
                                                  TIME_FRAME_ID
                                            ORDER BY START_PLAY_TIMESTAMP ASC) =  1 --> ME QUEDO CON EL PRIMER PLAY DEL TIMEFRAME PARA ATRIBUIR 1 
    ),
    CRUCE_FLAG AS (
            SELECT
            A.*,
            E.FLAG_N_R AS FLAG_N_R_FINAL
            FROM NEW_RET_RECO AS A
            LEFT JOIN ATTR_TIME_FRAME_ELEGIDO AS E ON E.SIT_SITE_ID = A.SIT_SITE_ID 
                                                  AND E.USER_ID = A.USER_ID
                                                  AND E.TIME_FRAME_ID = A.TIME_FRAME_ID
    ),
    RESUMEN_USER_TF AS (

          SELECT
          SIT_SITE_ID,
          USER_ID,
          TIME_FRAME_ID,
          FLAG_N_R_FINAL,
          SUM(PLAYBACK_TIME_MILLISECONDS/60000) AS TVM_TOTAL_TIMEFRAME,
          SUM(CASE WHEN UPPER(DEVICE_PLATFORM) LIKE '%TV%' THEN 
                    PLAYBACK_TIME_MILLISECONDS/60000 ELSE 0 END) AS TOTAL_TV,
          SUM(CASE WHEN UPPER(DEVICE_PLATFORM) LIKE '%MOBILE%' THEN 
                    PLAYBACK_TIME_MILLISECONDS/60000 ELSE 0 END) AS TOTAL_MOBILE,
          SUM(CASE WHEN UPPER(DEVICE_PLATFORM) LIKE '%DESK%' THEN 
                    PLAYBACK_TIME_MILLISECONDS/60000 ELSE 0 END) AS TOTAL_DESKTOP,
          SUM(CASE WHEN PLAYBACK_TIME_MILLISECONDS_CAST/1000 >= 20 THEN PLAYBACK_TIME_MILLISECONDS_CAST/60000 ELSE 0 END) AS TOTAL_CAST

          FROM CRUCE_FLAG
          GROUP BY ALL

    )
    SELECT
    SIT_SITE_ID,
    TIME_FRAME_ID AS WEEK_ID,
    CASE WHEN TVM_TOTAL_TIMEFRAME < 3 THEN 'A. MENOR A 3 MIN'
        WHEN TVM_TOTAL_TIMEFRAME  BETWEEN 3 AND 10 THEN 'B. ENTRE 3 Y 10 MIN'
        WHEN TVM_TOTAL_TIMEFRAME  BETWEEN 10 AND 30 THEN 'C. ENTRE 10 Y 30 MIN'              
        ELSE 'D. MAYOR A 30 MIN' 
        END AS TVM_TIMEFRAME,
    FLAG_N_R_FINAL AS CUST_TYPE,
    CASE WHEN SAFE_CAST(USER_ID AS INT64) IS NULL THEN 'NOT_LOG'
        ELSE 'LOG' 
        END AS FLAG_LOG,
    CASE
        WHEN ROUND(TVM_TOTAL_TIMEFRAME, 2) = ROUND(TOTAL_CAST, 2) AND TOTAL_CAST > 0 THEN 'CAST' -- Esto es lo nuevo 
        ELSE CONCAT(
          CASE WHEN TOTAL_TV > 0 THEN 'SMART' ELSE '' END ,'',
          CASE WHEN TOTAL_MOBILE > 0 THEN 'MOBILE' ELSE '' END ,'',
          CASE WHEN TOTAL_DESKTOP > 0 THEN 'DESKTOP' ELSE '' END ,'',
          CASE WHEN TOTAL_CAST > 0 THEN 'CAST' ELSE '' END
        ) END AS PLATFORM,
    SUM(TVM_TOTAL_TIMEFRAME) AS TVM_TOTAL,
    COUNT(DISTINCT USER_ID) AS TOTAL_USERS
    FROM  RESUMEN_USER_TF
    WHERE TIME_FRAME_ID >= '2024-12-30'
    GROUP BY ALL
    ORDER BY WEEK_ID;