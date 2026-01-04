-- description: Función para obtener resumen de reproducciones MPlay por sitio y mes, categorizando por tiempo total de reproducción, tipo de usuario y plataformas utilizadas
-- domain: media_analytics
-- product: mplay
-- use_case: user_engagement_summary
-- grain: site, month
-- time_grain: monthly
-- date_column: DS
-- date_filter: PLAYBACK_TIME_MILLISECONDS/1000 >= 20 AND DS <= CURRENT_DATE-1
-- metrics:
-- - TVM_TOTAL: Minutos totales de reproducción por usuario en el mes
-- - TOTAL_TV: Minutos de reproducción en dispositivos TV
-- - TOTAL_MOBILE: Minutos de reproducción en dispositivos móviles
-- - TOTAL_DESKTOP: Minutos de reproducción en escritorio
-- - TOTAL_CAST: Minutos de reproducción cast
-- - TOTAL_USERS: Conteo de usuarios distintos por mes
-- dimensions:
-- - SIT_SITE_ID
-- - MONTH_ID
-- - TVM_TIMEFRAME: Categoría de tiempo de reproducción
-- - CUST_TYPE: Tipo de usuario (NEW, RETAINED, RECOVERED)
-- - FLAG_LOG: Usuario logueado o no
-- - PLATFORM: Combinación de plataformas utilizadas
-- tables_read:
-- - WHOWNER.BT_MKT_MPLAY_PLAYS
-- - STG.TORRE_AUX_SITE
-- joins:
-- - LEFT JOIN CASTED_PLAYS y CRUCE_FLAG por SIT_SITE_ID, USER_ID y TIME_FRAME_ID
-- owner: data_team

CREATE OR REPLACE TABLE FUNCTION `STG.TORRE_LE_PLAY_PLAYS_NEW_L1` (v_sit_site_id STRING, v_dt_to DATE) AS 
  

SELECT  DISTINCT
          ds AS tim_day,
          p.sit_site_id,
          start_play_timestamp AS start_time,
          CASE
            WHEN UPPER(device_platform) = '/WEB/DESKTOP'    THEN 'DESKTOP'
            WHEN UPPER(device_platform) = '/MOBILE/ANDROID' THEN 'ANDROID'
            WHEN UPPER(device_platform) = '/MOBILE/IOS'     THEN 'IOS'
            WHEN UPPER(device_platform) = '/WEB/MOBILE'     THEN 'MOBILE-BROWSER'
            WHEN UPPER(device_platform) = '/TV/MACOS'       THEN 'APPLE-TV'
            WHEN UPPER(device_platform) = '/TV/MAC OS X'    THEN 'APPLE-TV'
            WHEN UPPER(device_platform) = '/TV/WEB0S'       THEN 'LG'
            WHEN UPPER(device_platform) = '/TV/TIZEN'       THEN 'SAMSUNG'
            WHEN UPPER(device_platform) = '/TV/ANDROID'     THEN 'ANDROID-TV'
            WHEN UPPER(device_platform) LIKE '/TV%'         THEN 'SMART-TV'
            WHEN UPPER(device_platform) IS NULL 
              OR UPPER(device_platform) = 'UNKNOWN'         THEN 'OTHERS'
                                                            ELSE UPPER(device_platform) 
          END AS device_platform,
          CASE
            
            WHEN UPPER(device_platform) = '/WEB/DESKTOP'    THEN 'DESKTOP'
            WHEN UPPER(device_platform) = '/MOBILE/ANDROID' THEN 'MOBILE'
            WHEN UPPER(device_platform) = '/MOBILE/IOS'     THEN 'MOBILE'
            WHEN UPPER(device_platform) = '/WEB/MOBILE'     THEN 'MOBILE'
            WHEN UPPER(device_platform) = '/TV/MACOS'       THEN 'SMART-TV'
            WHEN UPPER(device_platform) = '/TV/MAC OS X'    THEN 'SMART-TV'
            WHEN UPPER(device_platform) = '/TV/WEB0S'       THEN 'SMART-TV'
            WHEN UPPER(device_platform) = '/TV/TIZEN'       THEN 'SMART-TV'
            WHEN UPPER(device_platform) = '/TV/ANDROID'     THEN 'SMART-TV'
            WHEN UPPER(device_platform) LIKE '/TV%'         THEN 'SMART-TV'
            WHEN UPPER(device_platform) IS NULL 
              OR UPPER(device_platform) = 'UNKNOWN'         THEN 'OTHERS'
                                                            ELSE UPPER(device_platform)
          END AS platform,
          SAFE_CAST(user_id AS INT64) IS NOT NULL AS logged_user,
          user_id,
          (playback_time_milliseconds - playback_time_milliseconds_cast ) / 1000 / 60       AS tvm,
          
          LAG(ds) OVER (PARTITION BY p.sit_site_id, user_id ORDER BY start_play_timestamp) AS prev_tim_day
  FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` AS p
  INNER JOIN `STG.TORRE_AUX_SITE` site
    ON site.sit_site_id = p.sit_site_id
      AND site.bu = 'LE'
      AND IF(v_sit_site_id = 'ALL', TRUE, site.sit_site_id = v_sit_site_id)
 WHERE (playback_time_milliseconds - playback_time_milliseconds_cast ) / 1000 >= 20



UNION ALL

SELECT  DISTINCT
          ds AS tim_day,
          p.sit_site_id,
          start_play_timestamp AS start_time,
          CASE
            WHEN UPPER(device_platform) = '/WEB/DESKTOP'    THEN 'DESKTOP'
            WHEN UPPER(device_platform) = '/MOBILE/ANDROID' THEN 'ANDROID'
            WHEN UPPER(device_platform) = '/MOBILE/IOS'     THEN 'IOS'
            WHEN UPPER(device_platform) = '/WEB/MOBILE'     THEN 'MOBILE-BROWSER'
            WHEN UPPER(device_platform) = '/TV/MACOS'       THEN 'APPLE-TV'
            WHEN UPPER(device_platform) = '/TV/MAC OS X'    THEN 'APPLE-TV'
            WHEN UPPER(device_platform) = '/TV/WEB0S'       THEN 'LG'
            WHEN UPPER(device_platform) = '/TV/TIZEN'       THEN 'SAMSUNG'
            WHEN UPPER(device_platform) = '/TV/ANDROID'     THEN 'ANDROID-TV'
            WHEN UPPER(device_platform) LIKE '/TV%'         THEN 'SMART-TV'
            WHEN UPPER(device_platform) IS NULL 
              OR UPPER(device_platform) = 'UNKNOWN'         THEN 'OTHERS'
                                                            ELSE UPPER(device_platform) 
          END AS device_platform,
          'CAST' AS platform,
          SAFE_CAST(user_id AS INT64) IS NOT NULL AS logged_user,
          user_id,
          playback_time_milliseconds_cast / 1000 / 60       AS tvm,
          
          LAG(ds) OVER (PARTITION BY p.sit_site_id, user_id ORDER BY start_play_timestamp) AS prev_tim_day
  FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` AS p
  INNER JOIN `STG.TORRE_AUX_SITE` site
    ON site.sit_site_id = p.sit_site_id
      AND site.bu = 'LE'
      AND IF(v_sit_site_id = 'ALL', TRUE, site.sit_site_id = v_sit_site_id)
 WHERE playback_time_milliseconds_cast  / 1000 >= 20





















 WITH NEW_RET_RECO AS
  (
    SELECT 
        *
      , DATE_TRUNC(DS,MONTH) AS TIME_FRAME_ID --> ACA SOLAMENTE ELEGIMOS EL TIMEFRAME QUE SE QUIERE VER, WEEK,MONTH,DAY 
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
  CASTED_PLAYS AS (
      SELECT
        SIT_SITE_ID,
        USER_ID,
        DATE_TRUNC(DS, MONTH) AS TIME_FRAME_ID,
        SUM(PLAYBACK_TIME_MILLISECONDS_CAST / 60000) AS TOTAL_CAST_MINUTES
      FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS`
      WHERE PLAYBACK_TIME_MILLISECONDS_CAST / 1000 >= 20
      GROUP BY ALL
  ),
  RESUMEN_USER_TF AS (
      SELECT
        A.SIT_SITE_ID,
        A.USER_ID,
        A.TIME_FRAME_ID,
        A.FLAG_N_R_FINAL,
        SUM(A.PLAYBACK_TIME_MILLISECONDS/60000) AS TVM_TOTAL_TIMEFRAME,
        SUM(CASE WHEN UPPER(A.DEVICE_PLATFORM) LIKE '%TV%' THEN 
                A.PLAYBACK_TIME_MILLISECONDS/60000 ELSE 0 END) AS TOTAL_TV,
        SUM(CASE WHEN UPPER(A.DEVICE_PLATFORM) LIKE '%MOBILE%' THEN 
                A.PLAYBACK_TIME_MILLISECONDS/60000 ELSE 0 END) AS TOTAL_MOBILE,
        SUM(CASE WHEN UPPER(A.DEVICE_PLATFORM) LIKE '%DESK%' THEN 
                A.PLAYBACK_TIME_MILLISECONDS/60000 ELSE 0 END) AS TOTAL_DESKTOP,
        COALESCE(B.TOTAL_CAST_MINUTES, 0) AS TOTAL_CAST
      FROM CRUCE_FLAG AS A
      LEFT JOIN CASTED_PLAYS AS B ON A.SIT_SITE_ID = B.SIT_SITE_ID 
                                 AND A.USER_ID = B.USER_ID
                                 AND A.TIME_FRAME_ID = B.TIME_FRAME_ID
      GROUP BY ALL
  )
  SELECT
    SIT_SITE_ID,
    TIME_FRAME_ID AS MONTH_ID,
    CASE WHEN TVM_TOTAL_TIMEFRAME < 3 THEN 'A. MENOR A 3 MIN'
         WHEN TVM_TOTAL_TIMEFRAME  BETWEEN 3 AND 10 THEN 'B. ENTRE 3 Y 10 MIN'
         WHEN TVM_TOTAL_TIMEFRAME  BETWEEN 10 AND 30 THEN 'C. ENTRE 10 Y 30 MIN'            
         ELSE 'D. MAYOR A 30 MIN' 
         END AS TVM_TIMEFRAME,
    FLAG_N_R_FINAL AS CUST_TYPE,
    CASE WHEN SAFE_CAST(USER_ID AS INT64) IS NULL THEN 'NOT_LOG'
         ELSE 'LOG' 
         END AS FLAG_LOG,
    CONCAT(CASE WHEN TOTAL_TV > 0 THEN 'SMART' ELSE '' END ,'',
    CASE WHEN TOTAL_MOBILE > 0 THEN 'MOBILE' ELSE '' END ,'',
    CASE WHEN TOTAL_DESKTOP > 0 THEN 'DESKTOP' ELSE '' END,
    CASE WHEN TOTAL_CAST > 0 THEN 'CAST' ELSE '' END ) AS PLATFORM,
    SUM(TVM_TOTAL_TIMEFRAME) AS TVM_TOTAL,
    COUNT(DISTINCT USER_ID) AS TOTAL_USERS
  FROM RESUMEN_USER_TF
  WHERE TIME_FRAME_ID >= '2024-01-01'
  GROUP BY ALL
  ORDER BY MONTH_ID;


  