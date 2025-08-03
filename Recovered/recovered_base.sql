-- CAST --
-- Esta query genera la clasificacion de usuarios en función de varios aspectos como el tiempo de reproducción, la plataforma utilizada y si el usuario está logueado o no. La clasificacion principal es si el usuario es NEW, RETAINED o RECOVERED.
-- NOTA -- Se generan varias combinaciones de posibles clasificaciones en la consulta final.
-- TABLAS --
-- `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS`: tabla de reproducciones de Play
-- OBJETIVO --
-- Alimenta la hoja USERS TVM M y la hoja USERS TVM W del Sheets "Performance Mercado Play - Monthly & Weekly". Para alimentar USERS TVM W modificar el DATE_TRUNC en el CTE NEW_RET_RECO a WEEK


-- En esta CTE se clasifican a los usuarios en NEW, RETAINED o RECOVERED
WITH NEW_RET_RECO AS
  (
    SELECT 
        *
        -- Se toma el mes truncado de la fecha DS para definir el TIME_FRAME_ID
      , DATE_TRUNC(DS,MONTH) AS TIME_FRAME_ID --> ACA SOLAMENTE ELEGIMOS EL TIMEFRAME QUE SE QUIERE VER, WEEK,MONTH,DAY 
      -- Con este LAG se obtiene la fecha del día anterior al actual, particionando por SIT_SITE_ID y USER_ID
      , LAG(DS,1)OVER(PARTITION BY SIT_SITE_ID,USER_ID ORDER BY START_PLAY_TIMESTAMP ASC) AS DS_ANT
      -- En este CASE se define el FLAG_N_R, que puede ser NEW, RETAINED o RECOVERED dependiendo de la diferencia de días entre DS y DS_ANT
      , (CASE WHEN (LAG(DS,1)OVER(PARTITION BY SIT_SITE_ID,USER_ID ORDER BY START_PLAY_TIMESTAMP ASC)) IS NULL THEN 'NEW' 
              WHEN DATE_DIFF(DS, (LAG(DS,1)OVER(PARTITION BY SIT_SITE_ID,USER_ID ORDER BY START_PLAY_TIMESTAMP ASC)), DAY) <= 30 THEN 'RETAINED'
              WHEN DATE_DIFF(DS, (LAG(DS,1)OVER(PARTITION BY SIT_SITE_ID,USER_ID ORDER BY START_PLAY_TIMESTAMP ASC)), DAY) > 30  THEN 'RECOVERED'
              ELSE NULL END) AS FLAG_N_R
    FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS`
    WHERE  PLAYBACK_TIME_MILLISECONDS/1000 >= 20 
      AND DS <= CURRENT_DATE-1
  ),  
  -- La CTE ATTR_TIME_FRAME_ELEGIDO selecciona el primer registro de cada mes para cada usuario, manteniendo el FLAG_N_R
  ATTR_TIME_FRAME_ELEGIDO AS (
          SELECT 
          SIT_SITE_ID,
          USER_ID,
          TIME_FRAME_ID,
          FLAG_N_R
          FROM NEW_RET_RECO
          QUALIFY ROW_NUMBER()  OVER(PARTITION BY SIT_SITE_ID,USER_ID, TIME_FRAME_ID ORDER BY START_PLAY_TIMESTAMP ASC) =  1
  ),
  -- Se realiza un cruce entre NEW_RET_RECO y ATTR_TIME_FRAME_ELEGIDO para obtener el FLAG_N_R final teniendo en cuenta el primer registro de cada mes
  CRUCE_FLAG AS (
          SELECT
          A.*,
          E.FLAG_N_R AS FLAG_N_R_FINAL
          FROM NEW_RET_RECO AS A
          LEFT JOIN ATTR_TIME_FRAME_ELEGIDO AS E ON E.SIT_SITE_ID = A.SIT_SITE_ID AND E.USER_ID = A.USER_ID AND E.TIME_FRAME_ID = A.TIME_FRAME_ID
  ),
  -- Con RESUMEN_USER_TF se agrupan los datos por SIT_SITE_ID, USER_ID y TIME_FRAME_ID, sumando el tiempo de reproducción en minutos y separando por plataforma. Tambien se calcula el total de tiempo de reproduccion casteado
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
        SUM(PLAYBACK_TIME_MILLISECONDS_CAST/60000) AS TOTAL_CAST
        FROM CRUCE_FLAG
        GROUP BY ALL
  )
-- Esta es la consulta final que selecciona los datos agrupados de RESUMEN_USER_TF
    SELECT
  SIT_SITE_ID,
  TIME_FRAME_ID AS MONTH_ID,
-- Se genera una nueva clasificacion según el total de tiempo reproducido
CASE WHEN TVM_TOTAL_TIMEFRAME < 3 THEN 'A. MENOR A 3 MIN'
       WHEN TVM_TOTAL_TIMEFRAME  BETWEEN 3 AND 10 THEN 'B. ENTRE 3 Y 10 MIN'
       WHEN TVM_TOTAL_TIMEFRAME  BETWEEN 10 AND 30 THEN 'C. ENTRE 10 Y 30 MIN'              
       ELSE 'D. MAYOR A 30 MIN' 
       END AS TVM_TIMEFRAME,
-- Se trae el FLAG_N_R_FINAL como CUST_TYPE
  FLAG_N_R_FINAL AS CUST_TYPE,
-- Se clasifica segun si el usuario se logueo o no
  CASE WHEN SAFE_CAST(USER_ID AS INT64) IS NULL THEN 'NOT_LOG'
       ELSE 'LOG' 
       END AS FLAG_LOG,
  -- En este concat se genera una cadena que indica las plataformas en las que el usuario ha reproducido contenido
  CONCAT(CASE WHEN TOTAL_TV > 0 THEN 'SMART' ELSE '' END ,'',
  CASE WHEN TOTAL_MOBILE > 0 THEN 'MOBILE' ELSE '' END ,'',
  CASE WHEN TOTAL_DESKTOP > 0 THEN 'DESKTOP' ELSE '' END,
  CASE WHEN TOTAL_CAST > 0 THEN 'CAST' ELSE '' END ) AS PLATFORM,
  SUM(TVM_TOTAL_TIMEFRAME) AS TVM_TOTAL,
  -- Se cuenta el total de usuarios por mes
  COUNT(DISTINCT USER_ID) AS TOTAL_USERS
  FROM  RESUMEN_USER_TF
  WHERE TIME_FRAME_ID >= '2025-06-01'
  GROUP BY ALL
  ORDER BY MONTH_ID;
