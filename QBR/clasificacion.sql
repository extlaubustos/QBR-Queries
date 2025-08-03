-- CLASIFICACION CON AHA --
-- Esta query genera la clasificacion de usuarios en función de varios aspectos como el tiempo de reproducción, la plataforma utilizada y si el usuario está logueado o no. La clasificacion principal es si el usuario es NEW, RETAINED o RECOVERED. Una clasificación diferente a la realizada en cast es que se agrega el AHA_MOMENT.
-- NOTA -- Se generan varias combinaciones de posibles clasificaciones en la consulta final.
-- TABLAS --
-- `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS`: tabla de reproducciones de Play


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
              ELSE NULL END) AS FLAG_N_R,
    -- Se toma la menor fecha por SIT_SITE_ID y USER_ID para definir la primer fecha de visualización del usuario
     MIN(DS) OVER (PARTITION BY SIT_SITE_ID,USER_ID) AS FIRST_DS_USER
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
          E.FECHA_FLAG_N_R,
          E.FLAG_N_R AS FLAG_N_R_FINAL,
        -- Se define el AHA_MOMENT realizando un conteo de fechas distintas donde se haya visualizado entre la primer fecha de visualización del usuario y 30 días despues
        COUNT(DISTINCT CASE WHEN DS BETWEEN FIRST_DS_USER AND FIRST_DS_USER+30 
                            THEN DS ELSE NULL END) OVER (PARTITION BY A.SIT_SITE_ID,A.USER_ID) AS DS_AHA_MOMENT
                                                
          FROM NEW_RET_RECO AS A
          LEFT JOIN ATTR_TIME_FRAME_ELEGIDO AS E ON E.SIT_SITE_ID = A.SIT_SITE_ID 
                                                AND E.USER_ID = A.USER_ID
                                                AND E.TIME_FRAME_ID = A.TIME_FRAME_ID
  ),
  -- Con RESUMEN_USER_TF se agrupan los datos por SIT_SITE_ID, USER_ID y TIME_FRAME_ID, sumando el tiempo de reproducción en minutos y separando por plataforma
  RESUMEN_USER_TF AS (
        SELECT
        SIT_SITE_ID,
        USER_ID,
        TIME_FRAME_ID,
        FECHA_FLAG_N_R,
        FLAG_N_R_FINAL,
        DS_AHA_MOMENT,        
        SUM(PLAYBACK_TIME_MILLISECONDS/60000) AS TVM_TOTAL_TIMEFRAME,
        SUM(CASE WHEN UPPER(DEVICE_PLATFORM) LIKE '%TV%' THEN 
                   PLAYBACK_TIME_MILLISECONDS/60000 ELSE 0 END) AS TOTAL_TV,
        SUM(CASE WHEN UPPER(DEVICE_PLATFORM) LIKE '%MOBILE%' THEN 
                   PLAYBACK_TIME_MILLISECONDS/60000 ELSE 0 END) AS TOTAL_MOBILE,
        SUM(CASE WHEN UPPER(DEVICE_PLATFORM) LIKE '%DESK%' THEN 
                   PLAYBACK_TIME_MILLISECONDS/60000 ELSE 0 END) AS TOTAL_DESKTOP,
        FROM CRUCE_FLAG
        GROUP BY ALL

  )
  -- Esta es la consulta final que selecciona los datos agrupados de RESUMEN_USER_TF
  SELECT
  A.TIME_FRAME_ID,
  A.FLAG_N_R_FINAL,
  A.SIT_SITE_ID,
-- Se clasifica segun si el usuario se logueo o no
  case when safe_cast(A.user_id as int64)is null then 'not_log' else 'log' end as flag_user,
  -- En este concat se genera una cadena que indica las plataformas en las que el usuario ha reproducido contenido
  CONCAT( CASE WHEN A.TOTAL_TV > 0 THEN 'SMART' ELSE '' END ,' - ',
  CASE WHEN A.TOTAL_MOBILE > 0 THEN 'MOBILE' ELSE '' END ,' - ',
  CASE WHEN A.TOTAL_DESKTOP > 0 THEN 'DESKTOP' ELSE '' END ) AS PLATFORM_CONCAT,
-- Se genera una nueva clasificacion según el total de tiempo reproducido
        CASE WHEN A.TVM_TOTAL_TIMEFRAME < 3 THEN 'A. MENOR A 3 MIN'
              WHEN A.TVM_TOTAL_TIMEFRAME  BETWEEN 3 AND 10 THEN 'B. ENTRE 3 Y 10 MIN'
              WHEN A.TVM_TOTAL_TIMEFRAME  BETWEEN 10 AND 30 THEN 'C. ENTRE 10 Y 30 MIN'              
              ELSE 'D. MAYOR A 30 MIN' END AS RANGE_TVM_TIMEFRAME,
-- Si vio mas de 3 dias distintos en el periodo de 30 dias se flaguea como AHA_MOMENT sino no
CASE WHEN A.DS_AHA_MOMENT >= 3 THEN 'AHA_MOMENT' ELSE 'NOT_AHA' END AS FLAG_AHA_MOMENT,
  COUNT(DISTINCT A.USER_ID) AS TOTAL_USERS,

  FROM   RESUMEN_USER_TF AS A                                                                       
  GROUP BY ALL