CREATE OR REPLACE TABLE `meli-sbox.MPLAY.MPLAY_NEGOCIO_ADQUITSTION_METRICS` AS (
  -- CTE para calcular las conversiones por sitio, origen y fecha
  WITH SOURCE_CONVERTION AS (
    SELECT
      SIT_SITE_ID,
      -- Si ORIGIN_PATH es nulo, se reemplaza con 'NULO'
      COALESCE(ORIGIN_PATH,'NULO') AS ORIGIN_PATH,
      FECHA_ESTIMULOS,
      -- Suma los pesos de conversión
      SUM(CONVERTION_W) AS CONVERTIONS
    FROM `meli-sbox.MPLAY.MPLAY_NEGOCIO_ATT_NEW_USERS`
    GROUP BY ALL
  ),
  -- CTE para calcular el tráfico total y el tráfico de visitantes por origen y fecha
  TOTAL_SOURCE_TRAFICO AS (
    SELECT
      -- Si ORIGIN_PATH de la sesión es nulo, se reemplaza con 'NULO'
      COALESCE(S.ORIGIN_PATH,'NULO') AS ORIGIN_PATH,
      S.DS AS FECHA_ESTIMULOS,
      S.SIT_SITE_ID,
      -- Cuenta el total de usuarios únicos que tuvieron una sesión
      COUNT(DISTINCT S.USER_ID) AS TOTAL_TRAFICO,
      -- Cuenta el total de usuarios únicos que tuvieron una interacción significativa (búsqueda, VCP, play, VCM o más de 1 impresión en feed)
      COUNT(DISTINCT CASE WHEN ((HAS_SEARCH IS TRUE OR HAS_VCP IS TRUE OR HAS_PLAY IS TRUE OR HAS_VCM IS TRUE) OR TOTAL_FEED_IMPRESSIONS > 1) THEN S.USER_ID ELSE NULL END) AS TOTAL_TRAFICO_VISITOR,
    FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_SESSION` AS S
    -- Se hace un LEFT JOIN con una subconsulta para identificar usuarios ya convertidos
    LEFT JOIN
      ( SELECT DISTINCT
        USER_ID,
        SIT_SITE_ID,
        FECHA_CONV
      FROM `meli-sbox.MPLAY.MPLAY_NEGOCIO_ATT_NEW_USERS`) AS C
      -- Condiciones de join para SIT_SITE_ID y USER_ID
      ON C.USER_ID = S.USER_ID
      AND C.SIT_SITE_ID = S.SIT_SITE_ID
      -- Excluye sesiones de usuarios que ya se habían convertido antes de la fecha de la sesión actual
      AND C.FECHA_CONV < S.DS
    -- Se filtran solo los usuarios que no han sido convertidos (es decir, tráfico de usuarios nuevos)
    WHERE C.USER_ID IS NULL
    GROUP BY ALL
  )

  -- Consulta final para crear la tabla MPLAY_NEGOCIO_ADQUITSTION_METRICS
  SELECT
  -- COALESCE(SC.DEVICE_PLATFORM AS PLATFORM_CONVERTION, -- Comentario de columna, posiblemente deshabilitado
    -- Se unifica el nombre del negocio (SOURCE_SESSION_L1) o 'NULO' si es nulo
    COALESCE(OM.SOURCE_SESSION_L1,'NULO') AS NEGOCIO,
    -- Se unifica el nombre del equipo o 'NULO' si es nulo
    COALESCE(OM.TEAM,'NULO') AS TEAM,
    -- Se unifica el ORIGIN_PATH (SOURCE_SESSION_L2) o 'NULO' si es nulo
    COALESCE(OM.SOURCE_SESSION_L2,'NULO') AS ORIGIN_PATH,
    S.FECHA_ESTIMULOS,
    S.SIT_SITE_ID,
    S.TOTAL_TRAFICO,
    S.TOTAL_TRAFICO_VISITOR,
    SC.CONVERTIONS
  FROM TOTAL_SOURCE_TRAFICO AS S
  -- LEFT JOIN con SOURCE_CONVERTION para combinar el tráfico con las conversiones
  LEFT JOIN SOURCE_CONVERTION AS SC
    ON SC.SIT_SITE_ID = S.S.SIT_SITE_ID
    AND SC.ORIGIN_PATH = S.ORIGIN_PATH
    AND SC.FECHA_ESTIMULOS = S.FECHA_ESTIMULOS
  -- LEFT JOIN con la tabla de lookup de tipos de origen de sesión para obtener las dimensiones de negocio y equipo
  LEFT JOIN `meli-sbox.MPLAY.LK_MPLAY_SOURCE_TYPE_ORIGIN_SESSION` AS OM
    ON OM.SOURCE_TYPE = S.ORIGIN_PATH
);

CREATE OR REPLACE TABLE `meli-sbox.MPLAY.MPLAY_NEGOCIO_ATT_NEW_USERS` AS (
  SELECT
    USER_ID,
    SIT_SITE_ID,
    FECHA_CONV,
    DEVICE_PLATFORM,
    ORIGIN_PATH,
    FECHA_ESTIMULOS,
    DIAS_TO_CONV,
    ORDER_DESC_TIME,
    CONVERTION_W,
    (
      -- Calcula la retención a 30 días: 1 si el usuario tuvo un play de >= 20 segundos entre FECHA_CONV + 1 y FECHA_CONV + 30 días
      SELECT MAX(CASE WHEN P.USER_ID IS NOT NULL THEN 1 ELSE 0 END)
      FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` AS P
      WHERE P.USER_ID = A.USER_ID
        AND P.SIT_SITE_ID = A.SIT_SITE_ID
        AND P.DS BETWEEN A.FECHA_CONV +1 AND A.FECHA_CONV+30
        AND P.PLAYBACK_TIME_MILLISECONDS/1000>=20
    ) AS RET_1_30,
    (
      -- Calcula la retención a 60 días: 1 si el usuario tuvo un play de >= 20 segundos entre FECHA_CONV + 31 y FECHA_CONV + 61 días
      SELECT MAX(CASE WHEN P.USER_ID IS NOT NULL THEN 1 ELSE 0 END)
      FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` AS P
      WHERE P.USER_ID = A.USER_ID
        AND P.SIT_SITE_ID = A.SIT_SITE_ID
        AND P.DS BETWEEN A.FECHA_CONV + 31 AND A.FECHA_CONV + 61
        AND P.PLAYBACK_TIME_MILLISECONDS/1000>=20) AS RET_31_60,
    (
      -- Calcula el 'AHA_MOMENT': 1 si el usuario tuvo plays en al menos 3 días distintos en los primeros 29 días desde FECHA_CONV
      SELECT CASE WHEN COUNT(DISTINCT P.DS) >= 3 THEN 1 ELSE 0 END
      FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` AS P
      WHERE P.USER_ID = A.USER_ID
        AND P.SIT_SITE_ID = A.SIT_SITE_ID
        AND P.DS BETWEEN A.FECHA_CONV AND A.FECHA_CONV + 29
        AND P.PLAYBACK_TIME_MILLISECONDS/1000>=20) AS AHA_MOMENT
  FROM `meli-sbox.MPLAY.MPLAY_NEGOCIO_ATT_NEW_USERS` AS A);

CREATE OR REPLACE TABLE meli-sbox.MPLAY.MPLAY_NEGOCIO_COHORTS_DASH AS (
  WITH PLAY_DAYS AS (
    -- CTE para obtener los días de reproducción y el tiempo total de reproducción por usuario
    SELECT DISTINCT
      P.SIT_SITE_ID AS SIT_SITE_ID,
      (P.USER_ID) AS USER_ID,
      (P.DS) AS DAY_PLAY,
      -- Suma el tiempo de reproducción en minutos (convirtiendo milisegundos)
      SUM(P.PLAYBACK_TIME_MILLISECONDS/60000) AS PLAYBACK_TIME,
    FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` as P
    -- Se consideran solo los plays de 20 segundos o más
    WHERE P.PLAYBACK_TIME_MILLISECONDS/1000 >= 20
    GROUP BY ALL
  ),
  PLAY_FIRST_DAY AS (
    -- CTE para encontrar el primer día de reproducción para cada usuario en cada sitio
    SELECT
      SIT_SITE_ID,
      USER_ID,
      MIN(DAY_PLAY) AS FIRST_DAY
    FROM PLAY_DAYS
    GROUP BY ALL
  ),
  TABLE_CALENDAR AS (
    -- CTE compleja para generar un calendario de cohortes mensuales de 30 días
    SELECT
      *
    FROM (
      SELECT
        *,
        -- Asigna un número de mes a cada cohorte de 30 días dentro del mismo FECHA_COHORT
        ROW_NUMBER()OVER(PARTITION BY FECHA_COHORT ORDER BY FECHA_INI ASC) AS MONTH_NUMBER
      FROM (
        SELECT
          T.TIM_DAY AS FECHA_COHORT,
          -- Calcula la fecha de inicio del período de 30 días (29 días antes de FECHA_FIN)
          T2.TIM_DAY -29 AS FECHA_INI,
          T2.TIM_DAY AS FECHA_FIN,
        FROM `meli-bi-data.WHOWNER.LK_TIM_DAYS` AS T
        LEFT JOIN `meli-bi-data.WHOWNER.LK_TIM_DAYS` AS T2 ON T.TIM_DAY+1 <= T2.TIM_DAY
      -- +1 PARA SACAR DIA DE ALTA (esto es un comentario original en el código)
        WHERE T.TIM_DAY >= DATE'2023-07-01'
        -- Filtra para obtener solo rangos de 30 días completos (modulo 30 es 0)
        QUALIFY MOD(ROW_NUMBER()OVER(PARTITION BY T.TIM_DAY ORDER BY T2.TIM_DAY ASC), 30) = 0
      ) AS A
      -- Asegura que la fecha final del período de 30 días no sea mayor que el último día del mes anterior al actual
      WHERE FECHA_FIN <= DATE_TRUNC(CURRENT_DATE-1,MONTH)-1
      ) AS B
    -- Asegura que la cohorte tenga 30 días completos en el mes correspondiente
    QUALIFY COUNT(FECHA_FIN)OVER (PARTITION BY DATE_TRUNC(FECHA_COHORT,MONTH),MONTH_NUMBER) = EXTRACT(DAY FROM(LAST_DAY(FECHA_COHORT)))
  -- COHORT_HASTA AYER (esto es un comentario original en el código)
  ),
  USERS_CALENDAR AS (
    -- CTE que une el primer día de play de cada usuario con los períodos del calendario de cohortes
    SELECT
      PF.*,
      T.FECHA_INI,
      T.FECHA_FIN,
      MONTH_NUMBER
    FROM PLAY_FIRST_DAY AS PF
    LEFT JOIN TABLE_CALENDAR AS T ON PF.FIRST_DAY = T.FECHA_COHORT
  ) ,
  BASE AS (
    -- CTE base para los cálculos de retención
    SELECT
      *,
      -- Define si el usuario es 'ALL_MONTH' (activo en todos los meses consecutivos desde la adquisición) o 'NOT_RECURRENT'
      CASE WHEN FLAG_TVM > 0 AND
      ROW_NUMBER()OVER(PARTITION BY SIT_SITE_ID,USER_ID,FLAG_TVM ORDER BY MONTH_NUMBER ASC) = MONTH_NUMBER THEN 'ALL_MONTH' ELSE 'NOT_RECURRENT' END AS FLAG_CONSEC,
      -- Calcula el total de usuarios en cada cohorte de adquisición
      COUNT(DISTINCT USER_ID || SIT_SITE_ID) OVER(PARTITION BY MONTH_COHORT_ACQ,SIT_SITE_ID) AS TOTAL_USERS_COHORT
    FROM (
      SELECT
        U.USER_ID,
        U.SIT_SITE_ID,
        U.MONTH_NUMBER,
        -- Trunca el primer día de play al inicio del mes para definir la cohorte de adquisición
        DATE_TRUNC(U.FIRST_DAY,MONTH) AS MONTH_COHORT_ACQ,
        -- Suma el tiempo total visto en minutos (TVM) para cada usuario en cada período de 30 días
        SUM(PLAYBACK_TIME) AS TVM,
        -- Flag para indicar si hubo actividad (TVM > 0)
        CASE WHEN SUM(PLAYBACK_TIME)>0 THEN 1 ELSE 0 END AS FLAG_TVM
      FROM USERS_CALENDAR AS U
      -- LEFT JOIN con PLAY_DAYS para obtener el tiempo de reproducción dentro del período de lah cohorte
      LEFT JOIN PLAY_DAYS AS P
        ON U.USER_ID = P.USER_ID
        AND U.SIT_SITE_ID = P.SIT_SITE_ID
        AND P.DAY_PLAY BETWEEN U.FECHA_INI AND U.FECHA_FIN
      GROUP BY ALL
    )
  )

  -- Consulta final para crear la tabla MPLAY_NEGOCIO_COHORTS_DASH
  SELECT
    SIT_SITE_ID,
    MONTH_COHORT_ACQ,
    -- Calcula el mes de retención (MONTH_NUMBER - 1 para que el primer mes sea 0)
    MONTH_NUMBER-1 AS MONTH_RETENTION,
    TOTAL_USERS_COHORT AS TOTAL_USERS_COHORT,
    -- Cuenta el total de usuarios con retención (TVM > 0)
    COUNT(DISTINCT CASE WHEN TVM > 0 THEN USER_ID||SIT_SITE_ID ELSE NULL END) AS TOTAL_USERS_RETENTION,
    -- Suma el tiempo total visto en minutos
    SUM(TVM) AS TVM,
    -- Cuenta los usuarios que tuvieron retención 'ALL_MONTH'
    COUNT(DISTINCT CASE WHEN TVM > 0 AND FLAG_CONSEC = 'ALL_MONTH' THEN USER_ID||SIT_SITE_ID ELSE NULL END) AS ALL_MONTH_USER_RET,
    -- Suma el TVM de los usuarios que tuvieron retención 'ALL_MONTH'
    SUM( CASE WHEN TVM > 0 AND FLAG_CONSEC = 'ALL_MONTH' THEN TVM ELSE NULL END) AS ALL_MONTH_TVM
  FROM BASE
  -- Excluye registros donde MONTH_NUMBER es nulo (es decir, no asociados a una cohorte)
  WHERE MONTH_NUMBER IS NOT NULL
  GROUP BY ALL
); --> AHA_MOMENT_ATTRIBUTION

CREATE OR REPLACE TABLE `meli-sbox.MPLAY.NEGOCIO_MPLAY_CONTENT_DATA_USER`
PARTITION BY TIME_FRAME_ID
CLUSTER BY SIT_SITE_ID,CONTENT_ID,USER_ID
 AS (
  -- CTE para identificar nuevos usuarios, retenidos y recuperados en función de su actividad de reproducción
  WITH NEW_RET_RECO AS
  (
    SELECT
        *,
      -- Define el marco de tiempo (mes) para la cohorte
      DATE_TRUNC(DS,MONTH) AS TIME_FRAME_ID, --> ACA SOLAMENTE ELEGIMOS EL TIMEFRAME QUE SE QUIERE VER, WEEK,MONTH,DAY
      -- Obtiene la fecha del play anterior para el mismo usuario y sitio
      LAG(DS,1)OVER(PARTITION BY SIT_SITE_ID,USER_ID ORDER BY START_PLAY_TIMESTAMP ASC) AS DS_ANT,
      -- Clasifica a los usuarios como 'NEW' (primer play), 'RETAINED' (play dentro de 30 días del anterior) o 'RECOVERED' (play después de 30 días del anterior)
      (CASE WHEN (LAG(DS,1)OVER(PARTITION BY SIT_SITE_ID,USER_ID ORDER BY START_PLAY_TIMESTAMP ASC)) IS NULL THEN 'NEW'
              WHEN DATE_DIFF(DS, (LAG(DS,1)OVER(PARTITION BY SIT_SITE_ID,USER_ID ORDER BY START_PLAY_TIMESTAMP ASC)), DAY) <= 30 THEN 'RETAINED'
              WHEN DATE_DIFF(DS, (LAG(DS,1)OVER(PARTITION BY SIT_SITE_ID,USER_ID ORDER BY START_PLAY_TIMESTAMP ASC)), DAY) > 30  THEN 'RECOVERED'
              ELSE NULL END) AS FLAG_N_R,
      -- Obtiene el primer día de actividad del usuario
      MIN(DS) OVER (PARTITION BY SIT_SITE_ID,USER_ID) AS FIRST_DS_USER
    FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS`
    -- Considera solo los plays de 20 segundos o más, y hasta el día anterior
    WHERE  PLAYBACK_TIME_MILLISECONDS/1000 >= 20
      AND DS <= CURRENT_DATE-1
  ),
  -- CTE para atribuir el primer play del marco de tiempo
  ATTR_TIME_FRAME_ELEGIDO AS (
          SELECT
          SIT_SITE_ID,
          USER_ID,
          TIME_FRAME_ID,
          FLAG_N_R,
          DS AS FECHA_FLAG_N_R,
          CONTENT_ID AS CONTENT_FLAG_N_R
          FROM NEW_RET_RECO
          -- Se queda con el primer play del usuario dentro de cada marco de tiempo para fines de atribución
          QUALIFY ROW_NUMBER()  OVER(PARTITION BY SIT_SITE_ID,USER_ID,TIME_FRAME_ID
                                         ORDER BY START_PLAY_TIMESTAMP ASC) =  1 --> ME QUEDO CON EL PRIMER PLAY DEL TIMEFRAME PARA ATRIBUIR 1
  ),
  -- CTE para cruzar la información de bandera y el Aha Moment
  CRUCE_FLAG AS (
          SELECT
          A.*,
          E.FECHA_FLAG_N_R,
          E.FLAG_N_R AS FLAG_N_R_FINAL,
          E.CONTENT_FLAG_N_R,
          -- Cuenta los días distintos en los que el usuario tuvo actividad en los primeros 30 días de uso (para el Aha Moment)
          COUNT(DISTINCT CASE WHEN DS BETWEEN FIRST_DS_USER AND FIRST_DS_USER+29
                            THEN DS ELSE NULL END) OVER (PARTITION BY A.SIT_SITE_ID,A.USER_ID) AS DS_AHA_MOMENT

          FROM NEW_RET_RECO AS A
          LEFT JOIN ATTR_TIME_FRAME_ELEGIDO AS E ON E.SIT_SITE_ID = A.SIT_SITE_ID
                                                AND E.USER_ID = A.USER_ID
                                                AND E.TIME_FRAME_ID = A.TIME_FRAME_ID
  )
-- Consulta final para crear la tabla NEGOCIO_MPLAY_CONTENT_DATA_USER
SELECT
        A.SIT_SITE_ID,
        A.USER_ID,
        A.TIME_FRAME_ID,
        A.FECHA_FLAG_N_R,
        A.FLAG_N_R_FINAL,
        A.DS_AHA_MOMENT,
        A.CONTENT_FLAG_N_R,
        A.CONTENT_ID,
        -- Ajusta el CONTENT_TYPE a 'SHOW' si es 'EPISODE'
        CASE WHEN C1.CONTENT_TYPE = 'EPISODE' THEN 'SHOW' ELSE C1.CONTENT_TYPE END AS CONTENT_TYPE,
        C1.TITLE_ADJUSTED,
        C1.ORIGINAL_TITLE,
        C1.SEASON_NUMBER,
        C1.EPISODE_NUMBER,

-- AJUSTAMOS LOS TMVS ACUMULADOS AL RUNTIME DISPONIBLE PARA EVITAR OUTLIERS
        -- Limita el TVM total del marco de tiempo al runtime del contenido para evitar outliers
        CASE WHEN TVM_TOTAL_TIMEFRAME <= SAFE_CAST(RTRIM(C1.RUNTIME, 's') AS INT64)/60
             THEN TVM_TOTAL_TIMEFRAME
             ELSE SAFE_CAST(RTRIM(C1.RUNTIME, 's') AS INT64)/60 END AS TVM_TOTAL_TIMEFRAME ,
        -- Limita el TVM acumulado por CONTENT_ID al runtime del contenido para evitar outliers
        CASE WHEN SUM(TVM_TOTAL_TIMEFRAME)OVER(PARTITION BY A.SIT_SITE_ID,A.USER_ID,A.CONTENT_ID
                                                   ORDER BY A.TIME_FRAME_ID ASC)
                  <= SAFE_CAST(RTRIM(C1.RUNTIME, 's') AS INT64)/60
            THEN  SUM(TVM_TOTAL_TIMEFRAME)OVER(PARTITION BY A.SIT_SITE_ID,A.USER_ID,A.CONTENT_ID
                                                   ORDER BY A.TIME_FRAME_ID ASC)
            ELSE  SAFE_CAST(RTRIM(C1.RUNTIME, 's') AS INT64)/60  END  AS TVM_ACUMULADO_CONTENT_ID,
        -- Runtime del contenido en minutos
        SAFE_CAST(RTRIM(C1.RUNTIME, 's') AS INT64)/60 AS RUNTIME_CONTENT,
        TOTAL_TV,
        TOTAL_MOBILE,
        TOTAL_DESKTOP
FROM
(
        SELECT
        SIT_SITE_ID,
        USER_ID,
        TIME_FRAME_ID,
        FECHA_FLAG_N_R,
        FLAG_N_R_FINAL,
        DS_AHA_MOMENT,
        CONTENT_FLAG_N_R,
        CONTENT_ID,
        -- Suma el tiempo total visto en minutos para el marco de tiempo
        SUM(PLAYBACK_TIME_MILLISECONDS/60000) AS TVM_TOTAL_TIMEFRAME,
        -- Suma el tiempo visto en TV
        SUM(CASE WHEN UPPER(DEVICE_PLATFORM) LIKE '%TV%' THEN
                   PLAYBACK_TIME_MILLISECONDS/60000 ELSE 0 END) AS TOTAL_TV,
        -- Suma el tiempo visto en dispositivos móviles
        SUM(CASE WHEN UPPER(DEVICE_PLATFORM) LIKE '%MOBILE%' THEN
                   PLAYBACK_TIME_MILLISECONDS/60000 ELSE 0 END) AS TOTAL_MOBILE,
        -- Suma el tiempo visto en dispositivos de escritorio
        SUM(CASE WHEN UPPER(DEVICE_PLATFORM) LIKE '%DESK%' THEN
                   PLAYBACK_TIME_MILLISECONDS/60000 ELSE 0 END) AS TOTAL_DESKTOP
        FROM CRUCE_FLAG
        GROUP BY ALL
        )AS A
        -- LEFT JOIN con la tabla de catálogo de contenido para obtener detalles del contenido
        LEFT JOIN `meli-bi-data.WHOWNER.LK_MKT_MPLAY_CATALOGUE` AS C1 ON A.SIT_SITE_ID = C1.SIT_SITE_ID
                                                                     AND A.CONTENT_ID = C1.CONTENT_ID
)
;
CREATE OR REPLACE TABLE `meli-sbox.MPLAY.NEGOCIO_MPLAY_CONTENT_DATA_USER_RESUME` AS (
-- Tabla de resumen de datos de contenido por usuario
WITH ESTADISTICAS_TIMEFRAME AS (
        -- Calcula estadísticas de TVM por tipo de contenido para cada marco de tiempo y sitio
        SELECT
        A.USER_ID,
        A.TIME_FRAME_ID,
        -- Obtiene el siguiente marco de tiempo para el usuario y sitio
        LEAD(A.TIME_FRAME_ID,1)OVER(PARTITION BY USER_ID,SIT_SITE_ID ORDER BY TIME_FRAME_ID) AS TIME_FRAME_SIG,
        A.SIT_SITE_ID,
        -- Clasifica el engagement del usuario basado en si su TVM de SHOWS o MOVIES está por encima del percentil 80
        CASE WHEN TVM_SHOWS >= PERCENTILE_CONT(TVM_SHOWS,0.80)OVER(PARTITION BY TIME_FRAME_ID,SIT_SITE_ID)
             THEN 'GREATER'
             WHEN TVM_MOVIE >= PERCENTILE_CONT(TVM_MOVIE,0.80)OVER(PARTITION BY TIME_FRAME_ID,SIT_SITE_ID)
             THEN 'GREATER'
             ELSE 'LEAST' END AS FLAG_ENGAGEMENT_PERCENTILE
        FROM
        (SELECT
            TIME_FRAME_ID,
            SIT_SITE_ID,
            USER_ID,
            SUM(TVM_TOTAL_TIMEFRAME) AS TVM,
            -- Suma el TVM para contenido tipo 'SHOW'
            SUM(CASE WHEN CONTENT_TYPE = 'SHOW' THEN TVM_TOTAL_TIMEFRAME ELSE 0 END ) AS TVM_SHOWS,
            -- Suma el TVM para contenido tipo 'MOVIE'
            SUM(CASE WHEN CONTENT_TYPE = 'MOVIE' THEN TVM_TOTAL_TIMEFRAME ELSE 0 END ) AS TVM_MOVIE
            FROM `meli-sbox.MPLAY.NEGOCIO_MPLAY_CONTENT_DATA_USER`
            GROUP BY ALL) AS A
  )

  SELECT
  A.TIME_FRAME_ID,
  A.FLAG_N_R_FINAL,
  A.SIT_SITE_ID,
  -- Indica si el contenido que inició el marco de tiempo es el mismo que se está analizando
  CASE WHEN A.CONTENT_FLAG_N_R = A.CONTENT_ID THEN 'CONTENT_FLAG_NR' ELSE 'CONTINUOS_CONTENT' END AS FLAG_CONTENT_NR,
  -- Indica si el usuario está logueado o no
  CASE WHEN SAFE_CAST(A.USER_ID AS INT64) IS NULL THEN 'NOT_LOG' ELSE 'LOG' END AS FLAG_LOG,
  -- Clasifica el TVM total del marco de tiempo en rangos (menor a 3 min, entre 3 y 10, entre 10 y 30, mayor a 30)
  CASE WHEN A.TVM_TOTAL_TIMEFRAME < 3 THEN 'A. MENOR A 3 MIN'
       WHEN A.TVM_TOTAL_TIMEFRAME BETWEEN 3 AND 10 THEN 'B. ENTRE 3 Y 10 MIN'
       WHEN A.TVM_TOTAL_TIMEFRAME BETWEEN 10 AND 30 THEN 'C. ENTRE 10 Y 30 MIN'
       ELSE 'D. MAYOR A 30 MIN' END AS RANGE_TVM_TIMEFRAME,
  -- Indica si el usuario alcanzó el 'Aha Moment' (3 días de play en 30 días)
  CASE WHEN A.DS_AHA_MOMENT >= 3 THEN 'AHA_MOMENT' ELSE 'NOT_AHA' END AS FLAG_AHA_MOMENT,
  E.FLAG_ENGAGEMENT_PERCENTILE,
  -- Concatena las plataformas usadas (SMART TV, MOBILE, DESKTOP)
  CONCAT( CASE WHEN A.TOTAL_TV > 0 THEN 'SMART' ELSE '' END ,' - ',
          CASE WHEN A.TOTAL_MOBILE > 0 THEN 'MOBILE' ELSE '' END ,' - ',
          CASE WHEN A.TOTAL_DESKTOP > 0 THEN 'DESKTOP' ELSE '' END ) AS PLATFORM_CONCAT,

  CONTENT_TYPE,
  TITLE_ADJUSTED,
  ORIGINAL_TITLE,
  SEASON_NUMBER,
  EPISODE_NUMBER,
  -- Suma el TVM del marco de tiempo
  SUM(A.TVM_TOTAL_TIMEFRAME) AS TVM_TIME_FRAME,
  -- Cuenta el total de usuarios únicos
  COUNT(DISTINCT A.USER_ID) AS TOTAL_USERS,
  -- Cuenta los usuarios que se retienen al mes siguiente
  COUNT(DISTINCT CASE WHEN E.TIME_FRAME_SIG = DATE_ADD(A.TIME_FRAME_ID,INTERVAL 1 MONTH) THEN A.USER_ID ELSE NULL END) AS TOTAL_USERS_RET_MONTHS_SIG,
  -- Suma el TVM acumulado por contenido
  SUM(A.TVM_ACUMULADO_CONTENT_ID) AS TVM_ACUMULADO,
  -- Suma el runtime total del contenido en el mes
  SUM(A.RUNTIME_CONTENT) AS TOTAL_RUNTIME_MONTH

FROM `meli-sbox.MPLAY.NEGOCIO_MPLAY_CONTENT_DATA_USER` AS A
        -- LEFT JOIN con ESTADISTICAS_TIMEFRAME para traer el flag de engagement
        LEFT JOIN ESTADISTICAS_TIMEFRAME AS E ON E.USER_ID = A.USER_ID
                                             AND E.SIT_SITE_ID  = A.SIT_SITE_ID
                                             AND E.TIME_FRAME_ID = A.TIME_FRAME_ID
GROUP BY ALL
)
;
--meli-sbox.MPLAY.MOVIE_EMBEDIDINGS

CREATE OR REPLACE TABLE `meli-sbox.MPLAY.NEGOCIO_MPLAY_CONTENT_PERFORMANCE_SCORES_V2`
PARTITION BY TIME_FRAME_ID
CLUSTER BY SIT_SITE_ID, CONTENT_TYPE AS (
-- CTE para calcular métricas de contenido
WITH ContentMetrics AS (
    SELECT
        TIME_FRAME_ID,
        SIT_SITE_ID,
        CONTENT_TYPE AS CONTENT_TYPE,
        TITLE_ADJUSTED AS TITLE_ADJUSTED,
        -- Usuarios de adquisición: nuevos usuarios cuyo primer contenido visto es este
        SUM(CASE WHEN FLAG_N_R_FINAL = 'NEW'
                  AND FLAG_CONTENT_NR = 'CONTENT_FLAG_NR'
                 THEN TOTAL_USERS ELSE 0 END) AS ACQUSITION_USERS,
        -- Usuarios de adquisición con 'Aha Moment'
        SUM( CASE WHEN FLAG_N_R_FINAL = 'NEW'
                   AND FLAG_CONTENT_NR = 'CONTENT_FLAG_NR'
                   AND FLAG_AHA_MOMENT = 'AHA_MOMENT'
                 THEN TOTAL_USERS ELSE 0 END) AS ACQUISITION_AHA_USERS,
        -- Total de usuarios que interactuaron con el contenido
        SUM(TOTAL_USERS) AS TOTAL_USERS,
        -- Total de usuarios retenidos al mes siguiente
        SUM(TOTAL_USERS_RET_MONTHS_SIG) AS TOTAL_USERS_RET,
        -- Total de usuarios en el percentil 80 de engagement
        SUM(CASE WHEN FLAG_ENGAGEMENT_PERCENTILE = 'GREATER' THEN TOTAL_USERS ELSE 0 END) AS PERCENT_80_USERS,
        -- Usuarios recuperados cuyo primer contenido visto es este
        SUM(CASE WHEN FLAG_N_R_FINAL = 'RECOVERED'
                  AND FLAG_CONTENT_NR = 'CONTENT_FLAG_NR'
                 THEN TOTAL_USERS ELSE 0 END) AS RECOVER_USERS,
        -- Tiempo total visto en minutos
        SUM(TVM_TIME_FRAME) AS TOTAL_TVM,
        -- TVM acumulado para el contenido en el marco de tiempo
        SUM(TVM_ACUMULADO) AS TVM_AUCMULADO_TIMEFRANE,
        -- Runtime total acumulado del contenido en el mes
        SUM(TOTAL_RUNTIME_MONTH) AS TOTAL_RUNTIME_ACUMULADO
    FROM `meli-sbox.MPLAY.NEGOCIO_MPLAY_CONTENT_DATA_USER_RESUME`
    GROUP BY ALL
),
GlobalMetrics AS (
    -- Calculamos los totales globales necesarios para la normalización de los scores
    SELECT
        TIME_FRAME_ID,
        SIT_SITE_ID,
        -- Total de nuevos usuarios en el marco de tiempo
        SUM( CASE WHEN FLAG_N_R_FINAL = 'NEW' AND FLAG_CONTENT_NR = 'CONTENT_FLAG_NR' THEN TOTAL_USERS ELSE 0 END) AS TOTAL_NEW_USERS_TIMEFRAME,
        -- Total de usuarios recuperados en el marco de tiempo
        SUM( CASE WHEN FLAG_N_R_FINAL = 'RECOVERED' AND FLAG_CONTENT_NR = 'CONTENT_FLAG_NR' THEN TOTAL_USERS ELSE 0 END) AS TOTAL_RECOVERED_USERS_TIMEFRAME,
        -- Total de minutos vistos a nivel global para el timeframe
        SUM(TVM_TIME_FRAME) AS TOTAL_TVM_TIMEFRAME,
        -- Total de usuarios únicos en el marco de tiempo
        SUM(CASE WHEN FLAG_CONTENT_NR = 'CONTENT_FLAG_NR' THEN TOTAL_USERS ELSE 0 END ) TOTAL_USERS_TIMEFRAME
    FROM `meli-sbox.MPLAY.NEGOCIO_MPLAY_CONTENT_DATA_USER_RESUME`
    GROUP BY ALL
)
-- Consulta principal para crear la tabla de SCORES de rendimiento de contenido V2 (sin ponderar aún)
SELECT
    cm.TIME_FRAME_ID,
    cm.SIT_SITE_ID,
    cm.CONTENT_TYPE,
    cm.TITLE_ADJUSTED,
    cm.ACQUSITION_USERS,
    cm.ACQUISITION_AHA_USERS,
    cm.TOTAL_USERS,
    cm.TOTAL_USERS_RET,
    cm.PERCENT_80_USERS,
    cm.RECOVER_USERS,
    gm.TOTAL_USERS_TIMEFRAME,
    gm.TOTAL_NEW_USERS_TIMEFRAME,
    gm.TOTAL_RECOVERED_USERS_TIMEFRAME,
    cm.TOTAL_TVM,
    gm.TOTAL_TVM_TIMEFRAME,
    cm.TVM_AUCMULADO_TIMEFRANE,
    cm.TOTAL_RUNTIME_ACUMULADO,
    -- Score de Volumen de Adquisición: Usuarios de adquisición sobre el total de nuevos usuarios
    COALESCE(SAFE_DIVIDE(cm.ACQUSITION_USERS, gm.TOTAL_NEW_USERS_TIMEFRAME), 0) AS Acquisition_Volume_Score,
    -- Score de Calidad de Adquisición: Usuarios AHA sobre usuarios de adquisición
    COALESCE(SAFE_DIVIDE(cm.ACQUISITION_AHA_USERS, cm.ACQUSITION_USERS), 0) AS Acquisition_Quality_Score,
    -- Score de Retención: Usuarios retenidos sobre el total de usuarios
    COALESCE(SAFE_DIVIDE(cm.TOTAL_USERS_RET, cm.TOTAL_USERS), 0) AS Retention_Score,
    -- Score de Recuperación: Usuarios recuperados sobre el total de usuarios recuperados
    COALESCE(SAFE_DIVIDE(cm.RECOVER_USERS, gm.TOTAL_RECOVERED_USERS_TIMEFRAME), 0) AS Recovery_Score,
    -- Score de Popularidad TVM: TVM del contenido sobre el TVM global
    COALESCE(SAFE_DIVIDE(cm.TOTAL_TVM, gm.TOTAL_TVM_TIMEFRAME), 0) AS TVM_Popularity_Score,
    -- Score de Engagement: Usuarios en el percentil 80 de engagement sobre el total de usuarios
    COALESCE(SAFE_DIVIDE(cm.PERCENT_80_USERS, cm.TOTAL_USERS), 0) AS Engagement_Score,
    -- Score de Tasa de Completado: TVM acumulado sobre el runtime acumulado
    COALESCE(SAFE_DIVIDE(cm.TVM_AUCMULADO_TIMEFRANE, cm.TOTAL_RUNTIME_ACUMULADO), 0) AS Completion_Rate_Score,
    -- Score de Popularidad de Usuarios: Usuarios del contenido sobre el total de usuarios
    COALESCE(SAFE_DIVIDE(cm.TOTAL_USERS, gm.TOTAL_USERS_TIMEFRAME),0) as USERS_Popularity_Score

FROM ContentMetrics AS cm
LEFT JOIN GlobalMetrics AS gm     ON cm.TIME_FRAME_ID = gm.TIME_FRAME_ID
                                 AND cm.SIT_SITE_ID = gm.SIT_SITE_ID
-- Filtra para incluir solo contenidos con más de 0 usuarios
WHERE cm.TOTAL_USERS > 0
)
;
CREATE OR REPLACE TABLE `meli-sbox.MPLAY.NEGOCIO_MPLAY_CONTENT_PERFORMANCE_SCORES_V2`
PARTITION BY TIME_FRAME_ID
CLUSTER BY SIT_SITE_ID, CONTENT_TYPE AS (
-- Esta tabla reemplaza la anterior para calcular SCORES de rendimiento de contenido V2 PONDERADOS
WITH ScoreDeviations AS (
    -- Calcula la desviación estándar de cada score por marco de tiempo, sitio y tipo de contenido
    SELECT
        TIME_FRAME_ID,
        SIT_SITE_ID,
        CONTENT_TYPE,
        COALESCE(STDDEV(Acquisition_Volume_Score), 0) AS std_dev_acquisition_volume,
        COALESCE(STDDEV(Acquisition_Quality_Score), 0) AS std_dev_acquisition_quality,
        COALESCE(STDDEV(Retention_Score), 0) AS std_dev_retention,
        COALESCE(STDDEV(Recovery_Score), 0) AS std_dev_recovery,
        COALESCE(STDDEV(TVM_Popularity_Score), 0) AS std_dev_tvm_popularity,
        COALESCE(STDDEV(Engagement_Score), 0) AS std_dev_engagement,
        COALESCE(STDDEV(Completion_Rate_Score), 0) AS std_dev_completion_rate,
        COALESCE(STDDEV(USERS_Popularity_Score), 0) AS std_dev_users_popularity
    FROM `meli-sbox.MPLAY.NEGOCIO_MPLAY_CONTENT_PERFORMANCE_SCORES_V2`
    GROUP BY ALL
),
TotalStdDevs AS (
    -- Suma las desviaciones estándar para cada combinación de marco de tiempo, sitio y tipo de contenido
    SELECT
        TIME_FRAME_ID,
        SIT_SITE_ID,
        CONTENT_TYPE,
        (
            std_dev_acquisition_volume +
            std_dev_acquisition_quality +
            std_dev_retention +
            std_dev_recovery +
            std_dev_tvm_popularity +
            std_dev_engagement +
            std_dev_completion_rate +
            std_dev_users_popularity
        ) AS total_std_dev_sum
    FROM ScoreDeviations
),
FINAL_W AS (
-- Calcula los pesos finales para cada score, normalizando por la suma total de desviaciones estándar
SELECT
    sd.TIME_FRAME_ID,
    sd.SIT_SITE_ID,
    sd.CONTENT_TYPE,
    SAFE_DIVIDE(sd.std_dev_acquisition_volume   , tsd.total_std_dev_sum) AS weight_acquisition_volume,
    SAFE_DIVIDE(sd.std_dev_acquisition_quality  , tsd.total_std_dev_sum) AS weight_acquisition_quality,
    SAFE_DIVIDE(sd.std_dev_retention            , tsd.total_std_dev_sum) AS weight_retention,
    SAFE_DIVIDE(sd.std_dev_recovery             , tsd.total_std_dev_sum) AS weight_recovery,
    SAFE_DIVIDE(sd.std_dev_tvm_popularity       , tsd.total_std_dev_sum) AS weight_tvm_popularity,
    SAFE_DIVIDE(sd.std_dev_engagement           , tsd.total_std_dev_sum) AS weight_engagement,
    SAFE_DIVIDE(sd.std_dev_completion_rate      , tsd.total_std_dev_sum) AS weight_completion_rate,
    SAFE_DIVIDE(sd.std_dev_users_popularity     , tsd.total_std_dev_sum) AS weight_users_popularity
FROM ScoreDeviations sd
JOIN TotalStdDevs tsd ON sd.TIME_FRAME_ID = tsd.TIME_FRAME_ID
                     AND sd.SIT_SITE_ID = tsd.SIT_SITE_ID
                     AND sd.CONTENT_TYPE = tsd.CONTENT_TYPE
-- Solo considera casos donde la suma total de desviaciones estándar es mayor que 0 para evitar divisiones por cero
WHERE tsd.total_std_dev_sum > 0
)

--> ver qe hacer con el ratio de retention para meses qe no se cerraron (comentario de línea original)

-- Consulta final para crear la tabla de SCORES de rendimiento de contenido V2 con SCORES PONDERADOS
SELECT
A.*,
-- Crea 10 clústeres de usuarios basándose en el TOTAL_USERS por marco de tiempo y sitio
NTILE(10)OVER(PARTITION BY A.TIME_FRAME_ID,A.SIT_SITE_ID ORDER BY A.TOTAL_USERS ASC) AS CLUSTERS_USERS,
-- Calcula el SCORE FINAL como la suma ponderada de todos los scores individuales
COALESCE(Acquisition_Volume_Score   * weight_acquisition_volume , 0) +
COALESCE(Acquisition_Quality_Score  * weight_acquisition_quality, 0) +
COALESCE(Retention_Score            * weight_retention , 0) +
COALESCE(Recovery_Score             * weight_recovery, 0) +
COALESCE(TVM_Popularity_Score       * weight_tvm_popularity , 0) +
COALESCE(Engagement_Score           * weight_engagement, 0) +
COALESCE(Completion_Rate_Score      * weight_completion_rate, 0) +
COALESCE(USERS_Popularity_Score     * weight_users_popularity , 0) AS SCORE_FINAL

FROM `meli-sbox.MPLAY.NEGOCIO_MPLAY_CONTENT_PERFORMANCE_SCORES_V2` AS A
    -- LEFT JOIN con FINAL_W para obtener los pesos de ponderación calculados
    LEFT JOIN FINAL_W AS F ON F.SIT_SITE_ID = A.SIT_SITE_ID
                          AND F.CONTENT_TYPE = A.CONTENT_TYPE
                          AND F.TIME_FRAME_ID = A.TIME_FRAME_ID
)
;

CREATE OR REPLACE TABLE  `meli-sbox.MPLAY.NEGOCIO_MPLAY_CONTENT_PERFORMANCE_SCORES_FINAL` AS (
-- Esta tabla final clasifica el contenido basándose en sus scores de rendimiento
SELECT
A.*,
-- Clasifica el contenido como 'HIGH_SCORE', 'MED_SCORE' o 'LOW_SCORE' basado en el percentil de SCORE_FINAL
CASE WHEN SCORE_FINAL >= PERCENTILE_CONT(SCORE_FINAL,0.80) OVER(PARTITION BY A.SIT_SITE_ID,A.TIME_FRAME_ID,A.CONTENT_TYPE)
     THEN 'HIGH_SCORE'
     WHEN SCORE_FINAL >= PERCENTILE_CONT(SCORE_FINAL,0.50) OVER(PARTITION BY A.SIT_SITE_ID,A.TIME_FRAME_ID,A.CONTENT_TYPE)
     THEN 'MED_SCORE'
     ELSE 'LOW_SCORE' END AS FLAG_SOCORE_FINAL,
-- Clasifica el rendimiento de adquisición como 'HIGH_ACQUISITION', 'MED_ACQUISITION' o 'LOW_ACQUISITION'
CASE WHEN Acquisition_Volume_Score >= PERCENTILE_CONT(Acquisition_Volume_Score,0.80) OVER(PARTITION BY A.SIT_SITE_ID,A.TIME_FRAME_ID,A.CONTENT_TYPE)
      AND Acquisition_Quality_Score >= PERCENTILE_CONT(Acquisition_Quality_Score,0.80) OVER(PARTITION BY A.SIT_SITE_ID,A.TIME_FRAME_ID,A.CONTENT_TYPE)
     THEN 'HIGH_ACQUISITION'
     WHEN Acquisition_Volume_Score >= PERCENTILE_CONT(Acquisition_Volume_Score,0.50) OVER(PARTITION BY A.SIT_SITE_ID,A.TIME_FRAME_ID,A.CONTENT_TYPE)
      AND Acquisition_Quality_Score >= PERCENTILE_CONT(Acquisition_Quality_Score,0.50) OVER(PARTITION BY A.SIT_SITE_ID,A.TIME_FRAME_ID,A.CONTENT_TYPE)
     THEN 'MED_ACQUISITION'
     ELSE 'LOW_ACQUISITION' END AS FLAG_ACQUISITIION_SCORE,
-- Clasifica el engagement como 'HIGH_ENGAGEMENT', 'MED_ENGAGEMENT' o 'LOW_ENGAGEMENT'
CASE WHEN Engagement_Score >= PERCENTILE_CONT(Engagement_Score,0.80) OVER(PARTITION BY A.SIT_SITE_ID,A.TIME_FRAME_ID,A.CONTENT_TYPE)
     THEN 'HIGH_ENGAGEMENT'
     WHEN Engagement_Score >= PERCENTILE_CONT(Engagement_Score,0.50) OVER(PARTITION BY A.SIT_SITE_ID,A.TIME_FRAME_ID,A.CONTENT_TYPE)
     THEN 'MED_ENGAGEMENT'
     ELSE 'LOW_ENGAGEMENT' END AS FLAG_ENGAGEMENT,
FROM `meli-sbox.MPLAY.NEGOCIO_MPLAY_CONTENT_PERFORMANCE_SCORES_V2` AS A
)