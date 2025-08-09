-- Declara la variable FECHA_FROM
DECLARE FECHA_FROM DATE;
-- Borra de MPLAY_NEGOCIO_ATT_NEW_USERS todo registro de los últimos 9 dias
DELETE FROM `meli-sbox.MPLAY.MPLAY_NEGOCIO_ATT_NEW_USERS`
WHERE FECHA_CONV BETWEEN CURRENT_DATE-8 AND CURRENT_DATE;
-- Seteo FECHA_FROM para que sea el dia siguiente a la ultima FECHA_CONV en la tabla MPLAY_NEGOCIO_ATT_NEW_USERS
SET FECHA_FROM = (SELECT MAX(FECHA_CONV) FROM  `meli-sbox.MPLAY.MPLAY_NEGOCIO_ATT_NEW_USERS`) + 1;

-- Se insertan los datos dentro de MPLAY_NEGOCIO_ATT_NEW_USERS
INSERT INTO  `meli-sbox.MPLAY.MPLAY_NEGOCIO_ATT_NEW_USERS` (
  -- En la CTE CONV se traen los datos de la tabla BT_MKT_MPLAY_PLAYS siempre que la reproduccion sea mayor a 20seg trayendo una fila por cada combinación SIT_SITE_ID y USER_ID trayendo la primer fecha ordenando por START_PLAY_TIMESAMP siempre que la fecha CONV esté entre la FECHA_FROM y CURRENT_DATE - 1
  WITH CONV AS (
    SELECT 
      *
      FROM (
        SELECT
          P.USER_ID,
          P.SIT_SITE_ID,
          P.DS AS FECHA_CONV,
          DEVICE_PLATFORM
        FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` AS P
        WHERE P.PLAYBACK_TIME_MILLISECONDS/1000 >= 20
        QUALIFY ROW_NUMBER()OVER(PARTITION BY SIT_SITE_ID,USER_ID ORDER BY P.START_PLAY_TIMESTAMP ASC) = 1
      )
    WHERE FECHA_CONV BETWEEN FECHA_FROM AND CURRENT_DATE-1
  ),
  -- Se unen los datos de CONV con datos puntuales de la sesiones
  ATT_ORIGINS AS (
    SELECT DISTINCT
      C.USER_ID,
      C.SIT_SITE_ID,
      C.FECHA_CONV,
      C.DEVICE_PLATFORM,
      S.ORIGIN_PATH,
      S.DS AS FECHA_ESTIMULOS,
      -- DIAS_TO_CONV sera la diferencia entre la fecha de conversión y la fecha de la sesión
      DATE_DIFF(FECHA_CONV,S.DS,DAY) AS DIAS_TO_CONV      
    FROM CONV AS C
    -- Se hace el join con BT_MKT_MPLAY_SESSION
    LEFT JOIN  `meli-bi-data.WHOWNER.BT_MKT_MPLAY_SESSION` AS S 
      ON S.USER_ID = C.USER_ID
      AND S.SIT_SITE_ID = C.SIT_SITE_ID
      -- Con estos filtros logramos traer las sesiones en un rango de 30 dias respecto FECHA_CONV
      AND S.DS BETWEEN C.FECHA_CONV-29 AND C.FECHA_CONV
      AND S.DS >= FECHA_FROM-5-30
  ),
  -- CTE que será de BASE
  BASE_CALCULOS AS (
  SELECT 
    *,
    DENSE_RANK()OVER(PARTITION BY USER_ID,SIT_SITE_ID ORDER BY DIAS_TO_CONV DESC) AS ORDER_DESC_TIME --> USAMOS DENSE PARA PONERLE EL MISMO PESO AL DIA
  FROM ATT_ORIGINS
  )
  -- Consulta final para la inserción de datos en MPLAY_NEGOCIO_ATT_NEW_USERS
  SELECT 
    *,
    -- Sirve para dar un peso de conversión con ORDER_DESC_TIME con DIAS_TO_CONV reciben un peso mayor
    POW(2,ORDER_DESC_TIME)/(SUM(POW(2,ORDER_DESC_TIME))OVER(PARTITION BY USER_ID,SIT_SITE_ID)) AS CONVERTION_W,
    0 AS RET_1_30,
    0 AS RET_31_60,
    0 AS AHA_MOMENT,
  FROM BASE_CALCULOS
);

CREATE OR REPLACE TABLE `meli-sbox.MPLAY.MPLAY_NEGOCIO_ADQUITSTION_METRICS` AS (
  -- CTE para calcular las conversiones por sitio, origen y fecha
  WITH SOURCE_CONVERTION AS (
    SELECT
      SIT_SITE_ID,
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
  -- COALESCE(SC.DEVICE_PLATFORM AS PLATFORM_CONVERTION,
    COALESCE(OM.SOURCE_SESSION_L1,'NULO') AS NEGOCIO,
    COALESCE(OM.TEAM,'NULO') AS TEAM,
    COALESCE(OM.SOURCE_SESSION_L2,'NULO') AS ORIGIN_PATH,
    S.FECHA_ESTIMULOS,
    S.SIT_SITE_ID,
    S.TOTAL_TRAFICO,
    S.TOTAL_TRAFICO_VISITOR,
    SC.CONVERTIONS
  FROM TOTAL_SOURCE_TRAFICO AS S
  -- LEFT JOIN con SOURCE_CONVERTION para combinar el tráfico con las conversiones
  LEFT JOIN SOURCE_CONVERTION AS SC
    ON SC.SIT_SITE_ID = S.SIT_SITE_ID
    AND SC.ORIGIN_PATH = S.ORIGIN_PATH
    AND SC.FECHA_ESTIMULOS = S.FECHA_ESTIMULOS
  -- LEFT JOIN con la tabla de lookup de tipos de origen de sesión para obtener las dimensiones de negocio y equipo
  LEFT JOIN `meli-sbox.MPLAY.LK_MPLAY_SOURCE_TYPE_ORIGIN_SESSION` AS OM
    ON OM.SOURCE_TYPE = S.ORIGIN_PATH
);

CREATE OR REPLACE TABLE `meli-sbox.MPLAY.MPLAY_NEGOCIO_ADQUITSTION_METRICS` AS (
  -- CTE para calcular las conversiones por sitio, origen y fecha
  WITH SOURCE_CONVERTION AS (
    SELECT
      SIT_SITE_ID,
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
  -- COALESCE(SC.DEVICE_PLATFORM AS PLATFORM_CONVERTION, 
    COALESCE(OM.SOURCE_SESSION_L1,'NULO') AS NEGOCIO,
    COALESCE(OM.TEAM,'NULO') AS TEAM,
    COALESCE(OM.SOURCE_SESSION_L2,'NULO') AS ORIGIN_PATH,
    S.FECHA_ESTIMULOS,
    S.SIT_SITE_ID,
    S.TOTAL_TRAFICO,
    S.TOTAL_TRAFICO_VISITOR,
    SC.CONVERTIONS
  FROM TOTAL_SOURCE_TRAFICO AS S
  -- LEFT JOIN con SOURCE_CONVERTION para combinar el tráfico con las conversiones
  LEFT JOIN SOURCE_CONVERTION AS SC
    ON SC.SIT_SITE_ID = S.SIT_SITE_ID
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

CREATE OR REPLACE TABLE `meli-sbox.MPLAY.MPLAY_NEGOCIO_COHORTS_DASH` AS (
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
      -- LEFT JOIN con PLAY_DAYS para obtener el tiempo de reproducción dentro del período de la cohorte
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

CREATE OR REPLACE TABLE meli-sbox.MPLAY.MPLAY_NEGOCIO_ATT_AHA_MOMENT_USERS AS (
  -- CTE para identificar a los usuarios que alcanzaron el 'Aha Moment'
  WITH CONV AS (
    SELECT
      A.USER_ID,
      A.SIT_SITE_ID,
      A.DS AS FECHA_CUMPLE_AHA,
      A.FIRST_DS_USER,
      A.DEVICE_PLATFORM
    FROM (
      SELECT DISTINCT
        P.USER_ID,
        P.SIT_SITE_ID,
        P.DS,
        -- Encuentra la primera fecha de play para cada usuario
        MIN(DS) OVER (PARTITION BY SIT_SITE_ID,USER_ID) AS FIRST_DS_USER,
        -- Obtiene la plataforma del dispositivo del primer play
        FIRST_VALUE(DEVICE_PLATFORM)OVER(PARTITION BY SIT_SITE_ID,USER_ID ORDER BY START_PLAY_TIMESTAMP ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS DEVICE_PLATFORM,
      FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` AS P
      -- Considera solo plays de 20 segundos o más
      WHERE P.PLAYBACK_TIME_MILLISECONDS/1000 >= 20
      ) AS A
    -- Filtra los plays dentro de los primeros 30 días desde el primer play del usuario
    WHERE A.DS BETWEEN FIRST_DS_USER AND FIRST_DS_USER+29 --> PRIMEROS 30 DIAS
    -- Califica para traer solo la fecha en la que el usuario alcanzó su tercer día de play
    QUALIFY ROW_NUMBER()OVER(PARTITION BY SIT_SITE_ID,USER_ID ORDER BY DS ASC ) = 3 --> LLEGO A 3 DIAS
  ),
  -- CTE para atribuir el 'Aha Moment' a los orígenes de sesión
  ATT_ORIGINS AS (
    SELECT DISTINCT
      C.USER_ID,
      C.SIT_SITE_ID,
      C.FIRST_DS_USER,
      C.FECHA_CUMPLE_AHA,
      C.DEVICE_PLATFORM,
      S.ORIGIN_PATH,
      S.DS AS FECHA_ESTIMULOS,
      -- Calcula los días desde la sesión de origen hasta la fecha del 'Aha Moment'
      DATE_DIFF(C.FECHA_CUMPLE_AHA,S.DS,DAY) AS DIAS_TO_CONV
    FROM CONV AS C
    -- LEFT JOIN con BT_MKT_MPLAY_SESSION para encontrar las sesiones que llevaron al 'Aha Moment'
    LEFT JOIN `meli-bi-data.WHOWNER.BT_MKT_MPLAY_SESSION` AS S
      ON S.USER_ID = C.USER_ID
      AND S.SIT_SITE_ID = C.SIT_SITE_ID
      -- Filtra las sesiones que ocurrieron entre el primer play del usuario y la fecha del 'Aha Moment'
      AND S.DS BETWEEN C.FIRST_DS_USER AND C.FECHA_CUMPLE_AHA
      -- AND S.DS >= FECHA_FROM-5-30 (comentario de línea original)
  ),
  -- CTE base para los cálculos de atribución del 'Aha Moment'
  BASE_CALCULOS AS (
    SELECT
      *,
      -- Asigna un rango denso para ponderar los orígenes de sesión, donde los más cercanos al 'Aha Moment' tienen un peso mayor
      dense_rank()OVER(PARTITION BY USER_ID,SIT_SITE_ID ORDER BY DIAS_TO_CONV DESC) AS ORDER_DESC_TIME --> USAMOS DENSE PARA PONERLE EL MISMO PESO AL DIA
    FROM ATT_ORIGINS
  )
  -- Consulta final para crear la tabla MPLAY_NEGOCIO_ATT_AHA_MOMENT_USERS
  SELECT
    *,
    -- Calcula el peso de conversión para el 'Aha Moment', ponderando los orígenes según su cercanía
    POW(2,ORDER_DESC_TIME)/(SUM(POW(2,ORDER_DESC_TIME))OVER(PARTITION BY USER_ID,SIT_SITE_ID)) AS CONVERTION_W
  FROM BASE_CALCULOS
)