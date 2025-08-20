BEGIN
  -- ANALISIS DE FIDELIZACIÓN MENSUAL (HACIA ADELANTE) --
  -- Esta query analiza la retención de usuarios en un periodo mensual, clasificando a los usuarios en grupos de retención M1, M2, etc. Se comienza tomando un mes base, especificando la cantidad de meses que se quieren analizar hacia adelante y la cantidad de clasificaciones M.
  -- TABLAS --
  -- `meli-bi-data.WHOWNER.DM_MKT_MPLAY_RAW_PLAYS`: tabla de control de torre
  -- `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS`: tabla de reproducciones de Play
  
  -- Se declara la variable la cual será mi mes base para el análisis
  DECLARE start_month DATE DEFAULT DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH), MONTH);
  -- Se definen la cantidad de meses que vamos a analizar hacia adelante
  DECLARE months_to_analyze INT64 DEFAULT 12;      
  -- Esta i será la variable que se usará para iterar
  DECLARE i INT64 DEFAULT 0;
  
  -- Se declaran las variables para el mes base y los 11 meses posteriores
  DECLARE base_month DATE;
  DECLARE post_month_1 DATE;
  DECLARE post_month_2 DATE;
  DECLARE post_month_3 DATE;
  DECLARE post_month_4 DATE;
  DECLARE post_month_5 DATE;
  DECLARE post_month_6 DATE;
  DECLARE post_month_7 DATE;
  DECLARE post_month_8 DATE;
  DECLARE post_month_9 DATE;
  DECLARE post_month_10 DATE;
  DECLARE post_month_11 DATE;
  
  -- Se declaran las variables para las fechas de inicio y fin de los meses base y posteriores
  DECLARE base_month_start DATE;
  DECLARE base_month_end DATE;
  DECLARE post_month_1_start DATE;
  DECLARE post_month_1_end DATE;
  DECLARE post_month_2_start DATE;
  DECLARE post_month_2_end DATE;
  DECLARE post_month_3_start DATE;
  DECLARE post_month_3_end DATE;
  DECLARE post_month_4_start DATE;
  DECLARE post_month_4_end DATE;
  DECLARE post_month_5_start DATE;
  DECLARE post_month_5_end DATE;
  DECLARE post_month_6_start DATE;
  DECLARE post_month_6_end DATE;
  DECLARE post_month_7_start DATE;
  DECLARE post_month_7_end DATE;
  DECLARE post_month_8_start DATE;
  DECLARE post_month_8_end DATE;
  DECLARE post_month_9_start DATE;
  DECLARE post_month_9_end DATE;
  DECLARE post_month_10_start DATE;
  DECLARE post_month_10_end DATE;
  DECLARE post_month_11_start DATE;
  DECLARE post_month_11_end DATE; 
  
  -- Se crea una tabla temporal para guardar los resultados
  CREATE TEMP TABLE recovered_summary (
    base_month STRING,
    sit_site_id STRING,
    m1_recovered INT64,
    m2_recovered INT64,
    m3_recovered INT64,
    m4_recovered INT64,
    m5_recovered INT64,
    m6_recovered INT64,
    m7_recovered INT64,
    m8_recovered INT64,
    m9_recovered INT64,
    m10_recovered INT64,
    m11_recovered INT64
  );
  
  -- Se comienza el ciclo de iteración
  WHILE i < months_to_analyze DO
    -- Primero se calculan el mes base y los meses posteriores
    SET base_month = DATE_ADD(start_month, INTERVAL i MONTH);
    SET post_month_1 = DATE_ADD(base_month, INTERVAL 1 MONTH);
    SET post_month_2 = DATE_ADD(base_month, INTERVAL 2 MONTH);
    SET post_month_3 = DATE_ADD(base_month, INTERVAL 3 MONTH);
    SET post_month_4 = DATE_ADD(base_month, INTERVAL 4 MONTH);
    SET post_month_5 = DATE_ADD(base_month, INTERVAL 5 MONTH);
    SET post_month_6 = DATE_ADD(base_month, INTERVAL 6 MONTH);
    SET post_month_7 = DATE_ADD(base_month, INTERVAL 7 MONTH);
    SET post_month_8 = DATE_ADD(base_month, INTERVAL 8 MONTH);
    SET post_month_9 = DATE_ADD(base_month, INTERVAL 9 MONTH);
    SET post_month_10 = DATE_ADD(base_month, INTERVAL 10 MONTH);
    SET post_month_11 = DATE_ADD(base_month, INTERVAL 11 MONTH); 
  
    -- A partir de acá se definen las fechas de inicio y fin de cada mes
    SET base_month_start = DATE_TRUNC(base_month, MONTH);
    SET base_month_end = DATE_SUB(DATE_ADD(base_month_start, INTERVAL 1 MONTH), INTERVAL 1 DAY);
  
    SET post_month_1_start = DATE_TRUNC(post_month_1, MONTH);
    SET post_month_1_end = DATE_SUB(DATE_ADD(post_month_1_start, INTERVAL 1 MONTH), INTERVAL 1 DAY);
  
    SET post_month_2_start = DATE_TRUNC(post_month_2, MONTH);
    SET post_month_2_end = DATE_SUB(DATE_ADD(post_month_2_start, INTERVAL 1 MONTH), INTERVAL 1 DAY);
  
    SET post_month_3_start = DATE_TRUNC(post_month_3, MONTH);
    SET post_month_3_end = DATE_SUB(DATE_ADD(post_month_3_start, INTERVAL 1 MONTH), INTERVAL 1 DAY);
  
    SET post_month_4_start = DATE_TRUNC(post_month_4, MONTH);
    SET post_month_4_end = DATE_SUB(DATE_ADD(post_month_4_start, INTERVAL 1 MONTH), INTERVAL 1 DAY);
  
    SET post_month_5_start = DATE_TRUNC(post_month_5, MONTH);
    SET post_month_5_end = DATE_SUB(DATE_ADD(post_month_5_start, INTERVAL 1 MONTH), INTERVAL 1 DAY);
  
    SET post_month_6_start = DATE_TRUNC(post_month_6, MONTH);
    SET post_month_6_end = DATE_SUB(DATE_ADD(post_month_6_start, INTERVAL 1 MONTH), INTERVAL 1 DAY);
    
    SET post_month_7_start = DATE_TRUNC(post_month_7, MONTH);
    SET post_month_7_end = DATE_SUB(DATE_ADD(post_month_7_start, INTERVAL 1 MONTH), INTERVAL 1 DAY);
    
    SET post_month_8_start = DATE_TRUNC(post_month_8, MONTH);
    SET post_month_8_end = DATE_SUB(DATE_ADD(post_month_8_start, INTERVAL 1 MONTH), INTERVAL 1 DAY);
    
    SET post_month_9_start = DATE_TRUNC(post_month_9, MONTH);
    SET post_month_9_end = DATE_SUB(DATE_ADD(post_month_9_start, INTERVAL 1 MONTH), INTERVAL 1 DAY);
    
    SET post_month_10_start = DATE_TRUNC(post_month_10, MONTH);
    SET post_month_10_end = DATE_SUB(DATE_ADD(post_month_10_start, INTERVAL 1 MONTH), INTERVAL 1 DAY);
    
    SET post_month_11_start = DATE_TRUNC(post_month_11, MONTH);
    SET post_month_11_end = DATE_SUB(DATE_ADD(post_month_11_start, INTERVAL 1 MONTH), INTERVAL 1 DAY); 
  
    -- En este execute esta la consulta principal
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO recovered_summary
    SELECT
      '%s' AS BASE_MONTH,
      SIT_SITE_ID,
      COUNTIF(RECOVERED_GROUP = 'M1') AS M1_recovered,
      COUNTIF(RECOVERED_GROUP = 'M2') AS M2_recovered,
      COUNTIF(RECOVERED_GROUP = 'M3') AS M3_recovered,
      COUNTIF(RECOVERED_GROUP = 'M4') AS M4_recovered,
      COUNTIF(RECOVERED_GROUP = 'M5') AS M5_recovered,
      COUNTIF(RECOVERED_GROUP = 'M6') AS M6_recovered,
      COUNTIF(RECOVERED_GROUP = 'M7') AS M7_recovered,
      COUNTIF(RECOVERED_GROUP = 'M8') AS M8_recovered,
      COUNTIF(RECOVERED_GROUP = 'M9') AS M9_recovered,
      COUNTIF(RECOVERED_GROUP = 'M10') AS M10_recovered,
      COUNTIF(RECOVERED_GROUP = 'M11') AS M11_recovered
    FROM (
      WITH BASE_RECOVERED_BASE AS (
        SELECT SIT_SITE_ID, USER_ID
        FROM `meli-bi-data.WHOWNER.DM_MKT_MPLAY_RAW_PLAYS`
        WHERE TIME_FRAME = 'MONTHLY'
          AND LIFE_CYCLE = 'RECOVERED'
          AND TIM_DAY BETWEEN DATE '%s' AND DATE '%s'
        GROUP BY SIT_SITE_ID, USER_ID
      ),
      BASE_RECOVERED_MAS_1 AS (
        SELECT SIT_SITE_ID, USER_ID
        FROM `meli-bi-data.WHOWNER.DM_MKT_MPLAY_RAW_PLAYS`
        WHERE TIME_FRAME = 'MONTHLY'
          AND LIFE_CYCLE = 'RECOVERED'
          AND TIM_DAY BETWEEN DATE '%s' AND DATE '%s'
        GROUP BY SIT_SITE_ID, USER_ID
      ),
      BASE_RECOVERED_MAS_2 AS (
        SELECT SIT_SITE_ID, USER_ID
        FROM `meli-bi-data.WHOWNER.DM_MKT_MPLAY_RAW_PLAYS`
        WHERE TIME_FRAME = 'MONTHLY'
          AND LIFE_CYCLE = 'RECOVERED'
          AND TIM_DAY BETWEEN DATE '%s' AND DATE '%s'
        GROUP BY SIT_SITE_ID, USER_ID
      ),
          BASE_RECOVERED_MAS_3 AS (
        SELECT SIT_SITE_ID, USER_ID
        FROM `meli-bi-data.WHOWNER.DM_MKT_MPLAY_RAW_PLAYS`
        WHERE TIME_FRAME = 'MONTHLY'
          AND LIFE_CYCLE = 'RECOVERED'
          AND TIM_DAY BETWEEN DATE '%s' AND DATE '%s'
        GROUP BY SIT_SITE_ID, USER_ID
      ),
          BASE_RECOVERED_MAS_4 AS (
        SELECT SIT_SITE_ID, USER_ID
        FROM `meli-bi-data.WHOWNER.DM_MKT_MPLAY_RAW_PLAYS`
        WHERE TIME_FRAME = 'MONTHLY'
          AND LIFE_CYCLE = 'RECOVERED'
          AND TIM_DAY BETWEEN DATE '%s' AND DATE '%s'
        GROUP BY SIT_SITE_ID, USER_ID
      ),
          BASE_RECOVERED_MAS_5 AS (
        SELECT SIT_SITE_ID, USER_ID
        FROM `meli-bi-data.WHOWNER.DM_MKT_MPLAY_RAW_PLAYS`
        WHERE TIME_FRAME = 'MONTHLY'
          AND LIFE_CYCLE = 'RECOVERED'
          AND TIM_DAY BETWEEN DATE '%s' AND DATE '%s'
        GROUP BY SIT_SITE_ID, USER_ID
      ),
          BASE_RECOVERED_MAS_6 AS (
        SELECT SIT_SITE_ID, USER_ID
        FROM `meli-bi-data.WHOWNER.DM_MKT_MPLAY_RAW_PLAYS`
        WHERE TIME_FRAME = 'MONTHLY'
          AND LIFE_CYCLE = 'RECOVERED'
          AND TIM_DAY BETWEEN DATE '%s' AND DATE '%s'
        GROUP BY SIT_SITE_ID, USER_ID
      ),
               BASE_RECOVERED_MAS_7 AS (
        SELECT SIT_SITE_ID, USER_ID
        FROM `meli-bi-data.WHOWNER.DM_MKT_MPLAY_RAW_PLAYS`
        WHERE TIME_FRAME = 'MONTHLY'
          AND LIFE_CYCLE = 'RECOVERED'
          AND TIM_DAY BETWEEN DATE '%s' AND DATE '%s'
        GROUP BY SIT_SITE_ID, USER_ID
      ),
              BASE_RECOVERED_MAS_8 AS (
        SELECT SIT_SITE_ID, USER_ID
        FROM `meli-bi-data.WHOWNER.DM_MKT_MPLAY_RAW_PLAYS`
        WHERE TIME_FRAME = 'MONTHLY'
          AND LIFE_CYCLE = 'RECOVERED'
          AND TIM_DAY BETWEEN DATE '%s' AND DATE '%s'
        GROUP BY SIT_SITE_ID, USER_ID
      ),
              BASE_RECOVERED_MAS_9 AS (
        SELECT SIT_SITE_ID, USER_ID
        FROM `meli-bi-data.WHOWNER.DM_MKT_MPLAY_RAW_PLAYS`
        WHERE TIME_FRAME = 'MONTHLY'
          AND LIFE_CYCLE = 'RECOVERED'
          AND TIM_DAY BETWEEN DATE '%s' AND DATE '%s'
        GROUP BY SIT_SITE_ID, USER_ID
      ),
              BASE_RECOVERED_MAS_10 AS (
        SELECT SIT_SITE_ID, USER_ID
        FROM `meli-bi-data.WHOWNER.DM_MKT_MPLAY_RAW_PLAYS`
        WHERE TIME_FRAME = 'MONTHLY'
          AND LIFE_CYCLE = 'RECOVERED'
          AND TIM_DAY BETWEEN DATE '%s' AND DATE '%s'
        GROUP BY SIT_SITE_ID, USER_ID
      ),
              BASE_RECOVERED_MAS_11 AS (
        SELECT SIT_SITE_ID, USER_ID
        FROM `meli-bi-data.WHOWNER.DM_MKT_MPLAY_RAW_PLAYS`
        WHERE TIME_FRAME = 'MONTHLY'
          AND LIFE_CYCLE = 'RECOVERED'
          AND TIM_DAY BETWEEN DATE '%s' AND DATE '%s'
        GROUP BY SIT_SITE_ID, USER_ID
      ),
      BASE_FULL AS (
        SELECT
          base.SIT_SITE_ID,
          base.USER_ID,
          TRUE AS RECOVERED_BASE,
          IF(m1.USER_ID IS NOT NULL, TRUE, FALSE) AS RECOVERED_MAS_1,
          IF(m2.USER_ID IS NOT NULL, TRUE, FALSE) AS RECOVERED_MAS_2,
          IF(m3.USER_ID IS NOT NULL, TRUE, FALSE) AS RECOVERED_MAS_3,
          IF(m4.USER_ID IS NOT NULL, TRUE, FALSE) AS RECOVERED_MAS_4,
          IF(m5.USER_ID IS NOT NULL, TRUE, FALSE) AS RECOVERED_MAS_5,
          IF(m6.USER_ID IS NOT NULL, TRUE, FALSE) AS RECOVERED_MAS_6 ,
           IF(m7.USER_ID IS NOT NULL, TRUE, FALSE) AS RECOVERED_MAS_7,
          IF(m8.USER_ID IS NOT NULL, TRUE, FALSE) AS RECOVERED_MAS_8,
          IF(m9.USER_ID IS NOT NULL, TRUE, FALSE) AS RECOVERED_MAS_9,
          IF(m10.USER_ID IS NOT NULL, TRUE, FALSE) AS RECOVERED_MAS_10,
          IF(m11.USER_ID IS NOT NULL, TRUE, FALSE) AS RECOVERED_MAS_11 
        -- Este FROM combina todas las bases de retención dentro de la subconsulta
        FROM BASE_RECOVERED_BASE base
        LEFT JOIN BASE_RECOVERED_MAS_1 m1
          ON base.USER_ID = m1.USER_ID AND base.SIT_SITE_ID = m1.SIT_SITE_ID
        LEFT JOIN BASE_RECOVERED_MAS_2 m2
          ON base.USER_ID = m2.USER_ID AND base.SIT_SITE_ID = m2.SIT_SITE_ID
                LEFT JOIN BASE_RECOVERED_MAS_3 m3
          ON base.USER_ID = m3.USER_ID AND base.SIT_SITE_ID = m3.SIT_SITE_ID
                LEFT JOIN BASE_RECOVERED_MAS_4 m4
          ON base.USER_ID = m4.USER_ID AND base.SIT_SITE_ID = m4.SIT_SITE_ID
                LEFT JOIN BASE_RECOVERED_MAS_5 m5
          ON base.USER_ID = m5.USER_ID AND base.SIT_SITE_ID = m5.SIT_SITE_ID
                LEFT JOIN BASE_RECOVERED_MAS_6 m6
          ON base.USER_ID = m6.USER_ID AND base.SIT_SITE_ID = m6.SIT_SITE_ID
                 LEFT JOIN BASE_RECOVERED_MAS_7 m7
          ON base.USER_ID = m7.USER_ID AND base.SIT_SITE_ID = m7.SIT_SITE_ID
                LEFT JOIN BASE_RECOVERED_MAS_8 m8
          ON base.USER_ID = m8.USER_ID AND base.SIT_SITE_ID = m8.SIT_SITE_ID
                LEFT JOIN BASE_RECOVERED_MAS_9 m9
          ON base.USER_ID = m9.USER_ID AND base.SIT_SITE_ID = m9.SIT_SITE_ID
                LEFT JOIN BASE_RECOVERED_MAS_10 m10
          ON base.USER_ID = m10.USER_ID AND base.SIT_SITE_ID = m10.SIT_SITE_ID
                LEFT JOIN BASE_RECOVERED_MAS_11 m11
          ON base.USER_ID = m11.USER_ID AND base.SIT_SITE_ID = m11.SIT_SITE_ID 
      ),
      CLASIFICACION AS (
        SELECT
          SIT_SITE_ID,
          USER_ID,
          CASE
          WHEN RECOVERED_BASE AND NOT RECOVERED_MAS_1 THEN 'M1'
          WHEN RECOVERED_BASE AND RECOVERED_MAS_1 AND NOT RECOVERED_MAS_2 THEN 'M2'
          WHEN RECOVERED_BASE AND RECOVERED_MAS_1 AND RECOVERED_MAS_2 AND NOT RECOVERED_MAS_3 THEN 'M3'
          WHEN RECOVERED_BASE AND RECOVERED_MAS_1 AND RECOVERED_MAS_2 AND RECOVERED_MAS_3 AND NOT RECOVERED_MAS_4 THEN 'M4'
          WHEN RECOVERED_BASE AND RECOVERED_MAS_1 AND RECOVERED_MAS_2 AND RECOVERED_MAS_3 AND RECOVERED_MAS_4 AND NOT RECOVERED_MAS_5 THEN 'M5'
          WHEN RECOVERED_BASE AND RECOVERED_MAS_1 AND RECOVERED_MAS_2 AND RECOVERED_MAS_3 AND RECOVERED_MAS_4 AND RECOVERED_MAS_5 AND NOT RECOVERED_MAS_6 THEN 'M6'
           WHEN RECOVERED_BASE AND RECOVERED_MAS_1 AND RECOVERED_MAS_2 AND RECOVERED_MAS_3 AND RECOVERED_MAS_4 AND RECOVERED_MAS_5 AND RECOVERED_MAS_6 AND NOT RECOVERED_MAS_7 THEN 'M7'
          WHEN RECOVERED_BASE AND RECOVERED_MAS_1 AND RECOVERED_MAS_2 AND RECOVERED_MAS_3 AND RECOVERED_MAS_4 AND RECOVERED_MAS_5 AND RECOVERED_MAS_6  AND  RECOVERED_MAS_7 AND NOT  RECOVERED_MAS_8 THEN 'M8'
          WHEN RECOVERED_BASE AND RECOVERED_MAS_1 AND RECOVERED_MAS_2 AND RECOVERED_MAS_3 AND RECOVERED_MAS_4 AND RECOVERED_MAS_5 AND RECOVERED_MAS_6  AND  RECOVERED_MAS_7 AND RECOVERED_MAS_8 AND NOT  RECOVERED_MAS_9 THEN 'M9'
          WHEN RECOVERED_BASE AND RECOVERED_MAS_1 AND RECOVERED_MAS_2 AND RECOVERED_MAS_3 AND RECOVERED_MAS_4 AND RECOVERED_MAS_5 AND RECOVERED_MAS_6  AND  RECOVERED_MAS_7 AND RECOVERED_MAS_8 AND RECOVERED_MAS_9 AND NOT  RECOVERED_MAS_10 THEN 'M10'
          WHEN RECOVERED_BASE AND RECOVERED_MAS_1 AND RECOVERED_MAS_2 AND RECOVERED_MAS_3 AND RECOVERED_MAS_4 AND RECOVERED_MAS_5 AND RECOVERED_MAS_6  AND  RECOVERED_MAS_7 AND RECOVERED_MAS_8 AND RECOVERED_MAS_9 AND RECOVERED_MAS_10 AND NOT  RECOVERED_MAS_11 THEN 'M11'
          ELSE 'OTROS'
          END AS RECOVERED_GROUP   
        FROM BASE_FULL
      )
      SELECT * FROM CLASIFICACION
    )
    GROUP BY SIT_SITE_ID
    """,
    -- Se pasan como parámetros las fechas del mes base y los meses posteriores
    FORMAT_DATE('%Y-%m-%d', base_month_start),
    FORMAT_DATE('%F', base_month_start), FORMAT_DATE('%F', base_month_end),
    FORMAT_DATE('%F', post_month_1_start), FORMAT_DATE('%F', post_month_1_end),
    FORMAT_DATE('%F', post_month_2_start), FORMAT_DATE('%F', post_month_2_end),
    FORMAT_DATE('%F', post_month_3_start), FORMAT_DATE('%F', post_month_3_end),
    FORMAT_DATE('%F', post_month_4_start), FORMAT_DATE('%F', post_month_4_end),
    FORMAT_DATE('%F', post_month_5_start), FORMAT_DATE('%F', post_month_5_end),
    FORMAT_DATE('%F', post_month_6_start), FORMAT_DATE('%F', post_month_6_end),
    FORMAT_DATE('%F', post_month_7_start), FORMAT_DATE('%F', post_month_7_end),
    FORMAT_DATE('%F', post_month_8_start), FORMAT_DATE('%F', post_month_8_end),
    FORMAT_DATE('%F', post_month_9_start), FORMAT_DATE('%F', post_month_9_end), 
    FORMAT_DATE('%F', post_month_10_start), FORMAT_DATE('%F', post_month_10_end),
    FORMAT_DATE('%F', post_month_11_start), FORMAT_DATE('%F', post_month_11_end) 
  );
  
  
    SET i = i + 1;
  END WHILE;
  
  -- Guardamos los resultados finales de la tabla temporal en la tabla de destino
  CREATE OR REPLACE TABLE `EXPLOTACION.FIDELIZACION_RECOVERED` AS
  SELECT * FROM recovered_summary 
  ORDER BY base_month 
  DESC, sit_site_id;

END;
