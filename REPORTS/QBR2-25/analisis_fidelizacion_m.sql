-- ANALISIS DE FIDELIZACIÓN MENSUAL (CLASIFICACION M) --
-- Esta query analiza la retención de usuarios en un periodo mensual, clasificando a los usuarios en grupos de retención M1, M2, etc. Se comienza tomando un mes base, especificando la cantidad de meses que se quieren analizar hacia adelante y la cantidad de clasificaciones M. 
-- NOTA -- Aquellos usuarios que tengan actividad en meses posteriores al M_LIMIT se los incluye en el grupo M_LIMIT, es decir, si se analiza M1, M2 y M3, los usuarios que tengan actividad en M4 o posteriores se los clasifica como M3
-- TABLAS --
-- `meli-bi-data.WHOWNER.DM_MKT_MPLAY_RAW_PLAYS`: tabla de control de torre
-- `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS`: tabla de reproducciones de Play

-- Primer día del mes inicial: hace 12 meses desde el mes actual
DECLARE start_month DATE DEFAULT DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH), MONTH);

-- Siempre analizamos 12 meses
DECLARE months_to_analyze INT64 DEFAULT 12;

-- Límite de clasificación M
DECLARE m_limit INT64 DEFAULT 12; -- o el valor que necesites

-- Con esta variable comenzaremos a iterar
DECLARE i INT64 DEFAULT 0;

-- Variables que se reutilizan en cada iteración de mes base y su fecha de inicio y fin. Tambien variables para los meses posteriores y sus fechas de inicio y fin
DECLARE base_month DATE;
DECLARE base_month_start DATE;
DECLARE base_month_end DATE;
DECLARE post_months ARRAY<DATE>;
DECLARE post_months_start ARRAY<DATE>;
DECLARE post_months_end ARRAY<DATE>;

-- Variables para construir la query dinámica
DECLARE insert_columns STRING;
DECLARE retained_joins STRING;
DECLARE retained_flags STRING;
DECLARE retained_case STRING;
DECLARE m_cte_definitions STRING;
DECLARE current_retained_condition STRING;
DECLARE previous_months_conditions STRING;
DECLARE k INT64;
DECLARE full_with_clause_string STRING;

-- Crea una tabla temporal para guardar los resultados
DECLARE column_defs STRING DEFAULT '';
DECLARE j INT64 DEFAULT 1;

-- Armamos las columnas m1_retained, m2_retained... hasta m_limit
WHILE j <= m_limit DO
  SET column_defs = column_defs || FORMAT('m%d_retained INT64', j);
  IF j < m_limit THEN
    SET column_defs = column_defs || ', ';
  END IF;
  SET j = j + 1;
END WHILE;
-- En esta parte armamos la definición de las columnas de la tabla temporal
SET column_defs = 'base_month STRING, sit_site_id STRING, ' || column_defs;
-- Con este EXECUTE IMMEDIATE creamos la tabla temporal con las columnas definidas antes
EXECUTE IMMEDIATE FORMAT("""
  CREATE TEMP TABLE retained_summary (%s)
""", column_defs);

-- Seteamos i con el valor 0 permitiendonos comenzar la iteracion en el while
SET i = 0;
-- Comenzamos el while siempre que i sea menor a la cantidad de meses a analizar para generar los meses base con sus respectivas fechas de inicio y fin, y los meses posteriores con sus respectivas fechas de inicio y fin
WHILE i < months_to_analyze DO
  -- En este SET definimos el mes base haciendo un date_add al mes de inicio sumando el valor de i que viene en el while
  SET base_month = DATE_ADD(start_month, INTERVAL i MONTH);
  -- Aca definimos el inicio del mes con un date_trunc al mes base
  SET base_month_start = DATE_TRUNC(base_month, MONTH);
  -- Aca definimos el fin del mes base restando un dia al primer dia del mes siguiente
  SET base_month_end = DATE_SUB(DATE_ADD(base_month_start, INTERVAL 1 MONTH), INTERVAL 1 DAY);

  -- Aca definino los meses siguientes creando un array
  SET post_months = ARRAY(
    -- Utilizo el date_add sumando m meses al mes base donde m sale de generar un array de 1 a m_limit
    SELECT DATE_ADD(base_month, INTERVAL m MONTH)
    FROM UNNEST(GENERATE_ARRAY(1, m_limit)) AS m
  );
  -- Aca hacemos algo similar al paso anterior pero ahora generamos un array con el primer dia de cada mes
  SET post_months_start = ARRAY(
    SELECT DATE_TRUNC(m, MONTH)
    FROM UNNEST(post_months) AS m
  );
  -- Al igual que el set anteior, generamos un array con el ultimo dia de cada mes
  SET post_months_end = ARRAY(
    SELECT DATE_SUB(DATE_ADD(m, INTERVAL 1 MONTH), INTERVAL 1 DAY)
    FROM UNNEST(post_months_start) AS m
  );

  -- Reseteamos las variables a valores iniciales
  SET insert_columns = '';
  SET retained_joins = '';
  SET retained_flags = '';
  SET retained_case = '';
  SET m_cte_definitions = '';
  SET full_with_clause_string = '';
  SET j = 1;

  -- Con este while ya empezamos a armar las columnas de la tabla temporal y las subconsultas para cada M
  WHILE j <= m_limit DO
    -- Reseteo las variables para cada iteración de M
    SET current_retained_condition = '';
    SET previous_months_conditions = '';
    SET k = 1;

    -- En este insert voy a armar las columnas por cada M analizado pasando j como parametro
    SET insert_columns = insert_columns || FORMAT(', COUNTIF(RETAINED_GROUP = "M%d") AS M%d_RETAINED', j, j);

    -- Este CTE es la base de datos que contiene los usuarios retenidos en el mes base
    SET m_cte_definitions = m_cte_definitions || FORMAT("""
    BASE_RETAINED_MAS_%d AS (
      SELECT SIT_SITE_ID, USER_ID
      FROM `meli-bi-data.WHOWNER.DM_MKT_MPLAY_RAW_PLAYS`
      WHERE TIME_FRAME = 'MONTHLY'
        AND LIFE_CYCLE = 'RETAINED'
        AND TIM_DAY BETWEEN DATE '%s' AND DATE '%s'
      GROUP BY SIT_SITE_ID, USER_ID
    )%s""",
      j,
      -- Para prestar atención aca, con este format_date hago un offset de los meses posteriores al mes base
      FORMAT_DATE('%F', post_months_start[OFFSET(j - 1)]),
      FORMAT_DATE('%F', post_months_end[OFFSET(j - 1)]),
      IF(j < m_limit, ',', '') -- Con este IF agrego una coma al final de cada CTE excepto el último
    );

    -- Aca ya generamos un LEFT JOIN a la base de datos de usuarios retenidos en el mes base
    SET retained_joins = retained_joins || FORMAT('\n  LEFT JOIN BASE_RETAINED_MAS_%d m%d\n    ON base.USER_ID = m%d.USER_ID AND base.SIT_SITE_ID = m%d.SIT_SITE_ID', j, j, j, j);

    -- Con este retained_flags armamos las columnas que indican si el usuario fue retenido en el mes base y en los meses posteriores
    SET retained_flags = retained_flags || FORMAT(', IF(m%d.USER_ID IS NOT NULL, TRUE, FALSE) AS RETAINED_MAS_%d', j, j);

    -- Si j es el primer mes entonces el current_retained_condition es "RETAINED_BASE AND NOT RETAINED_MAS_1", si no, armamos una cadena AND con los meses anteriores para el filtro
    IF j = 1 THEN
      SET current_retained_condition = 'RETAINED_BASE AND NOT RETAINED_MAS_1';
    ELSE
      -- Este k es para iterar siempre que sea menor a j y asi armarmos las condiciones de los meses anteriores
      WHILE k < j DO
        SET previous_months_conditions = previous_months_conditions || FORMAT('RETAINED_MAS_%d AND ', k);
        SET k = k + 1;
      END WHILE;
      -- Lo que hacemos aca es quitar el ultimo AND de previous_months_conditions
      SET previous_months_conditions = SUBSTR(previous_months_conditions, 1, LENGTH(previous_months_conditions) - 5);

      IF j = m_limit THEN
        -- Cuando j sea igual al m_limit, significa que es el último mes y no hay meses posteriores, por lo que se mantiene la condición de "RETAINED_BASE AND" con los meses anteriores
        SET current_retained_condition = 'RETAINED_BASE AND ' || previous_months_conditions;
      ELSE
        -- Si no es el último mes, entonces agregamos "AND NOT RETAINED_MAS_j" para filtrar los usuarios que no fueron retenidos en el mes j
        SET current_retained_condition = 'RETAINED_BASE AND ' || previous_months_conditions || FORMAT(' AND NOT RETAINED_MAS_%d', j);
      END IF;
    END IF;
    -- Armamos el CASE para clasificar los usuarios en los distintos grupos de retención
    SET retained_case = retained_case || FORMAT('\n        WHEN %s THEN "M%d"', current_retained_condition, j);

    SET j = j + 1;
  END WHILE;

  -- Ya en este punto tenemos todas las variables armadas y podemos construir la consulta completa pasando por parametros las variables que armamos antes
  SET full_with_clause_string = FORMAT("""
    WITH BASE_RETAINED_BASE AS (
      SELECT SIT_SITE_ID, USER_ID
      FROM `meli-bi-data.WHOWNER.DM_MKT_MPLAY_RAW_PLAYS`
      WHERE TIME_FRAME = 'MONTHLY'
        AND LIFE_CYCLE = 'RETAINED'
        AND TIM_DAY BETWEEN DATE '%s' AND DATE '%s'
      GROUP BY SIT_SITE_ID, USER_ID
    )%s
    BASE_FULL AS (
      SELECT
        base.SIT_SITE_ID,
        base.USER_ID,
        TRUE AS RETAINED_BASE%s
      FROM BASE_RETAINED_BASE base%s
    ),
    CLASIFICACION AS (
      SELECT
        SIT_SITE_ID,
        USER_ID,
        CASE%s
        ELSE 'OTROS'
        END AS RETAINED_GROUP
      FROM BASE_FULL
    )
  """,
  FORMAT_DATE('%F', base_month_start),
  FORMAT_DATE('%F', base_month_end),
  IF(m_limit > 0, ',\n' || m_cte_definitions || ',', ''), -- Este IF agrega las definiciones de los CTEs de M si m_limit es mayor a 0
  retained_flags,
  retained_joins,
  retained_case
  );

  -- Armamos la query completa con FORMAT y la ejecutamos
  EXECUTE IMMEDIATE FORMAT("""
  INSERT INTO retained_summary
  SELECT
    '%s' AS BASE_MONTH,
    SIT_SITE_ID%s
  FROM (
    %s -- This is the full_with_clause_string
    SELECT * FROM CLASIFICACION
  )
  GROUP BY SIT_SITE_ID
  """,
  FORMAT_TIMESTAMP('%F', base_month),
  insert_columns,
  full_with_clause_string
  );

  SET i = i + 1; 
END WHILE;

-- Consulta final para mostrar los resultados
SELECT * FROM retained_summary ORDER BY base_month, sit_site_id;
