DECLARE start_month DATE DEFAULT DATE '2025-01-01';
DECLARE months_to_analyze INT64 DEFAULT 7;
DECLARE m_limit INT64 DEFAULT 2;
DECLARE i INT64 DEFAULT 0;

-- Variables que se reutilizan en cada iteración
DECLARE base_month DATE;
DECLARE base_month_start DATE;
DECLARE base_month_end DATE;
DECLARE post_months ARRAY<DATE>;
DECLARE post_months_start ARRAY<DATE>;
DECLARE post_months_end ARRAY<DATE>;

-- Variables para construir la query dinámica, declaradas una sola vez
DECLARE insert_columns STRING;
DECLARE retained_joins STRING;
DECLARE retained_flags STRING;
DECLARE retained_case STRING;
DECLARE m_cte_definitions STRING;
DECLARE current_retained_condition STRING;
DECLARE previous_months_conditions STRING;
DECLARE k INT64;
DECLARE full_with_clause_string STRING; -- New variable for the complete WITH clause

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

SET column_defs = 'base_month STRING, sit_site_id STRING, ' || column_defs;

EXECUTE IMMEDIATE FORMAT("""
  CREATE TEMP TABLE retained_summary (%s)
""", column_defs);

-- Main loop to iterate through each base month
SET i = 0;
WHILE i < months_to_analyze DO
  SET base_month = DATE_ADD(start_month, INTERVAL i MONTH);
  SET base_month_start = DATE_TRUNC(base_month, MONTH);
  SET base_month_end = DATE_SUB(DATE_ADD(base_month_start, INTERVAL 1 MONTH), INTERVAL 1 DAY);

  -- Generar dinámicamente los meses y sus fechas de inicio/fin para los post-meses (M1, M2, ...)
  SET post_months = ARRAY(
    SELECT DATE_ADD(base_month, INTERVAL m MONTH)
    FROM UNNEST(GENERATE_ARRAY(1, m_limit)) AS m
  );

  SET post_months_start = ARRAY(
    SELECT DATE_TRUNC(m, MONTH)
    FROM UNNEST(post_months) AS m
  );

  SET post_months_end = ARRAY(
    SELECT DATE_SUB(DATE_ADD(m, INTERVAL 1 MONTH), INTERVAL 1 DAY)
    FROM UNNEST(post_months_start) AS m
  );

  -- Reset variables for each iteration of the outer loop
  SET insert_columns = '';
  SET retained_joins = '';
  SET retained_flags = '';
  SET retained_case = '';
  SET m_cte_definitions = '';
  SET full_with_clause_string = ''; -- Reset for each iteration
  SET j = 1;

  -- Construimos partes variables de la query para la iteración actual
  WHILE j <= m_limit DO
    -- Reset variables for each iteration of the inner loop
    SET current_retained_condition = '';
    SET previous_months_conditions = '';
    SET k = 1; -- Reset k for each inner loop iteration

    -- INSERT y SELECT COUNTIF(...)
    SET insert_columns = insert_columns || FORMAT(', COUNTIF(RETAINED_GROUP = "M%d") AS M%d_RETAINED', j, j);

    -- CTEs para cada BASE_RETAINED_MAS_n (no agrega comas al final)
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
      FORMAT_DATE('%F', post_months_start[OFFSET(j - 1)]),
      FORMAT_DATE('%F', post_months_end[OFFSET(j - 1)]),
      IF(j < m_limit, ',', '') -- Add comma only if not the last CTE in this block
    );

    -- JOINs en BASE_FULL
    SET retained_joins = retained_joins || FORMAT('\n  LEFT JOIN BASE_RETAINED_MAS_%d m%d\n    ON base.USER_ID = m%d.USER_ID AND base.SIT_SITE_ID = m%d.SIT_SITE_ID', j, j, j, j);

    -- RETAINED_MAS_n flags en BASE_FULL
    SET retained_flags = retained_flags || FORMAT(', IF(m%d.USER_ID IS NOT NULL, TRUE, FALSE) AS RETAINED_MAS_%d', j, j);

    -- CASE en CLASIFICACION: Adjusted logic for building the WHEN condition
    IF j = 1 THEN
      SET current_retained_condition = 'RETAINED_BASE AND NOT RETAINED_MAS_1';
    ELSE
      -- Build the AND chain for previous months
      WHILE k < j DO
        SET previous_months_conditions = previous_months_conditions || FORMAT('RETAINED_MAS_%d AND ', k);
        SET k = k + 1;
      END WHILE;
      -- Remove trailing ' AND '
      SET previous_months_conditions = SUBSTR(previous_months_conditions, 1, LENGTH(previous_months_conditions) - 5);

      IF j = m_limit THEN
        -- If it's the last M group, it captures all users retained up to this point and beyond
        SET current_retained_condition = 'RETAINED_BASE AND ' || previous_months_conditions;
      ELSE
        -- For intermediate M groups, it's retained up to previous and NOT retained in current
        SET current_retained_condition = 'RETAINED_BASE AND ' || previous_months_conditions || FORMAT(' AND NOT RETAINED_MAS_%d', j);
      END IF;
    END IF;

    SET retained_case = retained_case || FORMAT('\n        WHEN %s THEN "M%d"', current_retained_condition, j);

    SET j = j + 1;
  END WHILE;

  -- Construct the full WITH clause string
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
  IF(m_limit > 0, ',\n' || m_cte_definitions || ',', ''), -- Comma, m_cte_definitions, and another comma if m_limit > 0
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

  SET i = i + 1; -- Increment counter for the next month
END WHILE;

-- Optional: Select the final results
SELECT * FROM retained_summary ORDER BY base_month, sit_site_id;
