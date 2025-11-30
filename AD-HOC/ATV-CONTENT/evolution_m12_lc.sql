WITH user_monthly_activity AS (
    -- 1. Tu consulta original: actividad mensual por usuario (La base de todos los usuarios M0)
    SELECT
        P.USER_ID,
        COALESCE(R.LIFE_CYCLE, 'Unknown') AS LIFE_CYCLE,
        P.SIT_SITE_ID AS PAIS,
        FORMAT_DATE('%Y-%m', P.DS) AS MONTH_YEAR,
        -- La métrica de interés: VIEWING_FLAG
        CASE 
            WHEN COUNTIF(C.CONTENT_TYPE = 'MOVIE') > 0 
                 AND COUNTIF(C.CONTENT_TYPE <> 'MOVIE') > 0 THEN 'Both'
            WHEN COUNTIF(C.CONTENT_TYPE = 'MOVIE') > 0 THEN 'Only Movies'
            ELSE 'Only Series' 
        END AS VIEWING_FLAG
    FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` P
    LEFT JOIN `meli-bi-data.WHOWNER.LK_MKT_MPLAY_CATALOGUE` C 
        ON P.SIT_SITE_ID = C.SIT_SITE_ID
        AND P.CONTENT_ID = C.CONTENT_ID
      LEFT JOIN `meli-bi-data.WHOWNER.DM_MKT_MPLAY_RAW_PLAYS` AS R
    ON P.USER_ID = R.USER_ID
    AND P.SIT_SITE_ID = R.SIT_SITE_ID
    AND DATE_TRUNC(P.DS,MONTH) = DATE_TRUNC(R.TIM_DAY,MONTH)
    WHERE 
        P.DS BETWEEN '2025-01-01' AND CURRENT_DATE - 1
        AND P.PLAYBACK_TIME_MILLISECONDS >= 20000 
      --  AND R.LIFE_CYCLE = 'NEW'
      AND R.TIME_FRAME = 'MONTHLY'
    GROUP BY 1, 2, 3, 4
),
cohort_base AS (
    -- 2. Define la cohorte M0 (todos los usuarios de un mes)
    SELECT 
        USER_ID,
        LIFE_CYCLE, 
        PAIS, 
        MONTH_YEAR AS COHORT_MONTH, 
        VIEWING_FLAG AS COHORT_VIEWING_FLAG_M0 -- La flag en el mes M0
    FROM user_monthly_activity
),
user_retention_check AS (
    -- 3. Genera todas las combinaciones posibles de (COHORT_MONTH, MONTH_YEAR) para cada usuario
    SELECT
        CB.COHORT_MONTH,
        CB.USER_ID,
        CB.LIFE_CYCLE,
        CB.PAIS,
        CB.COHORT_VIEWING_FLAG_M0,
        -- Genera una serie de meses desde el COHORT_MONTH hasta el mes actual
        T.MONTH_YEAR AS CURRENT_MONTH
    FROM cohort_base CB
    CROSS JOIN (SELECT DISTINCT MONTH_YEAR FROM user_monthly_activity) T
    -- Solo considera meses a partir del COHORT_MONTH
    WHERE DATE(T.MONTH_YEAR || '-01') >= DATE(CB.COHORT_MONTH || '-01')
      -- Limita a 12 meses (M0 hasta M12)
      AND DATE_DIFF(DATE(T.MONTH_YEAR || '-01'), DATE(CB.COHORT_MONTH || '-01'), MONTH) <= 12
),
continuity_flag AS (
    -- 4. Verifica la actividad y continuidad
    SELECT
        URC.COHORT_MONTH,
        URC.USER_ID,
        URC.LIFE_CYCLE,
        URC.PAIS,
        URC.COHORT_VIEWING_FLAG_M0,
        URC.CURRENT_MONTH,
        -- Flag para saber si el usuario estuvo activo en el mes actual
        IF(UMA.USER_ID IS NOT NULL, 1, 0) AS IS_ACTIVE_IN_MONTH,
        UMA.VIEWING_FLAG AS CURRENT_VIEWING_FLAG
    FROM user_retention_check URC
    LEFT JOIN user_monthly_activity UMA
        ON URC.USER_ID = UMA.USER_ID
        AND URC.PAIS = UMA.PAIS
        AND URC.CURRENT_MONTH = UMA.MONTH_YEAR
),
retained_users AS (
    -- 5. Filtra solo los usuarios que tuvieron actividad continua (Retención Estricta)
    SELECT
        C.COHORT_MONTH,
        C.CURRENT_MONTH,
        C.USER_ID,
        C.LIFE_CYCLE,
        C.PAIS,
        C.COHORT_VIEWING_FLAG_M0,
        C.CURRENT_VIEWING_FLAG,
        -- Contar cuántos meses consecutivos desde el COHORT_MONTH el usuario ha estado activo
        SUM(1 - C.IS_ACTIVE_IN_MONTH) OVER (
            PARTITION BY C.USER_ID, C.PAIS, C.COHORT_MONTH
            ORDER BY DATE(C.CURRENT_MONTH || '-01')
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS GAPS_COUNT 
    FROM continuity_flag C
)

-- 6. Resultado Final: Solo incluye usuarios si NO tienen huecos (GAPS_COUNT = 0)
SELECT
    A.COHORT_MONTH,
    -- Calcula el offset M0, M1, M2...
    DATE_DIFF(DATE(A.CURRENT_MONTH || '-01'), DATE(A.COHORT_MONTH || '-01'), MONTH) AS MONTH_OFFSET,
    A.CURRENT_VIEWING_FLAG,
    A.LIFE_CYCLE,
    COUNT(DISTINCT A.USER_ID) AS USERS_COUNT_RETAINED
FROM retained_users A
WHERE A.GAPS_COUNT = 0 -- Condición de Retención Estricta
GROUP BY 1, 2, 3, 4
ORDER BY A.COHORT_MONTH, MONTH_OFFSET, A.CURRENT_VIEWING_FLAG;