WITH monthly_life_cycle AS (
    -- Aseguramos un único LIFE_CYCLE por usuario/país/mes para evitar duplicación
    SELECT
        R.USER_ID,
        R.SIT_SITE_ID AS PAIS,
        FORMAT_DATE('%Y-%m', R.TIM_DAY) AS MONTH_YEAR,
        -- Elige el LIFE_CYCLE único por mes. Si hay varios, MAX/MIN es un placeholder; 
        -- elige el que tenga mayor prioridad en tu negocio (ej. 'NEW' > 'ACTIVE')
        MAX(R.LIFE_CYCLE) AS DOMINANT_LIFE_CYCLE 
    FROM `meli-bi-data.WHOWNER.DM_MKT_MPLAY_RAW_PLAYS` AS R
    WHERE R.TIME_FRAME = 'MONTHLY'
    GROUP BY 1, 2, 3
),
user_monthly_activity AS (
    -- 1. Actividad mensual: Calcula la VIEWING_FLAG y une el LIFE_CYCLE único
    SELECT
        P.USER_ID,
        COALESCE(MLC.DOMINANT_LIFE_CYCLE, 'Unknown') AS LIFE_CYCLE,
        P.SIT_SITE_ID AS PAIS,
        FORMAT_DATE('%Y-%m', P.DS) AS MONTH_YEAR,
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
    LEFT JOIN monthly_life_cycle MLC
        ON P.USER_ID = MLC.USER_ID
        AND P.SIT_SITE_ID = MLC.PAIS
        AND FORMAT_DATE('%Y-%m', P.DS) = MLC.MONTH_YEAR
    WHERE 
        P.DS BETWEEN '2025-01-01' AND CURRENT_DATE - 1
        AND P.PLAYBACK_TIME_MILLISECONDS >= 20000 
    GROUP BY 1, 2, 3, 4
)
-- 2. Análisis de Migración M0 -> M1
SELECT
    M0.COHORT_MONTH,
    M0.LIFE_CYCLE,
    M0.COHORT_VIEWING_FLAG_M0 AS VIEWING_FLAG_M0,
    COALESCE(M1.VIEWING_FLAG, 'Churn') AS VIEWING_FLAG_M1, -- Si M1 es NULL, el usuario hizo Churn
    COUNT(DISTINCT M0.USER_ID) AS USERS_COUNT
FROM (
    -- Usuarios M0: El mes base
    SELECT 
        USER_ID,
        LIFE_CYCLE, 
        PAIS, 
        MONTH_YEAR AS COHORT_MONTH, 
        VIEWING_FLAG AS COHORT_VIEWING_FLAG_M0
    FROM user_monthly_activity
) M0
-- 3. Unión con el mes M1 (el mes siguiente)
LEFT JOIN user_monthly_activity M1
    ON M0.USER_ID = M1.USER_ID
    AND M0.PAIS = M1.PAIS
    -- M1 debe ser exactamente un mes después de M0
    AND M1.MONTH_YEAR = FORMAT_DATE(
        '%Y-%m', 
        DATE_ADD(DATE(M0.COHORT_MONTH || '-01'), INTERVAL 1 MONTH)
    )
GROUP BY 1, 2, 3, 4
ORDER BY 
    M0.COHORT_MONTH, 
    M0.LIFE_CYCLE, 
    VIEWING_FLAG_M0, 
    VIEWING_FLAG_M1;