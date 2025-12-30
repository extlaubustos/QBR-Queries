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
















WITH monthly_life_cycle AS (
    -- Aseguramos un único LIFE_CYCLE por usuario/país/mes para evitar duplicación
    SELECT
        R.USER_ID,
        R.SIT_SITE_ID AS PAIS,
        FORMAT_DATE('%Y-%m', R.TIM_DAY) AS MONTH_YEAR,
        R.LIFE_CYCLE
    FROM `meli-bi-data.WHOWNER.DM_MKT_MPLAY_RAW_PLAYS` AS R
    WHERE R.TIME_FRAME = 'MONTHLY'
    GROUP BY 1, 2, 3
),
user_monthly_activity_with_tvm AS (
    -- 1. Actividad mensual: Calcula la VIEWING_FLAG, el TVM (en minutos) y une el LIFE_CYCLE
    SELECT
        P.USER_ID,
        COALESCE(MLC.LIFE_CYCLE, 'Unknown') AS LIFE_CYCLE,
        P.SIT_SITE_ID AS PAIS,
        FORMAT_DATE('%Y-%m', P.DS) AS MONTH_YEAR,
        -- Cálculo del TVM en minutos
        SUM(P.PLAYBACK_TIME_MILLISECONDS / 60000) AS TVM, 
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
-- 2. Análisis de Migración M0 -> M1 con TVM promedio
SELECT
    M0.COHORT_MONTH,
    M0.LIFE_CYCLE,
    M0.VIEWING_FLAG_M0,
    COALESCE(M1.VIEWING_FLAG, 'Churn') AS VIEWING_FLAG_M1, -- Si M1 es NULL, el usuario hizo Churn
    COUNT(DISTINCT M0.USER_ID) AS USERS_COUNT,
    -- TVM promedio en M0
    SUM(M0.TVM_M0) AS AVG_TVM_M0_MINUTES,
    -- TVM promedio en M1. Si hizo Churn, el TVM_M1 es 0.
    COALESCE(SUM(M1.TVM), 0) AS AVG_TVM_M1_MINUTES
FROM (
    -- Usuarios M0: El mes base con su TVM
    SELECT 
        USER_ID,
        LIFE_CYCLE, 
        PAIS, 
        MONTH_YEAR AS COHORT_MONTH, 
        VIEWING_FLAG AS VIEWING_FLAG_M0,
        TVM AS TVM_M0
    FROM user_monthly_activity_with_tvm
) M0
-- 3. Unión con el mes M1 (el mes siguiente)
LEFT JOIN user_monthly_activity_with_tvm M1
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















WITH monthly_life_cycle AS (
    -- Aseguramos un único LIFE_CYCLE por usuario/país/mes para evitar duplicación
    SELECT
        R.USER_ID,
        R.SIT_SITE_ID AS PAIS,
        date_trunc(r.tim_day, month) AS MONTH_YEAR,
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
        DATE_TRUNC(P.DS, WEEK(MONDAY)) AS WEEK_ID,
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
        AND (P.DS, MONTH) = MLC.MONTH_YEAR
    WHERE 
        P.DS BETWEEN '2025-01-01' AND CURRENT_DATE - 1
        AND P.PLAYBACK_TIME_MILLISECONDS >= 20000 
    GROUP BY 1, 2, 3, 4
)
-- 2. Análisis de Migración M0 -> M1
SELECT
    M0.COHORT_WEEK,
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
        WEEK_ID AS COHORT_WEEK, 
        VIEWING_FLAG AS COHORT_VIEWING_FLAG_M0
    FROM user_monthly_activity
) M0
-- 3. Unión con el mes M1 (el mes siguiente)
LEFT JOIN user_monthly_activity M1
    ON M0.USER_ID = M1.USER_ID
    AND M0.PAIS = M1.PAIS
    -- M1 debe ser exactamente un mes después de M0
    AND M1.WEEK_ID = DATE_ADD(M0.COHORT_WEEK, INTERVAL 7 DAY)
GROUP BY 1, 2, 3, 4
ORDER BY 
    M0.COHORT_WEEK, 
    M0.LIFE_CYCLE, 
    VIEWING_FLAG_M0, 
    VIEWING_FLAG_M1;















    WITH PLAY_DAYS AS (
               SELECT DISTINCT
                P.SIT_SITE_ID AS SIT_SITE_ID,
                (P.USER_ID) AS USER_ID,
                (P.DS) AS DAY_PLAY,
                SUM(P.PLAYBACK_TIME_MILLISECONDS/60000) AS PLAYBACK_TIME,
                FROM   `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` as P
               WHERE  P.PLAYBACK_TIME_MILLISECONDS/1000 >= 20
                GROUP BY ALL
),
PLAY_FIRST_DAY AS ( 
                SELECT 
                SIT_SITE_ID,
                USER_ID,
                MIN(DAY_PLAY) AS FIRST_DAY
                FROM PLAY_DAYS
                GROUP BY ALL
),
TABLE_CALENDAR AS (
            SELECT
             *
            FROM (
                  SELECT 
                  *,
                  ROW_NUMBER()OVER(PARTITION BY FECHA_COHORT ORDER BY FECHA_INI ASC) AS MONTH_NUMBER
                  FROM (
                              SELECT 
                              T.TIM_DAY AS FECHA_COHORT,
                              T2.TIM_DAY -29  AS FECHA_INI,
                              T2.TIM_DAY AS FECHA_FIN,
                              
                              FROM `meli-bi-data.WHOWNER.LK_TIM_DAYS` AS T
                              LEFT JOIN `meli-bi-data.WHOWNER.LK_TIM_DAYS` AS T2 ON T.TIM_DAY+1 <= T2.TIM_DAY
            --> +1 PARA SACAR DIA DE ALTA
                              WHERE T.TIM_DAY >= DATE'2023-07-01'
                              QUALIFY MOD(ROW_NUMBER()OVER(PARTITION BY T.TIM_DAY ORDER BY T2.TIM_DAY ASC), 30) = 0
                              ) AS A
                   WHERE FECHA_FIN <= DATE_TRUNC(CURRENT_DATE-1,MONTH)-1
                        ) AS B
                QUALIFY COUNT(FECHA_FIN)OVER (PARTITION BY DATE_TRUNC(FECHA_COHORT,MONTH),MONTH_NUMBER) = EXTRACT(DAY FROM(LAST_DAY(FECHA_COHORT)))
                     --> COHORT_HASTA AYER
),
USERS_CALENDAR AS (

            SELECT 
            PF.*,
            T.FECHA_INI,
            T.FECHA_FIN,
            MONTH_NUMBER
            FROM PLAY_FIRST_DAY AS PF
                LEFT JOIN TABLE_CALENDAR AS T ON PF.FIRST_DAY = T.FECHA_COHORT 

) ,
BASE AS (
          SELECT
          *,
          CASE WHEN FLAG_TVM > 0 AND
          ROW_NUMBER()OVER(PARTITION BY SIT_SITE_ID,USER_ID,FLAG_TVM ORDER BY MONTH_NUMBER ASC) = MONTH_NUMBER
          THEN 'ALL_MONTH' ELSE 'NOT_RECURRENT' END AS FLAG_CONSEC,
          COUNT(DISTINCT USER_ID || SIT_SITE_ID) OVER(PARTITION BY MONTH_COHORT_ACQ,SIT_SITE_ID) AS TOTAL_USERS_COHORT

          FROM (
                SELECT
                U.USER_ID,
                U.SIT_SITE_ID,
                U.MONTH_NUMBER,
                DATE_TRUNC(U.FIRST_DAY,MONTH) AS MONTH_COHORT_ACQ,
                SUM(PLAYBACK_TIME) AS TVM,
                CASE WHEN SUM(PLAYBACK_TIME)>0 THEN 1 ELSE 0 END AS FLAG_TVM
                FROM USERS_CALENDAR AS U

                    LEFT JOIN PLAY_DAYS AS P ON U.USER_ID = P.USER_ID
                                            AND U.SIT_SITE_ID = P.SIT_SITE_ID
                                            AND P.DAY_PLAY BETWEEN U.FECHA_INI AND U.FECHA_FIN

                GROUP BY ALL
          )

)


SELECT 
SIT_SITE_ID,
MONTH_COHORT_ACQ,
MONTH_NUMBER-1 AS MONTH_RETENTION,
TOTAL_USERS_COHORT AS TOTAL_USERS_COHORT,
COUNT(DISTINCT CASE WHEN TVM > 0 THEN USER_ID||SIT_SITE_ID ELSE NULL END) AS TOTAL_USERS_RETENTION,
SUM(TVM) AS TVM,
COUNT(DISTINCT CASE WHEN TVM > 0 AND FLAG_CONSEC = 'ALL_MONTH' THEN USER_ID||SIT_SITE_ID  ELSE NULL END) AS ALL_MONTH_USER_RET,
SUM( CASE WHEN TVM > 0 AND FLAG_CONSEC = 'ALL_MONTH' THEN TVM ELSE NULL END) AS ALL_MONTH_TVM

FROM BASE
     WHERE MONTH_NUMBER IS NOT NULL
GROUP BY ALL