-- description: Cohortes de usuarios "NEW" (primer día de reproducción) con métricas de reproducción (TVM) por plataforma y mes de cohorte, incluyendo retención mensual.
-- domain: behaviour
-- product: mplay
-- use_case: cohort_analysis / retention_reporting
-- grain: SIT_SITE_ID, USER_ID, MONTH_COHORT_ACQ, PLATFORM
-- time_grain: daily / monthly cohort
-- date_column: DS / FIRST_DAY
-- date_filter: between
-- threshold_rule: PLAYBACK_TIME_MILLISECONDS >= 20000
-- metrics:
-- - TVM: minutos reproducidos por usuario en el periodo
-- - TOTAL_USERS_COHORT: cantidad de usuarios en la cohorte
-- - TOTAL_USERS_RETENTION: cantidad de usuarios con consumo en el mes
-- - ALL_MONTH_USER_RET: usuarios activos en todo el mes
-- - ALL_MONTH_TVM: minutos reproducidos por usuarios activos todo el mes
-- tables_read:
-- - WHOWNER.BT_MKT_MPLAY_PLAYS
-- - WHOWNER.LK_TIM_DAYS
-- joins:
-- - PLAY_FIRST_DAY LEFT JOIN TABLE_CALENDAR ON FIRST_DAY = FECHA_COHORT
-- - PLAY_FIRST_DAY LEFT JOIN PLATFORM_DATA ON SIT_SITE_ID, USER_ID, MONTH
-- - BASE LEFT JOIN PLAY_DAYS ON USER_ID, SIT_SITE_ID, DAY_PLAY BETWEEN FECHA_INI AND FECHA_FIN
-- owner: data_team

WITH
-- ========================
-- 1) Plays base
-- ========================
PLAY_DAYS AS (
    SELECT
        P.SIT_SITE_ID,
        P.USER_ID,
        P.DS AS DAY_PLAY,
        SUM(P.PLAYBACK_TIME_MILLISECONDS/60000) AS PLAYBACK_TIME
    FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` P
    WHERE P.PLAYBACK_TIME_MILLISECONDS/1000 >= 20
    GROUP BY ALL
),

-- =========================================
-- 2) Primer día de reproducción por usuario (Cohorte NEW)
-- =========================================
PLAY_FIRST_DAY AS (
    SELECT
        SIT_SITE_ID,
        USER_ID,
        MIN(DAY_PLAY) AS FIRST_DAY
    FROM PLAY_DAYS
    GROUP BY ALL
),

-- =========================================
-- 3) Atribución de plataforma según tu lógica
-- =========================================
DATA_USERS AS (
    SELECT
        SIT_SITE_ID,
        USER_ID,
        DS,
        SUM(PLAYBACK_TIME_MILLISECONDS/60000) AS TVM,
        SUM(CASE WHEN UPPER(DEVICE_PLATFORM) LIKE '%TV%' THEN PLAYBACK_TIME_MILLISECONDS/60000 ELSE 0 END) AS TOTAL_TV,
        SUM(CASE WHEN UPPER(DEVICE_PLATFORM) LIKE '%MOBILE%' THEN PLAYBACK_TIME_MILLISECONDS/60000 ELSE 0 END) AS TOTAL_MOBILE,
        SUM(CASE WHEN UPPER(DEVICE_PLATFORM) LIKE '%DESK%' THEN PLAYBACK_TIME_MILLISECONDS/60000 ELSE 0 END) AS TOTAL_DESKTOP,
        SUM(CASE WHEN PLAYBACK_TIME_MILLISECONDS_CAST/1000 >= 20 THEN PLAYBACK_TIME_MILLISECONDS_CAST/60000 ELSE 0 END) AS TOTAL_CAST
    FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS`
    WHERE PLAYBACK_TIME_MILLISECONDS/1000 >= 20
      AND DS <= CURRENT_DATE - 1
    GROUP BY ALL
),

TABLE_CALENDAR_PLATFORM AS (
    SELECT
        DATE_TRUNC(DS, MONTH) AS MONTH_ID,
        SIT_SITE_ID,
        USER_ID,
        SUM(TVM) AS TVM,
        SUM(TOTAL_TV) AS TVM_TV,
        SUM(TOTAL_MOBILE) AS TVM_MOBILE,
        SUM(TOTAL_DESKTOP) AS TVM_DESKTOP,
        SUM(TOTAL_CAST) AS TVM_CAST
    FROM DATA_USERS
    GROUP BY ALL
),

PLATFORM_DATA AS (
    SELECT
        SIT_SITE_ID,
        USER_ID,
        MONTH_ID,
        CASE
            WHEN ROUND(TVM, 2) = ROUND(TVM_CAST, 2) AND TVM_CAST > 0 THEN 'CAST'
            ELSE CONCAT(
                CASE WHEN TVM_TV > 0 THEN 'SMART' ELSE '' END, ' - ',
                CASE WHEN TVM_MOBILE > 0 THEN 'MOBILE' ELSE '' END, ' - ',
                CASE WHEN TVM_DESKTOP > 0 THEN 'DESKTOP' ELSE '' END, ' - ',
                CASE WHEN TVM_CAST > 0 THEN 'CAST' ELSE '' END
            )
        END AS PLATFORM_CONCAT
    FROM TABLE_CALENDAR_PLATFORM
),

-- =========================================
-- 4) Tabla calendario de cohortes
-- =========================================
TABLE_CALENDAR AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER(PARTITION BY FECHA_COHORT ORDER BY FECHA_INI ASC) AS MONTH_NUMBER
        FROM (
            SELECT
                T.TIM_DAY AS FECHA_COHORT,
                T2.TIM_DAY - 29 AS FECHA_INI,
                T2.TIM_DAY AS FECHA_FIN
            FROM `meli-bi-data.WHOWNER.LK_TIM_DAYS` T
            LEFT JOIN `meli-bi-data.WHOWNER.LK_TIM_DAYS` T2
                ON T.TIM_DAY + 1 <= T2.TIM_DAY
            WHERE T.TIM_DAY >= DATE'2023-07-01'
            QUALIFY MOD(ROW_NUMBER() OVER(PARTITION BY T.TIM_DAY ORDER BY T2.TIM_DAY ASC), 30) = 0
        ) AS A
        WHERE FECHA_FIN <= DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 DAY)
    ) AS B
    QUALIFY COUNT(FECHA_FIN) OVER (PARTITION BY DATE_TRUNC(FECHA_COHORT, MONTH), MONTH_NUMBER) = EXTRACT(DAY FROM(LAST_DAY(FECHA_COHORT)))
),


-- =========================================
-- 5) Usuarios con calendario, atribuciones de plataforma y canal
-- =========================================
USERS_CALENDAR AS (
    SELECT
        PF.*,
        T.FECHA_INI,
        T.FECHA_FIN,
        T.MONTH_NUMBER,
        P.PLATFORM_CONCAT
    FROM PLAY_FIRST_DAY PF
    LEFT JOIN TABLE_CALENDAR T
        ON PF.FIRST_DAY = T.FECHA_COHORT
    LEFT JOIN PLATFORM_DATA P
        ON PF.SIT_SITE_ID = P.SIT_SITE_ID
        AND PF.USER_ID = P.USER_ID
        AND DATE_TRUNC(PF.FIRST_DAY, MONTH) = P.MONTH_ID
),

-- =========================================
-- 6) Base con métricas por cohorte y canal
-- =========================================
BASE AS (
    SELECT
        U.SIT_SITE_ID,
        U.USER_ID,
        U.MONTH_NUMBER,
        DATE_TRUNC(U.FIRST_DAY, MONTH) AS MONTH_COHORT_ACQ,
        SUM(P.PLAYBACK_TIME) AS TVM,
        CASE WHEN SUM(P.PLAYBACK_TIME) > 0 THEN 1 ELSE 0 END AS FLAG_TVM,
        U.PLATFORM_CONCAT,

    FROM USERS_CALENDAR U
    LEFT JOIN PLAY_DAYS P
        ON U.USER_ID = P.USER_ID
        AND U.SIT_SITE_ID = P.SIT_SITE_ID
        AND P.DAY_PLAY BETWEEN U.FECHA_INI AND U.FECHA_FIN
    GROUP BY U.SIT_SITE_ID, U.USER_ID, U.MONTH_NUMBER, U.FIRST_DAY, U.PLATFORM_CONCAT
)

-- =========================================
-- 7) Query final con cohorte + plataforma + canal
-- =========================================
SELECT
    SIT_SITE_ID,
    MONTH_COHORT_ACQ,
    MONTH_NUMBER - 1 AS MONTH_RETENTION,
    PLATFORM_CONCAT AS PLATFORM,
    COUNT(DISTINCT USER_ID || SIT_SITE_ID) AS TOTAL_USERS_COHORT,
    COUNT(DISTINCT CASE WHEN TVM > 0 THEN USER_ID || SIT_SITE_ID ELSE NULL END) AS TOTAL_USERS_RETENTION,
    SUM(TVM) AS TVM,
    COUNT(DISTINCT CASE WHEN TVM > 0 AND FLAG_TVM = 1 THEN USER_ID || SIT_SITE_ID ELSE NULL END) AS ALL_MONTH_USER_RET,
    SUM(CASE WHEN TVM > 0 AND FLAG_TVM = 1 THEN TVM ELSE NULL END) AS ALL_MONTH_TVM
FROM BASE
WHERE MONTH_NUMBER IS NOT NULL
GROUP BY SIT_SITE_ID, MONTH_COHORT_ACQ, MONTH_NUMBER, PLATFORM_CONCAT
ORDER BY MONTH_COHORT_ACQ, MONTH_RETENTION, PLATFORM;