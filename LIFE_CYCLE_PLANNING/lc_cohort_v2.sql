-- ========================
-- 1) Plays base
-- ========================
WITH PLAY_DAYS AS (
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
-- 2) Primera atribución diaria (Channel y Platform)
-- =========================================
FIRST_ATTRIB_DAILY AS (
    SELECT
        P.SIT_SITE_ID,
        P.USER_ID,
        P.DS AS DAY_PLAY,
        CASE 
            WHEN UPPER(P.DEVICE_PLATFORM) LIKE '%TV%' THEN 'SMART'
            WHEN UPPER(P.DEVICE_PLATFORM) LIKE '%MOBILE%' THEN 'MOBILE'
            WHEN UPPER(P.DEVICE_PLATFORM) LIKE '%DESK%' THEN 'DESKTOP'
            ELSE 'OTHER'
        END AS PLATFORM,
        CASE
            WHEN (MATT_PLAYER IS NOT NULL AND MATT_PLAYER != '') THEN MATT_PLAYER
            ELSE P.MATT_TYPE_SOURCE
        END AS CHANNEL
    FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` P
    LEFT JOIN `growth-attribution.production.BT_MATT_FINE_TUNED_MERCADOPLAY` M
        ON M.CONVERSION_ID = P.USER_ID  -- Ajustar si hay otra relación real
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY P.SIT_SITE_ID, P.USER_ID, P.DS
        ORDER BY P.DS ASC
    ) = 1
),

-- =========================================
-- 3) Primer día de reproducción por usuario
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
                ON T.TIM_DAY+1 <= T2.TIM_DAY
            WHERE T.TIM_DAY >= DATE'2023-07-01'
            QUALIFY MOD(ROW_NUMBER() OVER(PARTITION BY T.TIM_DAY ORDER BY T2.TIM_DAY ASC), 30) = 0
        ) AS A
        WHERE FECHA_FIN <= DATE_TRUNC(CURRENT_DATE-1, MONTH) - 1
    ) AS B
    QUALIFY COUNT(FECHA_FIN) OVER (PARTITION BY DATE_TRUNC(FECHA_COHORT, MONTH), MONTH_NUMBER) = EXTRACT(DAY FROM(LAST_DAY(FECHA_COHORT)))
),

-- =========================================
-- 5) Usuarios con calendario y primeras atribuciones
-- =========================================
USERS_CALENDAR AS (
    SELECT 
        PF.*,
        T.FECHA_INI,
        T.FECHA_FIN,
        T.MONTH_NUMBER,
        FAD.PLATFORM,
        FAD.CHANNEL
    FROM PLAY_FIRST_DAY PF
    LEFT JOIN TABLE_CALENDAR T 
        ON PF.FIRST_DAY = T.FECHA_COHORT
    LEFT JOIN FIRST_ATTRIB_DAILY FAD
        ON PF.SIT_SITE_ID = FAD.SIT_SITE_ID
        AND PF.USER_ID = FAD.USER_ID
        AND PF.FIRST_DAY = FAD.DAY_PLAY
),

-- =========================================
-- 6) Base con métricas por cohort
-- =========================================
BASE AS (
    SELECT
        U.SIT_SITE_ID,
        U.USER_ID,
        U.MONTH_NUMBER,
        DATE_TRUNC(U.FIRST_DAY, MONTH) AS MONTH_COHORT_ACQ,
        SUM(P.PLAYBACK_TIME) AS TVM,
        CASE WHEN SUM(P.PLAYBACK_TIME) > 0 THEN 1 ELSE 0 END AS FLAG_TVM,
        U.PLATFORM,
        U.CHANNEL
    FROM USERS_CALENDAR U
    LEFT JOIN PLAY_DAYS P 
        ON U.USER_ID = P.USER_ID
        AND U.SIT_SITE_ID = P.SIT_SITE_ID
        AND P.DAY_PLAY BETWEEN U.FECHA_INI AND U.FECHA_FIN
    GROUP BY U.SIT_SITE_ID, U.USER_ID, U.MONTH_NUMBER, U.FIRST_DAY, U.PLATFORM, U.CHANNEL
)

-- =========================================
-- 7) Query final con cohort + channel + platform
-- =========================================
SELECT 
    SIT_SITE_ID,
    MONTH_COHORT_ACQ,
    MONTH_NUMBER - 1 AS MONTH_RETENTION,
    PLATFORM,
    COALESCE(C.CHANNEL, 'UNKNOWN') AS CHANNEL,
    COUNT(DISTINCT USER_ID || SIT_SITE_ID) AS TOTAL_USERS_COHORT,
    COUNT(DISTINCT CASE WHEN TVM > 0 THEN USER_ID || SIT_SITE_ID ELSE NULL END) AS TOTAL_USERS_RETENTION,
    SUM(TVM) AS TVM,
    COUNT(DISTINCT CASE WHEN TVM > 0 AND FLAG_TVM = 1 THEN USER_ID || SIT_SITE_ID ELSE NULL END) AS ALL_MONTH_USER_RET,
    SUM(CASE WHEN TVM > 0 AND FLAG_TVM = 1 THEN TVM ELSE NULL END) AS ALL_MONTH_TVM
FROM BASE
WHERE MONTH_NUMBER IS NOT NULL
GROUP BY SIT_SITE_ID, MONTH_COHORT_ACQ, MONTH_NUMBER, PLATFORM, CHANNEL

