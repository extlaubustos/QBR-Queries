WITH retained_users_current_month AS (
    -- 1. Usuarios que fueron RETAINED en el mes actual (M)
    SELECT
        USER_ID,
        DATE_TRUNC(TIM_DAY, MONTH) AS MES_ACTUAL,
        SIT_SITE_ID
    FROM `meli-bi-data.WHOWNER.DM_MKT_MPLAY_RAW_PLAYS`
    WHERE 
        LIFE_CYCLE = 'RETAINED'
        AND TIME_FRAME = 'MONTHLY'
        AND TIM_DAY BETWEEN '2025-01-01' AND CURRENT_DATE - 1
),
new_users_previous_month AS (
    -- 2. Usuarios que fueron NEW en el mes anterior (M-1)
    SELECT
        USER_ID,
        DATE_TRUNC(TIM_DAY, MONTH) AS MES_PREVIO,
        SIT_SITE_ID
    FROM `meli-bi-data.WHOWNER.DM_MKT_MPLAY_RAW_PLAYS`
    WHERE 
        LIFE_CYCLE = 'NEW' -- 🎯 Añadimos el filtro LIFE_CYCLE = 'NEW' aquí
        AND TIME_FRAME = 'MONTHLY'
        AND TIM_DAY BETWEEN '2025-01-01' AND CURRENT_DATE - 1
)
-- 3. Unir para encontrar la intersección: Retenido en M que fue NEW en M-1
,
base as (
  SELECT
    RC.USER_ID,
    RC.MES_ACTUAL, -- Mes en el que fue clasificado como RETAINED
    RC.SIT_SITE_ID
FROM retained_users_current_month RC
INNER JOIN new_users_previous_month NP
    ON RC.USER_ID = NP.USER_ID
    AND RC.SIT_SITE_ID = NP.SIT_SITE_ID
    -- Condición clave: MES_ACTUAL debe ser exactamente un mes después de MES_PREVIO
    AND RC.MES_ACTUAL = DATE_ADD(NP.MES_PREVIO, INTERVAL 1 MONTH)

ORDER BY RC.MES_ACTUAL, RC.USER_ID
)
select 
mes_actual as month_year,
SIT_SITE_ID,
COUNT(DISTINCT USER_ID) AS retained_new_users_count
from base