
-- description: Atribución de canales de marketing (MATT) para la adquisición de nuevos usuarios y su impacto en la retención (Day 1-30) y Aha Moment. Utiliza un modelo de atribución proporcional para distribuir el peso de los canales en el comportamiento posterior del usuario. 
-- domain: growth / attribution 
-- product: mplay 
-- use_case: marketing channel performance / user quality analysis 
-- grain: month_id, flag_user, sit_site_id, channel 
-- time_grain: monthly 
-- date_column: CONVERSION_CREATED_DATE 
-- date_filter: dinámico (con cambio de lógica de hash_id en 2024-11-01) 
-- threshold_rule: 
-- - Valid Play: playback_time >= 20s 
-- - Retention: Al menos una reproducción entre el día +1 y +30 después del primer play. 
-- - Aha Moment: Al menos 2 días distintos de reproducción en la ventana de 30 días. 
-- metrics: 
-- - TOTAL_USERS: Suma atribuida de usuarios (proporcional por canal). 
-- - TOTAL_USERS_RETENTION: Usuarios atribuidos que regresaron en su primer mes. 
-- - TOTAL_USERS_AHA_MOMENT: Usuarios atribuidos que alcanzaron el umbral de frecuencia (2 días+). 
-- tables_read: 
-- - growth-attribution.production.BT_MATT_FINE_TUNED_MERCADOPLAY 
-- - meli-bi-data.WHOWNER.LK_MPLAY_FIRST_PLAY 
-- - meli-bi-data.WHOWNER.LK_MPLAY_FIRST_SESSION 
-- - meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS 
-- joins: 
-- - BT_MATT LEFT JOIN LK_MPLAY (Play/Session): Para identificar al USER_ID según la fecha de conversión. 
-- - TOTAL_X_USER LEFT JOIN F (Subconsulta de métricas): Cruce de atribución con comportamiento real de retención. 
-- owner: data_team
with matt_total_channel_month as (
SELECT
M.SIT_SITE_ID,
COALESCE(c.user_id,S.USER_ID) AS USER_ID,
date_trunc(CONVERSION_CREATED_DATE,month) as month_id,
case when (MATT_PLAYER is not null and MATT_PLAYER !='') then MATT_PLAYER else MATT_TYPE_SOURCE end as channel,
ROUND(SUM(MATT_PORC_LAST_CLICK/100.0)) SUM_MATT
from growth-attribution.production.BT_MATT_FINE_TUNED_MERCADOPLAY as m
left join `meli-bi-data.WHOWNER.LK_MPLAY_FIRST_PLAY` as C ON C.first_play_hash_id = M.CONVERSION_ID
                                                          AND M.CONVERSION_CREATED_DATE <= DATE'2024-10-31'
left join `meli-bi-data.WHOWNER.LK_MPLAY_FIRST_SESSION` as S ON S.FIRST_SESSION_HASH_ID = M.CONVERSION_ID
                                                          AND M.CONVERSION_CREATED_DATE >= DATE'2024-11-01'

GROUP BY ALL  
), 

TOTAL_X_USER AS (

SELECT
M.SIT_SITE_ID,
M.USER_ID,
M.MONTH_ID,
M.CHANNEL,
M.SUM_MATT/SUM(SUM_MATT) OVER (PARTITION BY M.SIT_SITE_ID,M.USER_ID,M.MONTH_ID) AS ATTR_USER
FROM matt_total_channel_month AS M
)

SELECT
T.MONTH_ID,
CASE WHEN F.USER_ID IS NOT NULL THEN 'NEW' ELSE 'OLD' END AS FLAG_USER,
T.SIT_SITE_ID,
T.CHANNEL,
SUM(ATTR_USER) AS TOTAL_USERS,
SUM(CASE WHEN F.FLAG_RET = 1 THEN ATTR_USER ELSE NULL END) AS TOTAL_USERS_RETENTION,
SUM(CASE WHEN F.TOTAL_DAYS_DISTINCT >= 2 THEN ATTR_USER ELSE NULL END) AS TOTAL_USERS_AHA_MOMENT,

from TOTAL_X_USER AS T

left join(
SELECT
A.*,
MAX(CASE WHEN P.USER_ID IS NOT NULL THEN 1 ELSE 0 END) AS FLAG_RET,
COUNT(DISTINCT P.DS) AS TOTAL_DAYS_DISTINCT
FROM (
select
sit_site_id,
user_id,
min(ds) as fecha_new,
from `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` as P
where p.PLAYBACK_TIME_MILLISECONDS/1000>=20
group by all
) A
LEFT JOIN WHOWNER.BT_MKT_MPLAY_PLAYS AS P ON A.SIT_SITE_ID = P.SIT_SITE_ID
AND A.USER_ID = P.USER_ID
AND P.DS BETWEEN A.FECHA_NEW+1 AND A.FECHA_NEW+30
AND P.PLAYBACK_TIME_MILLISECONDS/1000 >= 20
GROUP BY ALL
)
as F on F.sit_site_id = T.sit_site_id
and F.user_id = T.USER_ID
AND DATE_TRUNC(F.fecha_new,MONTH) = T.MONTH_ID

GROUP BY ALL