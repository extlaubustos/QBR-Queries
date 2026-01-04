-- description: Análisis de retención de largo plazo para usuarios recuperados (RECOVERED). Rastrea el comportamiento posterior de los usuarios que regresaron al producto después de una inactividad >30 días, midiendo cuántos de ellos se mantienen activos en los meses siguientes. 
-- domain: behaviour 
-- product: mplay 
-- use_case: recovered user retention / long-term loyalty analysis 
-- grain: sit_site_id, recovery_month, play_month 
-- time_grain: monthly 
-- date_column: DS 
-- date_filter: <= CURRENT_DATE - 1 
-- threshold_rule: playback_time >= 20s 
-- metrics: 
-- - recovered_users_who_returned: cantidad de usuarios únicos recuperados que presentan actividad en meses posteriores al de su recuperación. 
-- tables_read: 
-- - meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS 
-- joins: 
-- - recovered_users (ru) JOIN all_future_plays (afp): Cruce de la cohorte de recuperación con toda su actividad futura para medir persistencia. 
-- owner: data_team
WITH user_classifications AS (
    SELECT
        SIT_SITE_ID,
        USER_ID,
        DATE_TRUNC(DS, MONTH) AS time_frame_id,
        (
            CASE 
                WHEN LAG(DS, 1) OVER(PARTITION BY SIT_SITE_ID, USER_ID ORDER BY START_PLAY_TIMESTAMP ASC) IS NULL THEN 'NEW'
                WHEN DATE_DIFF(DS, LAG(DS, 1) OVER(PARTITION BY SIT_SITE_ID, USER_ID ORDER BY START_PLAY_TIMESTAMP ASC), DAY) <= 30 THEN 'RETAINED'
                WHEN DATE_DIFF(DS, LAG(DS, 1) OVER(PARTITION BY SIT_SITE_ID, USER_ID ORDER BY START_PLAY_TIMESTAMP ASC), DAY) > 30 THEN 'RECOVERED'
                ELSE NULL 
            END
        ) AS flag_n_r
    FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS`
    WHERE PLAYBACK_TIME_MILLISECONDS / 1000 >= 20
      AND DS <= CURRENT_DATE - 1
),
first_user_month_classification AS (
    SELECT
        SIT_SITE_ID,
        USER_ID,
        time_frame_id,
        flag_n_r AS flag_n_r_final
    FROM user_classifications
    QUALIFY ROW_NUMBER() OVER(PARTITION BY SIT_SITE_ID, USER_ID, time_frame_id ORDER BY time_frame_id ASC) = 1
),
recovered_users AS (
    SELECT DISTINCT
        SIT_SITE_ID,
        USER_ID,
        time_frame_id AS recovery_month
    FROM first_user_month_classification
    WHERE flag_n_r_final = 'RECOVERED'
),
all_future_plays AS (
    SELECT DISTINCT
        SIT_SITE_ID,
        USER_ID,
        DATE_TRUNC(DS, MONTH) AS play_month
    FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS`
    WHERE PLAYBACK_TIME_MILLISECONDS / 1000 >= 20
      AND DS <= CURRENT_DATE - 1
)
SELECT
    ru.SIT_SITE_ID,
    ru.recovery_month,
    afp.play_month,
    COUNT(DISTINCT ru.USER_ID) AS recovered_users_who_returned
FROM recovered_users AS ru
JOIN all_future_plays AS afp
    ON ru.USER_ID = afp.USER_ID
    AND ru.SIT_SITE_ID = afp.SIT_SITE_ID
    AND afp.play_month > ru.recovery_month
GROUP BY 
    1, 2, 3
ORDER BY
    ru.SIT_SITE_ID,
    ru.recovery_month,
    afp.play_month;