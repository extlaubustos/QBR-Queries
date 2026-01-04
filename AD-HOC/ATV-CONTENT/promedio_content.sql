-- description: Promedio de reproducciones semanales por usuario, agregado a nivel mensual
-- domain: behaviour
-- product: mplay
-- use_case: analysis
-- grain: month
-- time_grain: monthly
-- date_column: plays.DS
-- date_filter: between
-- threshold_rule: none
-- metrics:
--   - TOTAL_PLAYS: cantidad total de reproducciones
--   - UNIQUE_USERS: usuarios únicos con al menos una reproducción
--   - WEEKS_IN_MONTH: semanas estimadas por mes calendario
--   - AVG_PLAYS_PER_USER_WEEKLY: promedio de reproducciones por usuario por semana
-- tables_read:
--   - WHOWNER.BT_MKT_MPLAY_PLAYS
--   - WHOWNER.LK_MKT_MPLAY_CATALOGUE
-- joins:
--   - PLAYS.SIT_SITE_ID = CATALOGUE.SIT_SITE_ID
--   - PLAYS.CONTENT_ID = CATALOGUE.CONTENT_ID
-- owner: data_team

SELECT
FORMAT_DATE('%Y %m', plays.DS) AS month_year,
COUNT(*) AS total_plays,
COUNT(DISTINCT plays.USER_ID) AS unique_users,
(EXTRACT(DAY FROM LAST_DAY(MAX(plays.DS))) / 7.0) AS weeks_in_month,
COUNT(*) / (
        COUNT(DISTINCT plays.USER_ID) * (EXTRACT(DAY FROM LAST_DAY(MAX(plays.DS))) / 7.0)
    ) AS avg_plays_per_user_weekly
FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` AS plays
LEFT JOIN `meli-bi-data.WHOWNER.LK_MKT_MPLAY_CATALOGUE` AS catalogue
    ON plays.SIT_SITE_ID = catalogue.SIT_SITE_ID
    AND plays.CONTENT_ID = catalogue.CONTENT_ID
WHERE
    plays.DS >= DATE('2025-01-01')
    AND plays.DS < DATE_ADD(DATE('2025-01-01'), INTERVAL 1 YEAR)
GROUP BY 1
ORDER BY 1 DESC