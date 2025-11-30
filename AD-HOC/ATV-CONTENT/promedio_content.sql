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