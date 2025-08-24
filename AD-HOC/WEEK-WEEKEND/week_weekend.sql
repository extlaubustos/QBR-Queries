WITH user_view_patterns AS (
    SELECT
        SIT_SITE_ID,
        USER_ID,
        DATE_TRUNC(DS, MONTH) AS time_frame_id,
        MAX(CASE WHEN EXTRACT(DAYOFWEEK FROM DS) IN (2, 3, 4, 5) THEN 1 ELSE 0 END) AS has_weekday_views,
        MAX(CASE WHEN EXTRACT(DAYOFWEEK FROM DS) IN (1, 6, 7) THEN 1 ELSE 0 END) AS has_weekend_views,
        COUNT(DISTINCT DS) AS total_view_days,
        SUM(PLAYBACK_TIME_MILLISECONDS / 60000) AS total_tvm
    FROM
        `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS`
    WHERE
        PLAYBACK_TIME_MILLISECONDS / 1000 >= 20
        AND DS <= CURRENT_DATE - 1
    GROUP BY
        SIT_SITE_ID,
        USER_ID,
        time_frame_id
)

SELECT
    SIT_SITE_ID,
    time_frame_id,
    total_view_days,
    CASE
        WHEN has_weekday_views = 1 AND has_weekend_views = 1 THEN 'Both'
        WHEN has_weekday_views = 1 THEN 'Semana'
        WHEN has_weekend_views = 1 THEN 'Finde'
    END AS week_frame_classification,
    COUNT(DISTINCT USER_ID) AS total_users,
    round(AVG(total_view_days), 2) AS average_frequency,
    round(SUM(total_tvm), 2) AS TVM,
    round(sum(total_tvm) / count(distinct user_id), 2) as ATV
FROM
    user_view_patterns
GROUP BY
    SIT_SITE_ID,
    time_frame_id,
    week_frame_classification,
    total_view_days
ORDER BY
    SIT_SITE_ID,
    time_frame_id,
    week_frame_classification;