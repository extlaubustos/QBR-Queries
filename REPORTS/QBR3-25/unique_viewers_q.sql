WITH BASE AS (
    SELECT
    DS,
    USER_ID,
    CONCAT(
        CAST(EXTRACT(YEAR FROM DS) AS STRING),
        FORMAT("%02d", EXTRACT(QUARTER FROM DS))
    ) AS yyyyqq
    FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS`

    WHERE  PLAYBACK_TIME_MILLISECONDS/1000 >= 20 
    and ds between '2024-01-01'
      AND DS <= CURRENT_DATE-1
)
SELECT DISTINCT
    yyyyqq,
    count(distinct user_id) as total_unique_viewers
from base