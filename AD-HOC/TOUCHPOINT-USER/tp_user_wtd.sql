INSERT INTO meli-sbox.MPLAY.MPLAY_TOUCHPOINT_USER AS 
(
 WITH NEW_RET_RECO AS
  (
      SELECT
          SIT_SITE_ID,
          USER_ID,
          DATE_TRUNC(DS, MONTH) AS TIME_FRAME_ID,
          (CASE WHEN (LAG(DS,1)OVER(PARTITION BY SIT_SITE_ID,USER_ID ORDER BY START_PLAY_TIMESTAMP ASC)) IS NULL THEN 'NEW'
                WHEN DATE_DIFF(DS, (LAG(DS,1)OVER(PARTITION BY SIT_SITE_ID,USER_ID ORDER BY START_PLAY_TIMESTAMP ASC)), DAY) <= 30 THEN 'RETAINED'
                WHEN DATE_DIFF(DS, (LAG(DS,1)OVER(PARTITION BY SIT_SITE_ID,USER_ID ORDER BY START_PLAY_TIMESTAMP ASC)), DAY) > 30 THEN 'RECOVERED'
                ELSE NULL END) AS FLAG_N_R
      FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS`
      WHERE PLAYBACK_TIME_MILLISECONDS/1000 >= 20
          AND DS <= CURRENT_DATE-1
  ),
  ATTR_TIME_FRAME_ELEGIDO AS (
      SELECT
          SIT_SITE_ID,
          USER_ID,
          TIME_FRAME_ID,
          FLAG_N_R
      FROM NEW_RET_RECO
      QUALIFY ROW_NUMBER() OVER(PARTITION BY SIT_SITE_ID,USER_ID,TIME_FRAME_ID ORDER BY TIME_FRAME_ID ASC) = 1
  ),
  SESSIONS AS (
    SELECT
      s.SIT_SITE_ID,
      s.ds,
      DATE_TRUNC(s.ds, WEEK(MONDAY)) AS WEEK_ID,
      ORIGIN_PATH AS FIRST_EVENT_SOURCE,
      FIRST_TRACK AS FIRST_EVENT_PATH,
      FIRST_PLAY_DATETIME AS PLAY_TIMESTAMP,
      s.USER_ID,
      A.FLAG_N_R AS FLAG_N_R,
      s.SESSION_ID AS MELIDATA_SESSION_ID,
      s.DEVICE_PLATFORM,
      IF(
        (
          (S.HAS_SEARCH IS TRUE OR S.HAS_VCP IS TRUE OR S.HAS_VCM IS TRUE OR HAS_PLAY IS TRUE)
          OR TOTAL_FEED_IMPRESSIONS > 1
        ),
        TRUE,
        FALSE
      ) AS FLAG_VALID_VISIT,
      HAS_PLAY,
      S.TOTAL_SESSION_MILLISECOND / 1000 AS session_time_sec
    FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_SESSION` AS s
    LEFT JOIN ATTR_TIME_FRAME_ELEGIDO AS A
      ON s.SIT_SITE_ID = A.SIT_SITE_ID
      AND s.USER_ID = A.USER_ID
      AND DATE_TRUNC(s.ds, MONTH) = A.TIME_FRAME_ID
    WHERE s.ds = CURRENT_DATE() - 1
      AND s.SIT_SITE_ID IN UNNEST(['MLC', 'MLA', 'MLB', 'MLM', 'MCO', 'MPE', 'MLU', 'MEC'])
    GROUP BY ALL
  ),
  SESSION_PLAY AS (
    SELECT DISTINCT
      s.SIT_SITE_ID,
      S.DS,
      S.WEEK_ID,
      s.FIRST_EVENT_SOURCE,
      s.FIRST_EVENT_PATH,
      s.PLAY_TIMESTAMP,
      s.USER_ID,
      S.FLAG_N_R,
      s.MELIDATA_SESSION_ID,
      s.FLAG_VALID_VISIT,
      s.HAS_PLAY,
      s.DEVICE_PLATFORM,
      s.session_time_sec,
      SUM(P.PLAYBACK_TIME_MILLISECONDS / 1000) AS TSV,
      SUM(P.PLAYBACK_TIME_MILLISECONDS / 60000) AS TVM
    FROM SESSIONS AS S
    LEFT JOIN `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` AS P
      ON S.SIT_SITE_ID = P.SIT_SITE_ID
      AND s.USER_ID = P.USER_ID
      AND S.MELIDATA_SESSION_ID = P.SESSION_ID
      AND P.PLAYBACK_TIME_MILLISECONDS / 1000 >= 20
    GROUP BY ALL
  ),
  BASE_MPLAY_DAILY AS (
    SELECT
      'DAILY' AS timeframe_type,
      s.DS AS timeframe_id,
      s.sit_site_id,
      CASE
          WHEN S.DEVICE_PLATFORM IN ('/tv/android') THEN '/tv/android'
          WHEN S.DEVICE_PLATFORM IN ('/tv/Tizen') THEN '/tv/Tizen'
          WHEN S.DEVICE_PLATFORM IN ('/tv/Web0S') THEN '/tv/Web0S'
          ELSE COALESCE(o.origin, 'Otros')
      END AS Origin,
      CASE 
          WHEN UPPER(S.DEVICE_PLATFORM) LIKE '%TV%' THEN 'SMART'
          WHEN UPPER(S.DEVICE_PLATFORM) LIKE '%MOBILE%' THEN 'MOBILE'
          WHEN UPPER(S.DEVICE_PLATFORM) LIKE '%DESK%' THEN 'DESKTOP'
          ELSE 'OTHER'
      END AS PLATFORM,
      COALESCE(s.FLAG_N_R, 'No definido') AS User_Classification,
      oc.TEAM AS team,
      COUNT(DISTINCT s.melidata_session_id) AS Sessions,
      COUNT(DISTINCT CASE WHEN s.FLAG_VALID_VISIT IS TRUE THEN s.melidata_session_id ELSE NULL END) AS Sessions_valid_visit,
      COUNT(DISTINCT CASE WHEN s.TSV >= 20 THEN s.melidata_session_id ELSE NULL END) AS Sessions_valid_view,
      ROUND(SUM(s.TVM),2) AS TVM,
      COUNT(DISTINCT s.USER_ID) AS Visitors,
      COUNT(DISTINCT CASE WHEN s.FLAG_VALID_VISIT IS TRUE THEN s.USER_ID ELSE NULL END) AS Valid_Visitors,
      COUNT(DISTINCT CASE WHEN s.TSV >= 20 THEN s.USER_ID ELSE NULL END) AS Viewers
    FROM SESSION_PLAY s
    LEFT JOIN `meli-sbox.MPLAY.CLASIFICATION_ORIGINS` o
      ON COALESCE(s.FIRST_EVENT_SOURCE, 'NULL') = COALESCE(o.origin, 'NULL')
    LEFT JOIN `meli-sbox.MPLAY.LK_MPLAY_SOURCE_TYPE_ORIGIN_SESSION` oc
      ON COALESCE(s.FIRST_EVENT_SOURCE, 'NULL') = COALESCE(oc.SOURCE_TYPE, 'NULL')
    GROUP BY
      s.sit_site_id,
      s.DS,
      Origin,
      User_Classification,
      PLATFORM,
      TEAM
  )
SELECT
  b.timeframe_type,
  b.timeframe_id,
  b.sit_site_id,
  b.Origin,
  b.User_Classification,
  CONCAT(o.Clasificacion, '-', o.Subclasificacion, '-', team) AS touchpoint_team,
  CONCAT(o.Clasificacion, '-', o.Subclasificacion) AS touchpoint_no_team,
  o.Clasificacion AS Clasificacion,
  o.Subclasificacion AS Clasificacion_2,
  b.team,
  b.platform, 
  b.Sessions,
  b.Sessions_valid_visit,
  b.Sessions_valid_view,
  b.TVM,
  b.Visitors,
  b.Valid_Visitors,
  b.Viewers
FROM BASE_MPLAY_DAILY b
LEFT JOIN `meli-sbox.MPLAY.CLASIFICATION_ORIGINS` o
  ON b.Origin = o.origin
)