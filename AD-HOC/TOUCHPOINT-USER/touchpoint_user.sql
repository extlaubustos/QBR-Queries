CREATE OR REPLACE TABLE meli-sbox.MPLAY.MPLAY_TOUCHPOINT_USER
AS
(
  DECLARE date_a_start DATE DEFAULT DATE_SUB(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL 7 DAY);
  DECLARE date_b_start DATE DEFAULT DATE_SUB(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL 14 DAY);
  DECLARE date_start_analysis DATE DEFAULT DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 MONTH);
  -- CTE para clasificar a los usuarios como NEW, RETAINED, RECOVERED
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
  -- CTE para obtener la bandera de atribución (primer play por mes)
  ATTR_TIME_FRAME_ELEGIDO AS (
      SELECT
          SIT_SITE_ID,
          USER_ID,
          TIME_FRAME_ID,
          FLAG_N_R
      FROM NEW_RET_RECO
      QUALIFY ROW_NUMBER() OVER(PARTITION BY SIT_SITE_ID,USER_ID,TIME_FRAME_ID
                                ORDER BY TIME_FRAME_ID ASC) = 1
  ),
  -- CTE 1 de la query original: Sesiones
  SESSIONS AS (
    SELECT
      s.SIT_SITE_ID,
      DATE_TRUNC(s.ds, MONTH) AS MONTH_ID,
      DATE_TRUNC(s.ds, WEEK(MONDAY)) AS fecha_week,
      s.ds,
      ORIGIN_PATH AS FIRST_EVENT_SOURCE,
      FIRST_TRACK AS FIRST_EVENT_PATH,
      FIRST_PLAY_DATETIME AS PLAY_TIMESTAMP,
      s.USER_ID,
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
    WHERE s.ds >= '2024-12-30'
      AND s.ds < CURRENT_DATE()
      AND s.SIT_SITE_ID IN UNNEST(['MLC', 'MLA', 'MLB', 'MLM', 'MCO', 'MPE', 'MLU', 'MEC'])
    GROUP BY ALL
  ),
  -- CTE 2 de la query original: Sesiones con datos de Play
  SESSION_PLAY AS (
    SELECT DISTINCT
      s.SIT_SITE_ID,
      s.MONTH_ID,
      s.fecha_week,
      S.DS,
      s.FIRST_EVENT_SOURCE,
      s.FIRST_EVENT_PATH,
      s.PLAY_TIMESTAMP,
      s.USER_ID,
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
  )
  -- Consulta final integrada
  SELECT
      s.sit_site_id,
      DATE_TRUNC(s.DS, MONTH) AS MONTH_ID,
      DATE_TRUNC(s.DS, WEEK(MONDAY)) AS WEEK_ID,
      CASE
          WHEN S.DEVICE_PLATFORM IN ('/tv/android') THEN '/tv/android'
          WHEN S.DEVICE_PLATFORM IN ('/tv/Tizen') THEN '/tv/Tizen'
          WHEN S.DEVICE_PLATFORM IN ('/tv/Web0S') THEN '/tv/Web0S'
          ELSE COALESCE(o.SOURCE_TYPE, 'Otros')
      END AS Origin,
      CASE 
          WHEN UPPER(S.DEVICE_PLATFORM) LIKE '%TV%' THEN 'SMART'
          WHEN UPPER(S.DEVICE_PLATFORM) LIKE '%MOBILE%' THEN 'MOBILE'
          WHEN UPPER(S.DEVICE_PLATFORM) LIKE '%DESK%' THEN 'DESKTOP'
          ELSE 'OTHER'
      END AS PLATFORM,
      -- Nuevos campos de clasificación
      COALESCE(e.FLAG_N_R, 'Undefined') AS User_Classification,
      CONCAT(o.SOURCE_SESSION_L1,'-', o.SOURCE_SESSION_L2, '-', o.TEAM) AS touchpoint_team,
      CONCAT(o.SOURCE_SESSION_L1,'-', o.SOURCE_SESSION_L2) AS touchpoint_no_team,
      o.SOURCE_SESSION_L1 AS Clasificacion,
      o.SOURCE_SESSION_L2 AS Clasificacion_2,
      o.TEAM AS Team,
      -- Métricas
      COUNT(DISTINCT s.melidata_session_id) Sessions,
      COUNT(DISTINCT CASE WHEN s.FLAG_VALID_VISIT IS TRUE THEN s.melidata_session_id ELSE NULL END) AS Sessions_valid_visit,
      COUNT(DISTINCT CASE WHEN s.TSV >= 20 THEN s.melidata_session_id ELSE NULL END) AS Sessions_valid_view,
      ROUND(SUM(s.TVM),2) AS TVM,
      COUNT(DISTINCT s.USER_ID) Visitors,
      COUNT(DISTINCT CASE WHEN s.FLAG_VALID_VISIT IS TRUE THEN s.USER_ID ELSE NULL END) AS Valid_Visitors,
      COUNT(DISTINCT CASE WHEN s.TSV >= 20 THEN s.USER_ID ELSE NULL END) AS Viewers
  FROM SESSION_PLAY s
  LEFT JOIN `meli-sbox.MPLAY.LK_MPLAY_SOURCE_TYPE_ORIGIN_SESSION` o
      ON COALESCE(s.FIRST_EVENT_SOURCE, 'NULL') = COALESCE(o.SOURCE_TYPE, 'NULL')
  -- LEFT JOIN para unir la clasificación de usuario
  LEFT JOIN ATTR_TIME_FRAME_ELEGIDO e
      ON s.SIT_SITE_ID = e.SIT_SITE_ID
      AND s.USER_ID = e.USER_ID
      AND s.MONTH_ID = e.TIME_FRAME_ID
  WHERE WEEK_ID IN (date_a_start, date_b_start)
  GROUP BY
      s.sit_site_id,
      MONTH_ID,
      WEEK_ID,
      Origin,
      User_Classification,
      touchpoint_team,
      touchpoint_no_team,
      Clasificacion,
      Clasificacion_2,
      Team, 
      PLATFORM
  ORDER BY WEEK_ID ASC
);
