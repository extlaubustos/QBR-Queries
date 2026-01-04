-- description: Inserción mensual de usuarios MPlay por touchpoint, agregación para el mes anterior completo.
-- domain: media_analytics
-- product: mplay
-- use_case: monthly_user_touchpoint_load
-- grain: site, month, user_classification, origin, platform
-- time_grain: monthly
-- date_column: DS
-- date_filter: DS >= primer día del mes pasado AND DS < primer día del mes actual
-- metrics:
-- - Sessions: total de sesiones
-- - Sessions_valid_visit: sesiones válidas
-- - Sessions_valid_view: sesiones con reproducción válida
-- - TVM: tiempo total de reproducción en minutos
-- - Visitors: total de usuarios distintos
-- - Valid_Visitors: usuarios con visitas válidas
-- - Viewers: usuarios con reproducción válida
-- dimensions:
-- - timeframe_type: MONTHLY
-- - timeframe_id: primer día del mes pasado
-- - sit_site_id: identificador de sitio
-- - MONTH_ID: mes
-- - WEEK_ID: NULL
-- - Origin: origen del primer evento
-- - User_Classification: NEW, RETAINED, RECOVERED
-- - touchpoint_team: combinación Clasificacion-Subclasificacion-Team
-- - touchpoint_no_team: combinación Clasificacion-Subclasificacion
-- - Clasificacion, Clasificacion_2: categoría de touchpoint
-- - team: equipo asignado
-- - platform: SMART, MOBILE, DESKTOP, OTHER
-- tables_read:
-- - WHOWNER.BT_MKT_MPLAY_PLAYS
-- - WHOWNER.BT_MKT_MPLAY_SESSION
-- - MPLAY.CLASIFICATION_ORIGINS
-- - MPLAY.LK_MPLAY_SOURCE_TYPE_ORIGIN_SESSION
-- joins:
-- - LEFT JOIN de orígenes y equipos de clasificación por origin
-- owner: data_team

INSERT INTO meli-sbox.MPLAY.MPLAY_TOUCHPOINT_USER
(
    timeframe_type, timeframe_id, sit_site_id, MONTH_ID, WEEK_ID, Origin, 
    User_Classification, touchpoint_team, touchpoint_no_team, 
    Clasificacion, Clasificacion_2, team, platform, Sessions, 
    Sessions_valid_visit, Sessions_valid_view, TVM, Visitors, 
    Valid_Visitors, Viewers
)
-- CTEs de Clasificación de Usuario (Se mantienen sin cambios)
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
        -- El límite máximo de DS es el fin del mes pasado
        AND DS <= CURRENT_DATE() - 1 
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

-- CTE de Sesiones (Filtro de fecha aplicado aquí para el mes anterior)
SESSIONS AS (
    SELECT
        s.SIT_SITE_ID,
        DATE_TRUNC(s.ds, MONTH) AS MONTH_ID,
        DATE_TRUNC(s.ds, WEEK(MONDAY)) AS WEEK_ID,
        s.ds,
        ORIGIN_PATH AS FIRST_EVENT_SOURCE,
        FIRST_TRACK AS FIRST_EVENT_PATH,
        FIRST_PLAY_DATETIME AS PLAY_TIMESTAMP,
        s.USER_ID,
        A.FLAG_N_R AS FLAG_N_R,
        s.SESSION_ID AS MELIDATA_SESSION_ID,
        s.DEVICE_PLATFORM,
        IF(
            ((S.HAS_SEARCH IS TRUE OR S.HAS_VCP IS TRUE OR S.HAS_VCM IS TRUE OR HAS_PLAY IS TRUE)
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
    WHERE 
        -- 🚨 FILTRO CLAVE: Rango del mes anterior (Día 1 al último día del mes)
        s.ds >= DATE_TRUNC(CURRENT_DATE(), MONTH) - INTERVAL 1 MONTH -- Primer día del mes pasado
        AND s.ds < DATE_TRUNC(CURRENT_DATE(), MONTH)                 -- Hasta el primer día del mes actual (exclusivo)
        AND s.SIT_SITE_ID IN UNNEST(['MLC', 'MLA', 'MLB', 'MLM', 'MCO', 'MPE', 'MLU', 'MEC'])
    GROUP BY ALL
),
SESSION_PLAY AS (
    SELECT DISTINCT
        s.SIT_SITE_ID,
        s.MONTH_ID,
        s.WEEK_ID,
        S.DS,
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

-- CTE de Agregación Mensual (Agregando al nivel MONTH_ID)
BASE_MPLAY_MONTHLY AS (
    SELECT
        'MONTHLY' AS timeframe_type,
        DATE_TRUNC(s.DS, MONTH) AS timeframe_id, -- El MONTH_ID del mes pasado es el ID
        s.sit_site_id,
        DATE_TRUNC(s.DS, MONTH) AS MONTH_ID,
        NULL AS WEEK_ID, -- Se deja NULL o vacío ya que es una agregación mensual
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
        MONTH_ID,
        Origin,
        User_Classification,
        PLATFORM,
        TEAM
)

-- Consulta Final de Inserción
SELECT
    b.timeframe_type,
    b.timeframe_id,
    b.sit_site_id,
    b.MONTH_ID,
    NULL AS WEEK_ID, -- Se mantiene como NULL en la salida final para la agregación mensual
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
FROM BASE_MPLAY_MONTHLY b
LEFT JOIN `meli-sbox.MPLAY.CLASIFICATION_ORIGINS` o
    ON b.Origin = o.origin;