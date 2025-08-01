-- EXTRACCIÓN DE SESSIONS Y VIEWS --
-- Esta query se utiliza para calcular por sit_site_id, mes y semana, la cantidad de sesiones, usuarios, plataformas y tiempo de reproducción. Se agrupan los datos por origen de la sesión y se cuentan las sesiones y usuarios distintos.
-- TABLAS --
-- `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS`: tabla de reproducciones de Play
-- meli-bi-data.WHOWNER.BT_MKT_MPLAY_SESSION: tabla de sesiones de Play
-- meli-sbox.MPLAY.LK_MPLAY_SOURCE_TYPE_ORIGIN_SESSION: tabla de origen de sesiones de Play
-- OBJETIVO --
-- Alimenta la hoja BASE y BASE Daily la hoja USERS TVM W del Sheets "Seguimiento Weekly & Monthly por Touchpoint - Mercado Play". Para alimentar BASE Daily modificar el date_from por current_date()-21
-- Tambien se alimentan las hojas CVR M y CVR W del Sheets Performance Mercado Play - Monthly & Weekly. Para los Week se modifica la agrupación en el select final para agrupar por MONTH_ID o WEEK_ID


-- Se declaran las variables de SITES y la fecha de inicio y fin del analisis
DECLARE SITES ARRAY<STRING>;
DECLARE date_from DATE;
DECLARE date_to DATE;
SET SITES = ['MLC', 'MLA', 'MLB', 'MLM', 'MCO', 'MPE', 'MLU', 'MEC'];
SET date_from = '2025-03-03';
SET date_to = current_date();

-- En el CTE SESSIONS tomamos las sesiones de cada mes por SIT_SITE_ID, estableciendo su semana, el origen, su primer track, el primer play, etc., todo esto por usuario y session_id
WITH SESSIONS AS
              ( SELECT
                      s.SIT_SITE_ID,
                      -- Se trae el mes truncado de la fecha DS para definir el MONTH_ID
                      DATE_TRUNC(s.ds, MONTH) as MONTH_ID,
                      -- Se trae el lunes de cada semana
                      DATE_TRUNC(s.ds, WEEK(MONDAY)) as fecha_week,
                      s.ds,
                      ORIGIN_PATH AS FIRST_EVENT_SOURCE,
                      FIRST_TRACK AS FIRST_EVENT_PATH,
                      FIRST_PLAY_DATETIME AS PLAY_TIMESTAMP,
                      s.USER_ID,
                      s.SESSION_ID AS MELIDATA_SESSION_ID,
                      s.DEVICE_PLATFORM,
                      -- Se define si es una valid_visit teniendo en cuenta si hay alguna interaccion como una busqueda, si hubo vcp, vcm, mas de una impresion del feed o si hubo reproduccion
                      IF(((S.HAS_SEARCH IS TRUE OR S.HAS_VCP IS TRUE OR S.HAS_VCM IS TRUE OR HAS_PLAY IS TRUE) OR TOTAL_FEED_IMPRESSIONS > 1),TRUE,FALSE) AS FLAG_VALID_VISIT,
                      HAS_PLAY,
                      -- Se convierte el tiempo de la sesion a segundos
                      S.TOTAL_SESSION_MILLISECOND/1000 AS session_time_sec
              FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_SESSION` AS s
              -- Uso las variables declaradas en el inicio para filtrar por fecha y sites       
              WHERE s.ds >= date_from 
              AND s.ds < date_to
              AND s.SIT_SITE_ID IN UNNEST(SITES)
              GROUP BY ALL
)
-- En este CTE traemos la información del CTE agregandole informacion sobre el TSV y el TVM que se extraen de la tabla de Plays
, SESSION_PLAY AS   ( 
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
                    SUM(P.PLAYBACK_TIME_MILLISECONDS/1000) AS TSV,
                    SUM(P.PLAYBACK_TIME_MILLISECONDS/60000) AS TVM 
              FROM SESSIONS AS S 
              -- JOIN CON BT_MKT_MPLAY_PLAYS FILTRANDO CON COINCIDENCIAS EN SIT_SITE_ID, USER_ID, SESSION_ID Y QUE TSV MAYOR O IGUAL A 20SEG
              LEFT JOIN `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` AS P ON S.SIT_SITE_ID = P.SIT_SITE_ID
                                                                        AND s.USER_ID = P.USER_ID
                                                                        AND S.MELIDATA_SESSION_ID = P.SESSION_ID
                                                                        AND P.PLAYBACK_TIME_MILLISECONDS/1000 >= 20                                               
              GROUP BY ALL
)
--, New_Viewers AS (
--              SELECT
--                    PL.DS AS DS,
--                    DATE_TRUNC(PL.DS, MONTH) AS MONTH_ID,
--                    PL.SIT_SITE_ID,
--                    PL.USER_ID,
--              FROM  `WHOWNER.BT_MKT_MPLAY_PLAYS` AS PL
--                WHERE PL.PLAYBACK_TIME_MILLISECONDS/1000 >= 20 ---Para considerar solo views validos
--              QUALIFY ROW_NUMBER() OVER(PARTITION BY USER_ID ORDER BY ds ASC) = 1

--)
--, New_Visitors AS (
--                SELECT
--                      s.SIT_SITE_ID ,
--                      s.ds,
--                      DATE_TRUNC(s.DS, MONTH) AS MONTH_ID,
--                      s.USER_ID,
--                      s.SESSION_ID
--                FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_SESSION` AS s
--                WHERE s.SIT_SITE_ID IN UNNEST(SITES)
--                QUALIFY ROW_NUMBER() OVER(PARTITION BY USER_ID ORDER BY ds ASC) = 1
--)
--, Life_cycle as (
--SELECT *
--FROM `meli-sbox.MPLAY.MPLAY_USER_LIFECYCLE_SNAPSHOT`
--QUALIFY ROW_NUMBER() OVER(PARTITION BY USER_ID,DATE_TRUNC(SNAPSHOT_DATE, MONTH) ORDER BY SNAPSHOT_DATE ASC) = 1
--)
-----------------------------
-- Esta es la consulta final donde por sit_site_id, mes y semana se agrupan los datos de las sesiones, separando por origen, plataforma y contando las sesiones y usuarios
SELECT 
      s.sit_site_id,
      DATE_TRUNC(s.DS, MONTH) AS MONTH_ID,
      DATE_TRUNC(s.DS, WEEK(MONDAY)) as WEEK_ID,
      --s.FLAG_VALID_VISIT,
      --CASE WHEN VIS.USER_ID IS NOT NULL THEN 1 ELSE 0 END AS NEW_VISITOR_FLAG,
      --CASE WHEN V.USER_ID IS NOT NULL THEN 1 ELSE 0 END AS NEW_VIEWER_FLAG,
      --s.FIRST_EVENT_SOURCE,
      --LC.SEGMENT_LIFE_CYCLE,
      --LC.TYPE_USER,
      --o.channel,
      --o.team,
      --o.access,
      --o.negocio,
      CASE WHEN S.DEVICE_PLATFORM IN ('/tv/android') THEN '/tv/android'
           WHEN S.DEVICE_PLATFORM IN ('/tv/Tizen') THEN '/tv/Tizen'
           WHEN S.DEVICE_PLATFORM IN ('/tv/Web0S') THEN '/tv/Web0S'
           ELSE COALESCE(o.SOURCE_TYPE,'Otros')
           END AS Origin,
      --COALESCE(o.SOURCE_TYPE,'Otros') as Origin,
      -- Se cuenta el toatl de sesiones
      COUNT(DISTINCT s.melidata_session_id) Sessions,
      -- Se cuentan la cantidad de flag_valid_visit que son las interacciones validas y al contarlas generamos las sesiones que son valid visit
      COUNT(DISTINCT CASE WHEN s.FLAG_VALID_VISIT IS TRUE THEN s.melidata_session_id ELSE NULL END) as Sessions_valid_visit, ---no tiene en cuenta los bounced
      -- Se cuenta la cantidad de sesiones que tienen un tiempo de reproducción mayor o igual a 20 segundos
      COUNT(DISTINCT CASE WHEN s.TSV >= 20 THEN s.melidata_session_id ELSE NULL END) as Sessions_valid_view,
      SUM(s.TVM) as TVM,
      -- Las siguientes 3 columnas son similares a las anteriores pero cuentan los usuarios distintos
      COUNT(DISTINCT s.USER_ID) Visitors,
      COUNT(DISTINCT CASE WHEN s.FLAG_VALID_VISIT IS TRUE THEN s.USER_ID ELSE NULL END) as Valid_Visitors,
      COUNT(DISTINCT CASE WHEN s.TSV >= 20 THEN s.USER_ID ELSE NULL END) as Viewers
FROM SESSION_PLAY s
--LEFT JOIN New_Viewers V on S.USER_ID = V.USER_ID 
--                       AND S.sit_site_id = V.sit_site_id 
--                        AND  S.MONTH_ID = V.MONTH_ID
--LEFT JOIN New_Visitors VIS on S.USER_ID = VIS.USER_ID 
--                              AND S.sit_site_id = VIS.sit_site_id 
--                              AND  S.MONTH_ID = VIS.MONTH_ID
--LEFT JOIN Life_cycle LC ON S.USER_ID = LC.USER_ID 
--                        AND S.sit_site_id = LC.sit_site_id 
--                        AND  DATE_TRUNC(s.DS, MONTH) = DATE_TRUNC(LC.SNAPSHOT_DATE, MONTH)
---------------------
-- Se hace un join con LK_MPLAY_SOURCE_TYPE_ORIGIN_SESSION para obtener el origen de la sesión
LEFT JOIN `meli-sbox.MPLAY.LK_MPLAY_SOURCE_TYPE_ORIGIN_SESSION` o on coalesce(s.FIRST_EVENT_SOURCE,'NULL') = coalesce(o.SOURCE_TYPE,'NULL')
GROUP BY ALL
ORDER BY WEEK_ID ASC
;