-- AHA MOMENT (QUERY DE MPLAY) --
-- En esta query se realiza lo mismo que clasificación donde tambien clasificamos por AHA o NO AHA con la diferencia que el calculo en esta query del AHA se basa en el nuevo criterio donde se analiza todas las fechas de reproducción de los usuarios y si en los 30 dias posteriores logra el AHA. Con el criterio anterior solo se analizaba la 1er fecha del mes
-- TABLAS --
-- `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS`: tabla de reproducciones de Play

-- Separo la clásica NEW_RET_RECO para optimizar el analisis en 2 subconsultas. En USER_PLAYS_AGG se obtiene la actividad de los usuarios y en USER_DAILY_ACTIVITY se calcula el FLAG_N_R_DAILY como se hacia en NEW_RET_RECO
WITH USER_PLAYS_AGG AS (
    SELECT
        SIT_SITE_ID,
        USER_ID,
        DS,
        START_PLAY_TIMESTAMP,
        PLAYBACK_TIME_MILLISECONDS,
        DEVICE_PLATFORM,
        DATE_TRUNC(DS, MONTH) AS DS_MONTH_ID
    FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS`
    WHERE PLAYBACK_TIME_MILLISECONDS / 1000 >= 20
      AND DS <= CURRENT_DATE() - 1
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SIT_SITE_ID, USER_ID, DS, START_PLAY_TIMESTAMP ORDER BY 1) = 1
),
USER_DAILY_ACTIVITY AS (
    SELECT
        SIT_SITE_ID,
        USER_ID,
        DS,
        MIN(START_PLAY_TIMESTAMP) AS MIN_START_PLAY_TIMESTAMP_DAILY,
        MAX(START_PLAY_TIMESTAMP) AS MAX_START_PLAY_TIMESTAMP_DAILY,
        SUM(PLAYBACK_TIME_MILLISECONDS) AS DAILY_PLAY_MILLISECONDS,
        SUM(CASE WHEN UPPER(DEVICE_PLATFORM) LIKE '%TV%' THEN PLAYBACK_TIME_MILLISECONDS ELSE 0 END) AS DAILY_TV_MILLISECONDS,
        SUM(CASE WHEN UPPER(DEVICE_PLATFORM) LIKE '%MOBILE%' THEN PLAYBACK_TIME_MILLISECONDS ELSE 0 END) AS DAILY_MOBILE_MILLISECONDS,
        SUM(CASE WHEN UPPER(DEVICE_PLATFORM) LIKE '%DESK%' THEN PLAYBACK_TIME_MILLISECONDS ELSE 0 END) AS DAILY_DESKTOP_MILLISECONDS,
        DATE_TRUNC(DS, MONTH) AS TIME_FRAME_ID,
        LAG(DS,1) OVER(PARTITION BY SIT_SITE_ID, USER_ID ORDER BY DS ASC) AS DS_ANT,
        CASE
            WHEN LAG(DS,1) OVER(PARTITION BY SIT_SITE_ID, USER_ID ORDER BY DS ASC) IS NULL THEN 'NEW'
            WHEN DATE_DIFF(DS, LAG(DS,1) OVER(PARTITION BY SIT_SITE_ID, USER_ID ORDER BY DS ASC), DAY) <= 30 THEN 'RETAINED'
            ELSE 'RECOVERED'
        END AS FLAG_N_R_DAILY,
        MIN(DS) OVER(PARTITION BY SIT_SITE_ID, USER_ID) AS FIRST_DS_USER
    FROM USER_PLAYS_AGG
    GROUP BY SIT_SITE_ID, USER_ID, DS
),

-- En USER_MONTHLY_FLAG se obtiene el FLAG_N_R_DAILY del primer día de actividad en el mes y asi identificamos si el usuario es NEW, RETAINED o RECOVERED en el mes
USER_MONTHLY_FLAG AS (
    SELECT
        SIT_SITE_ID,
        USER_ID,
        TIME_FRAME_ID,
        FIRST_VALUE(FLAG_N_R_DAILY) OVER (PARTITION BY SIT_SITE_ID, USER_ID, TIME_FRAME_ID ORDER BY DS ASC) AS FLAG_N_R_FINAL
    FROM USER_DAILY_ACTIVITY
    QUALIFY ROW_NUMBER() OVER(PARTITION BY SIT_SITE_ID, USER_ID, TIME_FRAME_ID ORDER BY DS ASC) = 1
),
-- En AHA_CANDIDATE_LOGIC se identifica el AHA Moment por usuario y DS_BASE, creo un array de las reproducciones de los siguientes 30 días
AHA_CANDIDATE_LOGIC AS (
    SELECT
        SIT_SITE_ID,
        USER_ID,
        DS AS DS_BASE,
        DATE_TRUNC(DS, MONTH) AS AHA_MONTH_ID,
        -- Este ARRAY_AGG es medio complejo pero lo que hace es crear un array de las fechas de actividad del usuario en los siguientes 30 días
        ARRAY_AGG(DS) OVER (
            -- Acá particiono por SIT_SITE_ID y USER_ID para asegurar que cada usuario tenga su propio array
            PARTITION BY SIT_SITE_ID, USER_ID
            -- Acá ordeno por DS para que las fechas estén en orden ascendente. UNIX_DATE(DS) convierte la fecha a un número entero para facilitar el ordenamiento
            ORDER BY UNIX_DATE(DS) ASC
            -- Con RANGE BETWEEN 1 FOLLOWING AND 30 FOLLOWING, digo que quiero las fechas de los siguientes 30 días desde DS_BASE
            RANGE BETWEEN 1 FOLLOWING AND 30 FOLLOWING
        ) AS NEXT_30_DAYS_ACTIVITY_DS_ARRAY
    FROM USER_DAILY_ACTIVITY
),
-- El AHA_USERS_RAW filtra los usuarios que tienen al menos 2 días de actividad en los siguientes 30 días desde DS_BASE
AHA_USERS_RAW AS (
    SELECT
        SIT_SITE_ID,
        USER_ID,
        DS_BASE,
        AHA_MONTH_ID,
        -- En este array_lenght contamos cuántos días distintos de actividad hay en el array de los siguientes 30 días
        ARRAY_LENGTH(ARRAY(SELECT DISTINCT x FROM UNNEST(NEXT_30_DAYS_ACTIVITY_DS_ARRAY) as x)) AS DIAS_ADICIONALES_CONSUMO
    FROM AHA_CANDIDATE_LOGIC
    WHERE ARRAY_LENGTH(ARRAY(SELECT DISTINCT x FROM UNNEST(NEXT_30_DAYS_ACTIVITY_DS_ARRAY) as x)) >= 2
    AND NEXT_30_DAYS_ACTIVITY_DS_ARRAY IS NOT NULL
),
-- En FINAL_AHA_USERS se filtran los usuarios que logran el AHA MOMENT, asegurando que solo haya un AHA por mes
FINAL_AHA_USERS AS (
    SELECT
        SIT_SITE_ID,
        USER_ID,
        AHA_MONTH_ID,
        DS_BASE AS DIA_EN_QUE_LOGRO_EL_AHA
    FROM AHA_USERS_RAW
    -- Con este QUALIFY lo que hacemos es quedarnos con el primer AHA MOMENT por usuario y mes asi no hay duplicados
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SIT_SITE_ID, USER_ID, AHA_MONTH_ID ORDER BY DS_BASE ASC) = 1
),
-- En USER_MONTH_SUMMARY agregamos las métricas de actividad por usuario y mes
USER_MONTH_SUMMARY AS (
    -- Consolidamos las métricas mensuales. FLAG_N_R_FINAL se unirá después.
    SELECT
        ud.SIT_SITE_ID,
        ud.USER_ID,
        ud.TIME_FRAME_ID,
        SUM(ud.DAILY_PLAY_MILLISECONDS / 60000) AS TVM_TOTAL_TIMEFRAME,
        SUM(ud.DAILY_TV_MILLISECONDS / 60000) AS TOTAL_TV,
        SUM(ud.DAILY_MOBILE_MILLISECONDS / 60000) AS TOTAL_MOBILE,
        SUM(ud.DAILY_DESKTOP_MILLISECONDS / 60000) AS TOTAL_DESKTOP
    FROM USER_DAILY_ACTIVITY ud
    GROUP BY ud.SIT_SITE_ID, ud.USER_ID, ud.TIME_FRAME_ID
)

--Ya en la consulta final, unimos USER_MONTH_SUMMARY con FINAL_AHA_USERS y USER_MONTHLY_FLAG para obtener el resumen final
SELECT
    A.TIME_FRAME_ID,
    UMF.FLAG_N_R_FINAL,
    A.SIT_SITE_ID,
    CASE
        WHEN SAFE_CAST(A.USER_ID AS INT64) IS NULL THEN 'not_log'
        ELSE 'log'
    END AS flag_user,
    CONCAT(
        CASE WHEN A.TOTAL_TV > 0 THEN 'SMART' ELSE '' END, ' - ',
        CASE WHEN A.TOTAL_MOBILE > 0 THEN 'MOBILE' ELSE '' END, ' - ',
        CASE WHEN A.TOTAL_DESKTOP > 0 THEN 'DESKTOP' ELSE '' END
    ) AS PLATFORM_CONCAT,
    CASE
        WHEN A.TVM_TOTAL_TIMEFRAME < 3 THEN 'A. MENOR A 3 MIN'
        WHEN A.TVM_TOTAL_TIMEFRAME BETWEEN 3 AND 10 THEN 'B. ENTRE 3 Y 10 MIN'
        WHEN A.TVM_TOTAL_TIMEFRAME BETWEEN 10 AND 30 THEN 'C. ENTRE 10 Y 30 MIN'
        ELSE 'D. MAYOR A 30 MIN'
    END AS RANGE_TVM_TIMEFRAME,
    CASE
        WHEN B.USER_ID IS NOT NULL THEN 'AHA_MOMENT'
        ELSE 'NOT_AHA'
    END AS FLAG_AHA_MOMENT,
    COUNT(DISTINCT A.USER_ID) AS TOTAL_USERS
FROM USER_MONTH_SUMMARY A
LEFT JOIN FINAL_AHA_USERS B
  ON A.SIT_SITE_ID = B.SIT_SITE_ID
 AND A.USER_ID = B.USER_ID
 AND A.TIME_FRAME_ID = B.AHA_MONTH_ID
LEFT JOIN USER_MONTHLY_FLAG UMF 
  ON A.SIT_SITE_ID = UMF.SIT_SITE_ID
 AND A.USER_ID = UMF.USER_ID
 AND A.TIME_FRAME_ID = UMF.TIME_FRAME_ID
GROUP BY A.TIME_FRAME_ID, UMF.FLAG_N_R_FINAL, A.SIT_SITE_ID, flag_user, PLATFORM_CONCAT, RANGE_TVM_TIMEFRAME, FLAG_AHA_MOMENT;