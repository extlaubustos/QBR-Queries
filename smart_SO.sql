------------ VERSION CAMI 15:07 DEL 23/7 ----------- 
------ FALTA ULTIMAR ALGO ---------
-- SO_CLAS calcula por USER_ID, SIT_SITE_ID, DS la cantidad de minutos vistos en ANDROID, SAMSUNG, LG, Y TL TOTAL DE TVM siempre que la reproduccion esa mayor a 20seg y solo sea en televisión. ¿TVM va a ser siempre el mismo que los totales o quizas vio en 2 teles?
WITH SO_CLAS AS (
    SELECT
        USER_ID,
        SIT_SITE_ID,
        DS,
        SUM(CASE WHEN UPPER(DEVICE_PLATFORM) LIKE '%TV/ANDROID%' THEN 
                   PLAYBACK_TIME_MILLISECONDS/60000 ELSE 0 END) AS TOTAL_ANDROID,
        SUM(CASE WHEN UPPER(DEVICE_PLATFORM) LIKE '%TV/TIZEN%' THEN 
                   PLAYBACK_TIME_MILLISECONDS/60000 ELSE 0 END) AS TOTAL_SAMSUNG,
        SUM(CASE WHEN UPPER(DEVICE_PLATFORM) LIKE '%TV/WEB%' THEN 
                   PLAYBACK_TIME_MILLISECONDS/60000 ELSE 0 END) AS TOTAL_LG,
        SUM(PLAYBACK_TIME_MILLISECONDS/60000) AS TVM
    FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` 
    WHERE PLAYBACK_TIME_MILLISECONDS/1000 >= 20
      AND UPPER(DEVICE_PLATFORM) LIKE('%TV%')
    GROUP BY USER_ID, SIT_SITE_ID, DS
),
--TVM_CAST calcula por USER_ID, SIT_SITE_ID, DS la cantidad de minutos CAST siempre que la reproducción sea mayor a 20seg
TVM_CAST AS (
    SELECT
        USER_ID,
        SIT_SITE_ID,
        DS,
        SUM(PLAYBACK_TIME_MILLISECONDS_CAST/60000) AS TOTAL_CAST
    FROM `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS`
    WHERE PLAYBACK_TIME_MILLISECONDS_CAST/1000 >= 20
    GROUP BY USER_ID, SIT_SITE_ID, DS
),
-- UNION_TV_CAST realiza la unión entre SO_CLAS y TVM_CAST con un FULL OUTER JOIN trayendo datos de ambas tablas. Se utiliza COALESCE para traer USER_ID, SIT_SITE_ID, DS, y los totales que se calcularon en ambas tablas. Para el JOIN se utiliza USER_ID, SIT_SITE_ID, DS para joinear
UNION_TV_CAST AS (
    SELECT
        COALESCE(SO.USER_ID, TC.USER_ID) AS USER_ID,
        COALESCE(SO.SIT_SITE_ID, TC.SIT_SITE_ID) AS SIT_SITE_ID,
        COALESCE(SO.DS, TC.DS) AS DS,
        COALESCE(SO.TOTAL_ANDROID,0) AS TOTAL_ANDROID,
        COALESCE(SO.TOTAL_SAMSUNG,0) AS TOTAL_SAMSUNG,
        COALESCE(SO.TOTAL_LG,0) AS TOTAL_LG,
        COALESCE(TC.TOTAL_CAST, 0) AS TOTAL_CAST
    FROM SO_CLAS AS SO
    FULL OUTER JOIN TVM_CAST AS TC 
        ON SO.USER_ID = TC.USER_ID 
       AND SO.SIT_SITE_ID = TC.SIT_SITE_ID 
       AND SO.DS = TC.DS
    GROUP BY ALL
),
-- TOTAL_TV_CAST trae los datos de UNION_TV_CAST sumando los totales por USER_ID, SIT_SITE_ID, DS
TOTAL_TV_CAST AS (
    SELECT
        USER_ID,
        SIT_SITE_ID,
        DS,
        SUM(TOTAL_ANDROID) AS TOTAL_ANDROID,
        SUM(TOTAL_SAMSUNG) AS TOTAL_SAMSUNG,
        SUM(TOTAL_LG) AS TOTAL_LG,
        SUM(TOTAL_CAST) AS TOTAL_CAST
    FROM UNION_TV_CAST AS U
    GROUP BY ALL
)
-- La consulta final agrupa por SIT_SITE_ID, MONTH_ID, PLATFORM_CONCAT donde PLATFORM_CONCAT es una concatenación de los distintos sistemas operativos donde hay reproducciones. Para crear PLATFORM_CONCAT se chequean los totales. También se cuenta la cantidad de viewers y el TOTAL_TV_TVM sumando los totales en cada Operating System y CAST
SELECT
    TTC.SIT_SITE_ID,
    DATE_TRUNC(TTC.DS, MONTH) AS MONTH_ID,
    CONCAT(
        CASE WHEN TTC.TOTAL_ANDROID > 0 THEN 'ANDROID' ELSE '' END,
        CASE WHEN TTC.TOTAL_LG > 0 THEN 'LG' ELSE '' END,
        CASE WHEN TTC.TOTAL_SAMSUNG > 0 THEN 'SAMSUNG' ELSE '' END,
        CASE WHEN TTC.TOTAL_CAST > 0 THEN 'CAST' ELSE '' END
    ) AS PLATFORM_CONCAT,
    COUNT(DISTINCT TTC.USER_ID) AS VIEWERS,
    ROUND(SUM(TOTAL_ANDROID + TOTAL_SAMSUNG + TOTAL_LG + TOTAL_CAST), 3) AS TOTAL_TV_TVM
FROM TOTAL_TV_CAST AS TTC
GROUP BY SIT_SITE_ID, MONTH_ID, PLATFORM_CONCAT