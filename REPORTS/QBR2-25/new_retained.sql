-- description: Análisis de Cohortes de Retención de usuarios (Retention Cohorts) basado en periodos de 30 días desde el primer consumo. Calcula la recurrencia de visualización y el volumen de consumo (TVM). 
-- domain: behaviour 
-- product: mplay 
-- use_case: retention analysis / cohort lifecycle 
-- grain: sit_site_id, month_cohort_acq, month_retention 
-- time_grain: monthly (sliding window of 30 days) 
-- date_column: FIRST_DAY (Cohort Acquisition) / DAY_PLAY (Retention) 
-- date_filter: >= '2023-07-01' 
-- threshold_rule: playback_time >= 20s. La retención se mide en bloques de 30 días exactos mediante una tabla calendario custom. 
-- metrics: 
-- - TOTAL_USERS_COHORT: Tamaño total de la cohorte adquirida en un mes específico. 
-- - TOTAL_USERS_RETENTION: Usuarios únicos de la cohorte que volvieron a consumir en el mes N. 
-- - TVM: Minutos totales reproducidos por la cohorte en el periodo de retención. 
-- - ALL_MONTH_USER_RET: Usuarios con retención consecutiva perfecta (recurrentes en todos los meses hasta el actual). 
-- tables_read: 
-- - meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS 
-- - meli-bi-data.WHOWNER.LK_TIM_DAYS 
-- joins: 
-- - PLAY_FIRST_DAY LEFT JOIN TABLE_CALENDAR: Para normalizar los periodos de 30 días por cada usuario según su fecha de entrada. 
-- - USERS_CALENDAR LEFT JOIN PLAY_DAYS: Para atribuir el consumo histórico a los periodos de retención definidos. 
-- owner: data_team
-- En PLAY_DAYS se agrupan por SIT_SITE_ID, USER_ID y DAY_PLAY, y se trae el total de minutos reproducidos por día
WITH PLAY_DAYS AS (
               SELECT DISTINCT
                P.SIT_SITE_ID AS SIT_SITE_ID,
                (P.USER_ID) AS USER_ID,
                (P.DS) AS DAY_PLAY,
                SUM(P.PLAYBACK_TIME_MILLISECONDS/60000) AS PLAYBACK_TIME,
                FROM   `meli-bi-data.WHOWNER.BT_MKT_MPLAY_PLAYS` as P
               WHERE  P.PLAYBACK_TIME_MILLISECONDS/1000 >= 20
                GROUP BY ALL
),
-- Acá se trae el primer dia de reproducción por usuario
PLAY_FIRST_DAY AS ( 
                SELECT 
                SIT_SITE_ID,
                USER_ID,
                MIN(DAY_PLAY) AS FIRST_DAY
                FROM PLAY_DAYS
                GROUP BY ALL
),
TABLE_CALENDAR AS (
            SELECT
             *
             -- Este FROM tiene una subconsulta
            FROM (
                  SELECT 
                  *,
                  -- Con este ROW_NUMBER se obtiene el número de mes de la fecha de cohort
                  ROW_NUMBER()OVER(PARTITION BY FECHA_COHORT ORDER BY FECHA_INI ASC) AS MONTH_NUMBER
                  FROM (
                              SELECT 
                              -- Se toma el TIM_DAY como FECHA_COHORT
                              T.TIM_DAY AS FECHA_COHORT,
                              -- Se calcula FECHA_INI restando 29 dias a TIM_DAY de la tabla T2
                              T2.TIM_DAY -29  AS FECHA_INI,
                              -- FECHA_FIN es el TIM_DAY de la tabla T2
                              T2.TIM_DAY AS FECHA_FIN,
                              
                              FROM `meli-bi-data.WHOWNER.LK_TIM_DAYS` AS T
                              -- Join con la tabla TIM_DAYS nuevamente donde TIM_DAY+1 es menor o igual a TIM_DAY
                              LEFT JOIN `meli-bi-data.WHOWNER.LK_TIM_DAYS` AS T2 ON T.TIM_DAY+1 <= T2.TIM_DAY
                              WHERE T.TIM_DAY >= DATE'2023-07-01'
                              -- Con este QUALIFY se obtiene el TIM_DAY que es múltiplo de 30, es decir, el último día del mes
                              QUALIFY MOD(ROW_NUMBER()OVER(PARTITION BY T.TIM_DAY ORDER BY T2.TIM_DAY ASC), 30) = 0
                              ) AS A
                    -- Siempre que la FECHA_FIN sea menor o igual a la fecha de ayer
                   WHERE FECHA_FIN <= DATE_TRUNC(CURRENT_DATE, MONTH) - 1
                        ) AS B
                -- Con este QUALIFY contamos que el número de mes sea igual al número de días del mes
                QUALIFY COUNT(FECHA_FIN)OVER (PARTITION BY DATE_TRUNC(FECHA_COHORT,MONTH),MONTH_NUMBER) = EXTRACT(DAY FROM(LAST_DAY(FECHA_COHORT)))
),
-- Con USERS_CALENDAR se une la informacion de la primera reproducción con la tabla calendario trayendo del calendario la fecha de inicio y fin siempre que la primera reproducción sea igual a la fecha de cohort
USERS_CALENDAR AS (

            SELECT 
            PF.*,
            T.FECHA_INI,
            T.FECHA_FIN,
            MONTH_NUMBER
            FROM PLAY_FIRST_DAY AS PF
                LEFT JOIN TABLE_CALENDAR AS T ON PF.FIRST_DAY = T.FECHA_COHORT 

) ,
-- En BASE voy a tener toda la información final
BASE AS (
          SELECT
          *,
          -- Con este CASE se obtiene el FLAG_TVM que indica si el usuario reprodujo en TV o no
          CASE 
          WHEN FLAG_TVM > 0 AND ROW_NUMBER()OVER(PARTITION BY SIT_SITE_ID,USER_ID,FLAG_TVM ORDER BY MONTH_NUMBER ASC) = MONTH_NUMBER
          THEN 'ALL_MONTH' ELSE 'NOT_RECURRENT' END AS FLAG_CONSEC,
          COUNT(DISTINCT USER_ID || SIT_SITE_ID) OVER(PARTITION BY MONTH_COHORT_ACQ,SIT_SITE_ID) AS TOTAL_USERS_COHORT
          -- Este FROM tiene una subconsulta que agrupa por USER_ID, SIT_SITE_ID y MONTH_NUMBER para obtener el total de minutos reproducidos por usuario en cada mes 
          FROM (
                SELECT
                U.USER_ID,
                U.SIT_SITE_ID,
                U.MONTH_NUMBER,
                DATE_TRUNC(U.FIRST_DAY,MONTH) AS MONTH_COHORT_ACQ,
                SUM(PLAYBACK_TIME) AS TVM,
                -- Con este CASE se obtiene el FLAG_TVM que indica si el usuario reprodujo o no
                CASE WHEN SUM(PLAYBACK_TIME)>0 THEN 1 ELSE 0 END AS FLAG_TVM
                FROM USERS_CALENDAR AS U
                    -- Aca unimos en la subconsulta con PLAY_DAYS para obtener el total de minutos reproducidos por usuario en cada mes
                    LEFT JOIN PLAY_DAYS AS P ON U.USER_ID = P.USER_ID
                                            AND U.SIT_SITE_ID = P.SIT_SITE_ID
                                            AND P.DAY_PLAY BETWEEN U.FECHA_INI AND U.FECHA_FIN

                GROUP BY ALL
          )

)

-- En esta consulta final se agrupa por SIT_SITE_ID, MONTH_COHORT_ACQ y MONTH_NUMBER para obtener la cantidad de usuarios y el total de minutos reproducidos
SELECT 
SIT_SITE_ID,
MONTH_COHORT_ACQ,
MONTH_NUMBER-1 AS MONTH_RETENTION,
TOTAL_USERS_COHORT AS TOTAL_USERS_COHORT,
-- Este COUNT DISTINCT obtiene la cantidad de usuarios únicos que reprodujeron contenido
COUNT(DISTINCT CASE WHEN TVM > 0 THEN USER_ID||SIT_SITE_ID ELSE NULL END) AS TOTAL_USERS_RETENTION,
SUM(TVM) AS TVM,
COUNT(DISTINCT CASE WHEN TVM > 0 AND FLAG_CONSEC = 'ALL_MONTH' THEN USER_ID||SIT_SITE_ID  ELSE NULL END) AS ALL_MONTH_USER_RET,
SUM( CASE WHEN TVM > 0 AND FLAG_CONSEC = 'ALL_MONTH' THEN TVM ELSE NULL END) AS ALL_MONTH_TVM

FROM BASE
     WHERE MONTH_NUMBER IS NOT NULL
GROUP BY ALL