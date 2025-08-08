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
            FROM (
                  SELECT 
                  *,
                  ROW_NUMBER()OVER(PARTITION BY FECHA_COHORT ORDER BY FECHA_INI ASC) AS MONTH_NUMBER
                  FROM (
                              SELECT 
                              T.TIM_DAY AS FECHA_COHORT,
                              T2.TIM_DAY -29  AS FECHA_INI,
                              T2.TIM_DAY AS FECHA_FIN,
                              
                              FROM `meli-bi-data.WHOWNER.LK_TIM_DAYS` AS T
                              LEFT JOIN `meli-bi-data.WHOWNER.LK_TIM_DAYS` AS T2 ON T.TIM_DAY+1 <= T2.TIM_DAY
            --> +1 PARA SACAR DIA DE ALTA
                              WHERE T.TIM_DAY >= DATE'2023-07-01'
                              QUALIFY MOD(ROW_NUMBER()OVER(PARTITION BY T.TIM_DAY ORDER BY T2.TIM_DAY ASC), 30) = 0
                              ) AS A
                   WHERE FECHA_FIN <= DATE_TRUNC(CURRENT_DATE-1,MONTH)-1
                        ) AS B
                QUALIFY COUNT(FECHA_FIN)OVER (PARTITION BY DATE_TRUNC(FECHA_COHORT,MONTH),MONTH_NUMBER) = EXTRACT(DAY FROM(LAST_DAY(FECHA_COHORT)))
                     --> COHORT_HASTA AYER
),
USERS_CALENDAR AS (

            SELECT 
            PF.*,
            T.FECHA_INI,
            T.FECHA_FIN,
            MONTH_NUMBER
            FROM PLAY_FIRST_DAY AS PF
                LEFT JOIN TABLE_CALENDAR AS T ON PF.FIRST_DAY = T.FECHA_COHORT 

) ,
BASE AS (
          SELECT
          *,
          CASE WHEN FLAG_TVM > 0 AND
          ROW_NUMBER()OVER(PARTITION BY SIT_SITE_ID,USER_ID,FLAG_TVM ORDER BY MONTH_NUMBER ASC) = MONTH_NUMBER
          THEN 'ALL_MONTH' ELSE 'NOT_RECURRENT' END AS FLAG_CONSEC,
          COUNT(DISTINCT USER_ID || SIT_SITE_ID) OVER(PARTITION BY MONTH_COHORT_ACQ,SIT_SITE_ID) AS TOTAL_USERS_COHORT

          FROM (
                SELECT
                U.USER_ID,
                U.SIT_SITE_ID,
                U.MONTH_NUMBER,
                DATE_TRUNC(U.FIRST_DAY,MONTH) AS MONTH_COHORT_ACQ,
                SUM(PLAYBACK_TIME) AS TVM,
                CASE WHEN SUM(PLAYBACK_TIME)>0 THEN 1 ELSE 0 END AS FLAG_TVM
                FROM USERS_CALENDAR AS U

                    LEFT JOIN PLAY_DAYS AS P ON U.USER_ID = P.USER_ID
                                            AND U.SIT_SITE_ID = P.SIT_SITE_ID
                                            AND P.DAY_PLAY BETWEEN U.FECHA_INI AND U.FECHA_FIN

                GROUP BY ALL
          )

)


SELECT 
SIT_SITE_ID,
MONTH_COHORT_ACQ,
MONTH_NUMBER-1 AS MONTH_RETENTION,
TOTAL_USERS_COHORT AS TOTAL_USERS_COHORT,
COUNT(DISTINCT CASE WHEN TVM > 0 THEN USER_ID||SIT_SITE_ID ELSE NULL END) AS TOTAL_USERS_RETENTION,
SUM(TVM) AS TVM,
COUNT(DISTINCT CASE WHEN TVM > 0 AND FLAG_CONSEC = 'ALL_MONTH' THEN USER_ID||SIT_SITE_ID  ELSE NULL END) AS ALL_MONTH_USER_RET,
SUM( CASE WHEN TVM > 0 AND FLAG_CONSEC = 'ALL_MONTH' THEN TVM ELSE NULL END) AS ALL_MONTH_TVM

FROM BASE
     WHERE MONTH_NUMBER IS NOT NULL
GROUP BY ALL