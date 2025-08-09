-- Crea o reemplaza la tabla final con los títulos de mejor rendimiento
CREATE OR REPLACE TABLE `meli-sbox.MPLAY.NEGOCIO_MPLAY_CONTENT_PERFORMANCE_BEST_TITLES` AS (

    -- Define una CTE (Common Table Expression) llamada 'ESTADISTICAS_TIMEFRAME' para calcular métricas de usuario.
    WITH ESTADISTICAS_TIMEFRAME AS (

        -- Selecciona y calcula métricas clave para cada usuario y periodo de tiempo
        SELECT
            A.USER_ID,
            A.TIME_FRAME_ID,
            A.SIT_SITE_ID,
            A.TVM,
            A.TVM_SHOWS,
            A.TVM_MOVIE,
            
            -- Crea una bandera (FLAG_ENGAGEMENT_PERCENTILE) para identificar a los usuarios con alto compromiso.
            -- Un usuario se considera 'GREATER' si su tiempo de visualización de series (TVM_SHOWS) o películas (TVM_MOVIE)
            -- está en el percentil 80 o superior dentro de su respectivo periodo de tiempo y sitio.
            -- De lo contrario, se marca como 'LEAST'.
            CASE 
                WHEN TVM_SHOWS >= PERCENTILE_CONT(TVM_SHOWS, 0.80) OVER(PARTITION BY TIME_FRAME_ID, SIT_SITE_ID) 
                THEN 'GREATER' 
                WHEN TVM_MOVIE >= PERCENTILE_CONT(TVM_MOVIE, 0.80) OVER(PARTITION BY TIME_FRAME_ID, SIT_SITE_ID) 
                THEN 'GREATER' 
                ELSE 'LEAST' 
            END AS FLAG_ENGAGEMENT_PERCENTILE
        FROM 
            -- Subconsulta para agregar el tiempo total de visualización por usuario, periodo y sitio.
            (
                SELECT 
                    TIME_FRAME_ID,
                    SIT_SITE_ID,
                    USER_ID,
                    -- TVM total del usuario en el timeframe
                    SUM(TVM_TOTAL_TIMEFRAME) AS TVM,
                    -- TVM solo para series
                    SUM(CASE WHEN CONTENT_TYPE = 'SHOW' THEN TVM_TOTAL_TIMEFRAME ELSE 0 END ) AS TVM_SHOWS,
                    -- TVM solo para películas
                    SUM(CASE WHEN CONTENT_TYPE = 'MOVIE' THEN TVM_TOTAL_TIMEFRAME ELSE 0 END ) AS TVM_MOVIE
                FROM `meli-sbox.MPLAY.NEGOCIO_MPLAY_CONTENT_DATA_USER`
                GROUP BY ALL
            ) AS A
    )

    -- ---
    
    -- Consulta principal para la tabla final
    SELECT
        E.TIME_FRAME_ID,
        A.FLAG_N_R_FINAL,
        A.SIT_SITE_ID,
        -- Crea una bandera (FLAG_AHA_MOMENT) para identificar si un usuario ha superado un "AHA_MOMENT".
        -- Esto se define como haber visto 3 o más capítulos de series.
        CASE 
            WHEN A.DS_AHA_MOMENT >= 3 
            THEN 'AHA_MOMENT' 
            ELSE 'NOT_AHA' 
        END AS FLAG_AHA_MOMENT,
        E.FLAG_ENGAGEMENT_PERCENTILE,
        A.CONTENT_TYPE,
        A.TITLE_ADJUSTED,
        
        -- Cuenta el número de usuarios únicos en cada grupo.
        COUNT(DISTINCT E.USER_ID) AS TOTAL_USERS,
        
        -- Suma los tiempos de visualización para el grupo.
        SUM(E.TVM) AS TVM,
        SUM(E.TVM_SHOWS) AS TVM_SHOWS,
        SUM(E.TVM_MOVIE) AS TVM_MOVIE
    FROM ESTADISTICAS_TIMEFRAME AS E
    
    -- Une 'ESTADISTICAS_TIMEFRAME' con la tabla original 'NEGOCIO_MPLAY_CONTENT_DATA_USER'.
    -- El JOIN se realiza para traer información de los títulos que los usuarios de alto compromiso han visto.
    LEFT JOIN `meli-sbox.MPLAY.NEGOCIO_MPLAY_CONTENT_DATA_USER` AS A 
        ON E.USER_ID = A.USER_ID 
        AND E.SIT_SITE_ID = A.SIT_SITE_ID
    
    -- Filtra los datos para incluir solo a los usuarios marcados como 'NEW' y donde el contenido visto es el mismo que el del 'primer consumo' (CONTENT_FLAG_N_R).
    -- Este JOIN está diseñado para analizar el comportamiento del primer consumo de los nuevos usuarios de alto compromiso.
    AND A.FLAG_N_R_FINAL = 'NEW'
    AND A.CONTENT_ID = A.CONTENT_FLAG_N_R

    -- ---

    -- Filtra los resultados finales para incluir solo a los usuarios con alto compromiso ('GREATER').
    WHERE E.FLAG_ENGAGEMENT_PERCENTILE = 'GREATER' 

    -- Agrupa los resultados por todas las columnas seleccionadas (excepto las agregaciones) para obtener un resumen de métricas por título.
    GROUP BY ALL
);