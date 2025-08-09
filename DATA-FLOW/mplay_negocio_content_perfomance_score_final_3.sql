-- Parte 1: DELETE (Eliminar datos antiguos)

-- Esta instrucción elimina todas las filas de la tabla `MPLAY_NEGOCIO_SEARCH_WORDS`
-- donde la columna `DS` (que probablemente representa la fecha de la búsqueda)
-- es igual o posterior a la fecha actual menos 7 días.
-- Esto asegura que la tabla solo contenga los datos más recientes que serán
-- insertados en el siguiente paso.

DELETE FROM `meli-sbox.MPLAY.MPLAY_NEGOCIO_SEARCH_WORDS`
WHERE DS >= CURRENT_DATE-7 
;

-- Parte 2: INSERT (Insertar nuevos datos)

-- Esta instrucción inserta los resultados de la siguiente consulta en la tabla.
INSERT INTO `meli-sbox.MPLAY.MPLAY_NEGOCIO_SEARCH_WORDS`
(
    -- La consulta selecciona y procesa los datos para ser insertados.
    select 
        -- Extrae el sitio web (SIT_SITE_ID) del seguimiento.
        site AS SIT_SITE_ID,
        
        -- Extrae la fecha del seguimiento.
        DS,
        
        -- Extrae la palabra de búsqueda del campo `EVENT_DATA` (que es un JSON).
        -- La función `JSON_VALUE` extrae el valor del campo 'query'.
        -- `UPPER` convierte la palabra a mayúsculas para estandarizar.
        -- `TRIM` elimina cualquier espacio en blanco al principio o al final.
        TRIM(UPPER(JSON_VALUE(EVENT_DATA, '$.query'))) AS WORD,
        
        -- Cuenta cuántas veces se usó cada palabra clave.
        count(*) AS TOTAL_TRACKS
    from `meli-bi-data.MELIDATA.TRACKS`
    
    -- Filtra los datos de la tabla de seguimiento (`TRACKS`) para procesar solo los
    -- registros de los últimos 7 días. Esto coincide con el filtro del DELETE.
    WHERE DS >= CURRENT_DATE-7
    
    -- Filtra los registros para incluir únicamente aquellos que corresponden a
    -- la página de búsqueda de Mercado Play.
    AND PATH = '/mercadoplay/search'
    
    -- Agrupa los resultados por todas las columnas seleccionadas (`site`, `DS` y `WORD`)
    -- para poder contar el número de seguimientos por cada combinación única.
    group by ALL 
)